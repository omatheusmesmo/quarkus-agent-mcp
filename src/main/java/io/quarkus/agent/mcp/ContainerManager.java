package io.quarkus.agent.mcp;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.exception.NotModifiedException;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.dockerclient.DockerClientProviderStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * Manages pgvector containers for Quarkus documentation search.
 * <p>
 * For Quarkus 3.36+ (which ships RAG SQL artifacts), uses a generic pgvector image
 * with documentation loaded from SQL fragments by {@link RagSqlLoader}.
 * For older versions, falls back to the pre-built {@code chappie-ingestion-quarkus} image
 * with documentation baked in.
 * <p>
 * Containers are version-specific (each Quarkus version gets its own container) and outlive
 * the MCP server, so restarts and concurrent servers share one container per version and image.
 * They are managed through the Docker API directly rather than {@code GenericContainer}:
 * Testcontainers only honours {@code withReuse(true)} when the user opted in via
 * {@code ~/.testcontainers.properties}, and otherwise ties every container to a Ryuk reaper,
 * so each server process ended up starting a container of its own. A fixed container name
 * makes the lookup deterministic and lets Docker reject a second concurrent create.
 * <p>
 * What this server remembers about a container (its ID and host ports) is only a cache. Another
 * MCP server or the user can restart, re-publish or remove the shared container at any time, so
 * callers that fail to reach it call {@link #invalidate} and {@link #ensureRunning} again, which
 * resolves the container afresh from Docker. Nothing here ever deletes a named container.
 * <p>
 * With {@code agent-mcp.doc-search.pg-host} set, an existing database and embedding server are
 * used instead and no container runtime is needed, e.g. where the server runs in a sandbox.
 */
@ApplicationScoped
public class ContainerManager {

    private static final Logger LOG = Logger.getLogger(ContainerManager.class);
    static final int RAG_SQL_MIN_MINOR = 36;
    static final int RAG_SQL_MIN_PATCH_AT_36 = 1;

    @ConfigProperty(name = "agent-mcp.doc-search.image", defaultValue = "pgvector/pgvector:pg17")
    String image;

    @ConfigProperty(name = "agent-mcp.doc-search.image-prefix", defaultValue = "ghcr.io/quarkusio/chappie-ingestion-quarkus")
    String imagePrefix;

    @ConfigProperty(name = "agent-mcp.doc-search.image-tag", defaultValue = "latest")
    String defaultImageTag;

    @ConfigProperty(name = "agent-mcp.doc-search.pg-user", defaultValue = "quarkus")
    String pgUser;

    @ConfigProperty(name = "agent-mcp.doc-search.pg-password", defaultValue = "quarkus")
    String pgPassword;

    @ConfigProperty(name = "agent-mcp.doc-search.pg-database", defaultValue = "quarkus")
    String pgDatabase;

    /** Host of an existing doc-search database. When set, no container is started. */
    @ConfigProperty(name = "agent-mcp.doc-search.pg-host")
    Optional<String> externalHost = Optional.empty();

    @ConfigProperty(name = "agent-mcp.doc-search.pg-port", defaultValue = "5432")
    int externalPort = PG_PORT;

    /** Embedding server for an existing database; defaults to its port on {@code pg-host}. */
    @ConfigProperty(name = "agent-mcp.doc-search.embedding-url")
    Optional<String> externalEmbeddingUrl = Optional.empty();

    /** Never write to an existing database: its docs are loaded ahead of time by someone else. */
    @ConfigProperty(name = "agent-mcp.doc-search.read-only", defaultValue = "false")
    boolean readOnly;

    @Inject
    RagSqlLoader ragSqlLoader;

    @ConfigProperty(name = "agent-mcp.doc-search.warmup-timeout-millis", defaultValue = "300000")
    long warmupTimeoutMillis;

    /** A doc-search container nobody has used for this long is stopped (it keeps its data). */
    @ConfigProperty(name = "agent-mcp.doc-search.idle-stop-after", defaultValue = "PT24H")
    Duration idleStopAfter = Duration.ofHours(24);

    /** A doc-search container stopped for this long is removed. */
    @ConfigProperty(name = "agent-mcp.doc-search.idle-remove-after", defaultValue = "P14D")
    Duration idleRemoveAfter = Duration.ofDays(14);

    /** How long to wait for a doc-search database and embedding server to accept requests. */
    Duration startupTimeout = Duration.ofMinutes(3);

    static final String CONTAINER_NAME_PREFIX = "quarkus-agent-mcp-docs-";
    private static final String EXTERNAL_ID = "external";
    private static final int PG_PORT = 5432;
    private static final int EMBEDDING_PORT = 9222;
    private static final Duration CLEANUP_INTERVAL = Duration.ofHours(6);
    private static final long MARK_USED_INTERVAL_MS = 5 * 60_000;
    private static final String USAGE_TABLE = "agent_mcp_usage";
    private final Map<String, Long> lastMarkedUsed = new ConcurrentHashMap<>();
    private volatile boolean cleanupScheduled;
    private final Map<String, Object> versionLocks = new ConcurrentHashMap<>();
    /** Guards warm-up retries; never held while a container starts, unlike the version locks. */
    private final Object warmupLock = new Object();
    volatile Thread warmupWorker;

    /**
     * A doc-search container this server is using.
     *
     * @param embeddingPort the host port of the embedding server, or -1 for legacy images without one
     */
    record DocContainer(String id, String host, int pgPort, int embeddingPort) {
    }

    final ConcurrentHashMap<String, DocContainer> containers = new ConcurrentHashMap<>();
    private volatile DockerClient dockerClient;
    private final Set<String> fallbackVersions = ConcurrentHashMap.newKeySet();
    private final Set<String> ragSqlVersions = ConcurrentHashMap.newKeySet();
    private volatile Boolean dockerAvailable;
    private volatile boolean defaultWarmupStarted;
    private volatile long lastWarmupRetry;
    private static final long WARMUP_RETRY_INTERVAL_MS = 30_000;

    /**
     * Terminal outcome of the background warm-up, or null while it is still running.
     *
     * @param error the failure message, or null if the warm-up succeeded
     */
    private record WarmupOutcome(String error) {
        static final WarmupOutcome SUCCESS = new WarmupOutcome(null);
    }

    private final AtomicReference<WarmupOutcome> defaultWarmupOutcome = new AtomicReference<>();

    /**
     * Starts the default container in a background thread so the first searchDocs
     * call doesn't block for container startup.
     * <p>
     * A watchdog thread interrupts the warm-up after {@code warmupTimeoutMillis} (default 5
     * minutes) if it hasn't finished, so callers get a clear failure instead of "still warming
     * up" forever when a slow/unreachable dependency lookup (e.g. Maven artifact resolution over
     * a restricted network) blocks the pipeline. Check the agent-mcp log for per-step timing —
     * look for "RAG scan" and "Maven dependency" entries to see exactly which lookup was stuck.
     * <p>
     * The interrupt is best-effort, so a timed-out warm-up may still complete afterwards; if it
     * does, the success replaces the recorded timeout and the server reports itself ready.
     */
    public void warmUpDefaultAsync() {
        if (defaultWarmupStarted) {
            return;
        }
        defaultWarmupStarted = true;
        startWarmUp();
    }

    /**
     * Starts a new warm-up if the last one failed, so a failure is not permanent: Docker may not
     * have been running yet, another server may have held the load lock, or the user may since
     * have removed a broken container. Rate-limited, so a burst of searches starts one attempt.
     *
     * @return true if a new attempt was started
     */
    public boolean retryWarmUpIfFailed() {
        synchronized (warmupLock) {
            WarmupOutcome outcome = defaultWarmupOutcome.get();
            long now = System.currentTimeMillis();
            if (outcome == null || outcome.error() == null || now - lastWarmupRetry < WARMUP_RETRY_INTERVAL_MS) {
                return false;
            }
            Thread previous = warmupWorker;
            if (previous != null && previous.isAlive()) {
                // Timed out but still stuck (interrupts are best-effort). A new attempt would queue on
                // the same version lock, and the old one could later record its failure over it.
                return false;
            }
            lastWarmupRetry = now;
            if (Boolean.FALSE.equals(dockerAvailable)) {
                dockerAvailable = null;
            }
            LOG.infof("Retrying documentation search warm-up after failure: %s", outcome.error());
            defaultWarmupOutcome.set(null);
            startWarmUp();
            return true;
        }
    }

    private void startWarmUp() {
        long warmupStart = System.currentTimeMillis();
        Thread worker = Thread.ofVirtual().name("container-warmup").unstarted(() -> {
            try {
                ensureRunning(null, null);
                recordWarmupSuccess();
                LOG.infof("Documentation search is ready (%d ms)", System.currentTimeMillis() - warmupStart);
                if (!isExternal()) {
                    scheduleCleanup();
                }
            } catch (Throwable e) {
                // Catch Throwable, not just Exception: an uncaught Error (e.g. NoClassDefFoundError,
                // ExceptionInInitializerError) would otherwise kill this thread silently without ever
                // recording an outcome, leaving isDefaultReady()/isDefaultWarmupDone() both false
                // forever and searchDocs stuck reporting "still warming up" with no diagnostic trace.
                LOG.error("Background container warm-up failed after "
                        + (System.currentTimeMillis() - warmupStart) + " ms", e);
                recordWarmupFailure(e.getMessage() != null ? e.getMessage() : e.getClass().getName());
            }
        });
        warmupWorker = worker;
        worker.start();

        Thread.ofPlatform().daemon(true).name("container-warmup-watchdog").start(() -> {
            try {
                worker.join(warmupTimeoutMillis);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (!worker.isAlive()) {
                // Finished and recorded its own outcome; don't race a retry that has reset it
                return;
            }
            long elapsed = System.currentTimeMillis() - warmupStart;
            boolean timedOut = recordWarmupFailure("Warm-up timed out after " + elapsed + " ms. "
                    + "See the agent-mcp log for the last 'RAG scan' or 'Maven dependency' entry to "
                    + "identify which dependency lookup was stuck.");
            if (timedOut) {
                LOG.warnf(
                        "Documentation search warm-up exceeded %d ms (elapsed %d ms) — aborting. "
                                + "Check the agent-mcp log for 'RAG scan' / 'Maven dependency' entries above "
                                + "to see which lookup was stuck (often a slow/blocked network call to resolve "
                                + "a documentation artifact for a project dependency).",
                        warmupTimeoutMillis, elapsed);
                worker.interrupt();
            }
        });
    }

    /**
     * Records warm-up success, overwriting any failure already recorded. Interrupting a virtual
     * thread parked in JDBC or jar IO does not reliably stop it, so the worker can still finish
     * after the watchdog gave up on it. When that happens the container really is usable, and
     * that fact has to win over the watchdog's guess — otherwise the server reports a stale
     * timeout forever over a working database.
     */
    void recordWarmupSuccess() {
        defaultWarmupOutcome.set(WarmupOutcome.SUCCESS);
    }

    /**
     * Records a warm-up failure unless an outcome was already recorded, so that the first
     * failure reported wins and a late failure cannot mask an earlier, more specific one.
     *
     * @return true if this call is the one that recorded the outcome
     */
    boolean recordWarmupFailure(String error) {
        return defaultWarmupOutcome.compareAndSet(null, new WarmupOutcome(error));
    }

    public boolean isDefaultReady() {
        WarmupOutcome outcome = defaultWarmupOutcome.get();
        return outcome != null && outcome.error() == null;
    }

    public boolean isDefaultWarmupDone() {
        return defaultWarmupOutcome.get() != null;
    }

    public String getDefaultWarmupError() {
        WarmupOutcome outcome = defaultWarmupOutcome.get();
        return outcome == null ? null : outcome.error();
    }

    /**
     * Ensure a pgvector container is running for the given Quarkus version.
     * For 3.36+, starts a generic pgvector container and loads RAG SQL fragments.
     * For older versions, starts the pre-built chappie-ingestion image.
     * <p>
     * Returns straight away if this server already resolved the container. Call
     * {@link #invalidate} first when the cached container could not be reached.
     *
     * @param quarkusVersion the Quarkus version for docs, or null for default
     * @param projectDir     the project directory for non-core extension discovery, or null
     */
    public void ensureRunning(String quarkusVersion, String projectDir) {
        String versionKey = quarkusVersion != null ? quarkusVersion : "default";
        // Searches call this every time, so the cached case must not wait on any lock
        if (containers.containsKey(versionKey)) {
            return;
        }
        if (!isExternal()) {
            checkDockerAvailable();
        }
        // One lock per version: starting or loading one version's container can take minutes and
        // must not hold up searches or startups for other versions
        synchronized (versionLocks.computeIfAbsent(versionKey, k -> new Object())) {
            if (containers.containsKey(versionKey)) {
                return;
            }
            if (isExternal()) {
                connectExternal(versionKey, quarkusVersion, projectDir);
            } else {
                startAndLoad(versionKey, quarkusVersion, projectDir);
            }
        }
    }

    /**
     * Ensures the embedding server is up. In a container setup it runs in the default container,
     * which a reconnect may have dropped. An existing database's embedding server needs no setup,
     * and resolving the default version there would load its docs over the ones a project's
     * version loaded into the shared database.
     */
    public void ensureEmbeddingServer() {
        if (!isExternal()) {
            ensureRunning(null, null);
        }
    }

    /** True if doc search uses an existing database rather than a container. */
    boolean isExternal() {
        return externalHost.isPresent();
    }

    /**
     * Uses the database at {@code pg-host} for a version. Every version shares it, so it holds one
     * set of docs; unless it is read-only, each version's docs are loaded into it as they would be
     * into that version's container.
     */
    private void connectExternal(String versionKey, String quarkusVersion, String projectDir) {
        String where = externalHost.get() + ":" + externalPort;
        DocContainer database = new DocContainer(EXTERNAL_ID, externalHost.get(), externalPort, -1);
        LOG.infof("Using doc-search database at %s and embedding server at %s for Quarkus %s",
                where, embeddingUrl(), versionKey);
        awaitReady("doc-search database at " + where, database, embeddingUrl(), () -> false,
                detail -> new IllegalStateException("Doc-search database at " + where + " or embedding server at "
                        + embeddingUrl() + " is not reachable: " + detail));
        containers.put(versionKey, database);
        if (readOnly) {
            return;
        }
        try {
            if (supportsRagSql(quarkusVersion)) {
                loadRagData(versionKey, quarkusVersion, projectDir);
            } else {
                loadNonCoreRagData(versionKey, quarkusVersion, projectDir);
            }
        } catch (RuntimeException | Error e) {
            containers.remove(versionKey);
            throw e;
        }
    }

    private String embeddingUrl() {
        String url = externalEmbeddingUrl.orElseGet(() -> "http://" + externalHost.get() + ":" + EMBEDDING_PORT);
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private void startAndLoad(String versionKey, String quarkusVersion, String projectDir) {
        try {
            if (supportsRagSql(quarkusVersion)) {
                try {
                    startGenericContainer(versionKey);
                    loadRagData(versionKey, quarkusVersion, projectDir);
                    ragSqlVersions.add(versionKey);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Failed to start documentation container (" + image + "): " + e.getMessage(), e);
                }
            } else {
                startLegacyContainer(versionKey, quarkusVersion);
                loadNonCoreRagData(versionKey, quarkusVersion, projectDir);
            }
        } catch (RuntimeException | Error e) {
            // The container is cached before its docs load; a failed load must not leave it cached,
            // or the next attempt would return early and report ready over missing docs
            containers.remove(versionKey);
            throw e;
        }
        markUsed(quarkusVersion);
    }

    /**
     * Forgets the cached container for a version, so the next {@link #ensureRunning} resolves it
     * from Docker again: finds it on its current ports, starts it if stopped, or recreates it
     * (and reloads its docs) if it was removed.
     */
    public void invalidate(String quarkusVersion) {
        String versionKey = quarkusVersion != null ? quarkusVersion : "default";
        if (containers.remove(versionKey) != null) {
            LOG.infof("Forgetting cached doc-search container for Quarkus %s; it will be resolved again", versionKey);
        }
    }

    /**
     * Returns true if the given version fell back to the default image tag
     * (only applicable to the legacy pre-built image path).
     */
    public boolean isUsingFallback(String quarkusVersion) {
        String versionKey = quarkusVersion != null ? quarkusVersion : "default";
        return fallbackVersions.contains(versionKey);
    }

    /**
     * Loads any new RAG SQL fragments into an already-running container.
     * Called after extensions are added to a project to pick up their docs.
     * For legacy containers, only non-core extension docs are loaded (core docs are baked in).
     */
    public void loadIncrementalRagData(String quarkusVersion, String projectDir) {
        if (isExternal() && readOnly) {
            return;
        }
        String versionKey = quarkusVersion != null ? quarkusVersion : "default";
        DocContainer container = containers.get(versionKey);
        if (container == null) {
            LOG.debugf("No running container for version %s — skipping incremental RAG load", versionKey);
            return;
        }

        ragSqlLoader.ensureLoaded(
                quarkusVersion, projectDir,
                container.host(), container.pgPort(),
                pgDatabase, pgUser, pgPassword);
    }

    /**
     * Returns the host port mapped to PostgreSQL's 5432 inside the container.
     */
    public int getMappedPort(String quarkusVersion) {
        return getContainer(quarkusVersion).pgPort();
    }

    /**
     * Returns the host where the container is accessible.
     */
    public String getHost(String quarkusVersion) {
        return getContainer(quarkusVersion).host();
    }

    /** The embedding server's base URL, without a trailing slash. */
    public String getEmbeddingUrl() {
        if (isExternal()) {
            return embeddingUrl();
        }
        DocContainer container = getContainer(null);
        return "http://" + container.host() + ":" + container.embeddingPort();
    }

    /**
     * Returns a Docker client built from the Testcontainers transport config, so Docker/Podman
     * discovery (DOCKER_HOST, Podman sockets, Docker Desktop, ...) works as before.
     * {@link DockerClientFactory#client()} is avoided because it also starts a Ryuk reaper.
     */
    /** True once this server has created a Docker client, i.e. has tried to talk to Docker. */
    boolean dockerClientCreated() {
        return dockerClient != null;
    }

    private DockerClient docker() {
        DockerClient client = dockerClient;
        if (client == null) {
            synchronized (this) {
                client = dockerClient;
                if (client == null) {
                    client = DockerClientProviderStrategy.getClientForConfig(
                            DockerClientFactory.instance().getTransportConfig());
                    dockerClient = client;
                }
            }
        }
        return client;
    }

    /**
     * Records that this server used a version's container, in a one-row table in the container's
     * own database, so that every server's cleanup can tell a shared container is still wanted.
     * Rate-limited per container and done off the caller's thread.
     */
    public void markUsed(String quarkusVersion) {
        if (isExternal()) {
            // Only container cleanup reads the record, and an existing database may be read-only
            return;
        }
        DocContainer container = containers.get(quarkusVersion != null ? quarkusVersion : "default");
        if (container == null) {
            return;
        }
        long now = System.currentTimeMillis();
        Long previous = lastMarkedUsed.get(container.id());
        if (previous != null && now - previous < MARK_USED_INTERVAL_MS) {
            return;
        }
        lastMarkedUsed.put(container.id(), now);
        Thread.ofVirtual().name("doc-search-mark-used").start(() -> {
            try (Connection conn = DriverManager.getConnection(jdbcUrl(container), pgUser, pgPassword);
                    Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS " + USAGE_TABLE
                        + " (id INT PRIMARY KEY, last_used TIMESTAMPTZ NOT NULL)");
                stmt.execute("INSERT INTO " + USAGE_TABLE + " (id, last_used) VALUES (1, now()) "
                        + "ON CONFLICT (id) DO UPDATE SET last_used = now()");
            } catch (SQLException e) {
                LOG.debugf("Failed to record doc-search container use: %s", e.getMessage());
            }
        });
    }

    private String jdbcUrl(DocContainer container) {
        return "jdbc:postgresql://" + container.host() + ":" + container.pgPort() + "/" + pgDatabase;
    }

    /** Runs {@link #cleanUpIdleContainers} now and every {@link #CLEANUP_INTERVAL} after. */
    private void scheduleCleanup() {
        synchronized (warmupLock) {
            if (cleanupScheduled) {
                return;
            }
            cleanupScheduled = true;
        }
        Thread.ofPlatform().daemon(true).name("doc-search-cleanup").start(() -> {
            while (true) {
                try {
                    cleanUpIdleContainers();
                } catch (Exception e) {
                    LOG.debugf("Doc-search container cleanup failed: %s", e.getMessage());
                }
                try {
                    Thread.sleep(CLEANUP_INTERVAL.toMillis());
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
    }

    enum CleanupAction {
        KEEP,
        STOP,
        REMOVE
    }

    /**
     * What to do with a doc-search container this server is not using.
     * <ul>
     * <li>Named containers are stopped once idle for {@code stopAfter}, which frees their memory but
     * keeps their docs: a server that needs one again starts it through its normal retry. They are
     * removed once stopped for {@code removeAfter}.</li>
     * <li>Unnamed containers come from releases before containers were named and are never used by
     * this one, so they are removed once stopped, or once idle for {@code stopAfter} (a server
     * of the old release still running may be using one until then).</li>
     * </ul>
     *
     * @param idleSince when the container was last used (running) or stopped, or null if unknown
     */
    static CleanupAction cleanupAction(boolean named, boolean running, Instant idleSince, Instant now,
            Duration stopAfter, Duration removeAfter) {
        if (!named && !running) {
            return CleanupAction.REMOVE;
        }
        if (idleSince == null) {
            return CleanupAction.KEEP;
        }
        Duration idle = Duration.between(idleSince, now);
        if (running) {
            if (idle.compareTo(stopAfter) < 0) {
                return CleanupAction.KEEP;
            }
            return named ? CleanupAction.STOP : CleanupAction.REMOVE;
        }
        return idle.compareTo(removeAfter) < 0 ? CleanupAction.KEEP : CleanupAction.REMOVE;
    }

    /**
     * Stops or removes doc-search containers nobody has used for a while: those of Quarkus versions
     * no longer in use, those left behind by a change of image or Postgres settings, and those of
     * earlier releases. The containers this server uses are never touched.
     */
    void cleanUpIdleContainers() {
        cleanUpIdleContainers(Map.of(), Instant.now());
    }

    /**
     * @param extraLabels narrows the containers considered, so tests leave the machine's real ones alone
     * @param now         the time to judge idleness against
     */
    void cleanUpIdleContainers(Map<String, String> extraLabels, Instant now) {
        Map<String, String> labels = new java.util.HashMap<>(extraLabels);
        labels.put("quarkus-agent-mcp", "doc-search");
        DockerClient client = docker();
        Set<String> inUse = new HashSet<>();
        containers.values().forEach(c -> inUse.add(c.id()));
        for (var summary : client.listContainersCmd().withShowAll(true)
                .withLabelFilter(labels).exec()) {
            if (inUse.contains(summary.getId())) {
                continue;
            }
            try {
                InspectContainerResponse info = client.inspectContainerCmd(summary.getId()).exec();
                boolean running = Boolean.TRUE.equals(info.getState().getRunning());
                boolean named = isNamedDocContainer(new String[] { info.getName() });
                Instant idleSince = running ? lastUsed(info) : stoppedSince(info);
                CleanupAction action = cleanupAction(named, running, idleSince, now, idleStopAfter, idleRemoveAfter);
                String name = info.getName().replaceFirst("^/", "");
                switch (action) {
                    case STOP -> {
                        LOG.infof("Stopping doc-search container %s, unused since %s", name, idleSince);
                        client.stopContainerCmd(info.getId()).withTimeout(10).exec();
                    }
                    case REMOVE -> {
                        LOG.infof("Removing doc-search container %s (%s since %s)", name,
                                running ? "unused" : "stopped", idleSince);
                        client.removeContainerCmd(info.getId()).withForce(true).withRemoveVolumes(true).exec();
                    }
                    case KEEP -> {
                    }
                }
            } catch (NotFoundException | NotModifiedException e) {
                // Removed or stopped meanwhile, e.g. by another server's cleanup
            } catch (Exception e) {
                LOG.debugf("Failed to clean up doc-search container %s: %s", summary.getId(), e.getMessage());
            }
        }
    }

    /**
     * When a running container was last used: the later of its usage record and its start. The
     * start counts because a server reviving a stopped container only records its use once the
     * docs are loaded, and the old record would otherwise get it stopped again mid-startup.
     * Containers of earlier releases have no record, so for them only the start counts.
     */
    private Instant lastUsed(InspectContainerResponse info) {
        Instant started = parseInstant(info.getState().getStartedAt());
        Instant recorded = usageRecord(info);
        if (recorded == null) {
            return started;
        }
        return started == null || recorded.isAfter(started) ? recorded : started;
    }

    private Instant usageRecord(InspectContainerResponse info) {
        try {
            // The container may have been created with other Postgres settings than this server's,
            // e.g. before a config change, so connect with the ones it was created with
            Map<String, String> env = new java.util.HashMap<>();
            for (String entry : info.getConfig().getEnv()) {
                int eq = entry.indexOf('=');
                if (eq > 0) {
                    env.put(entry.substring(0, eq), entry.substring(eq + 1));
                }
            }
            String url = "jdbc:postgresql://" + DockerClientFactory.instance().dockerHostIpAddress() + ":"
                    + hostPort(info, PG_PORT) + "/" + env.getOrDefault("POSTGRES_DB", pgDatabase);
            Properties props = new Properties();
            props.setProperty("user", env.getOrDefault("POSTGRES_USER", pgUser));
            props.setProperty("password", env.getOrDefault("POSTGRES_PASSWORD", pgPassword));
            props.setProperty("connectTimeout", "5");
            try (Connection conn = DriverManager.getConnection(url, props);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT last_used FROM " + USAGE_TABLE + " WHERE id = 1")) {
                if (rs.next()) {
                    return rs.getTimestamp(1).toInstant();
                }
            }
        } catch (Exception e) {
            LOG.debugf("No usage record for doc-search container %s: %s", info.getName(), e.getMessage());
        }
        return null;
    }

    /** When a container stopped, or when it was created if it never ran (e.g. its first start failed). */
    private static Instant stoppedSince(InspectContainerResponse info) {
        Instant finished = parseInstant(info.getState().getFinishedAt());
        return finished != null ? finished : parseInstant(info.getCreated());
    }

    private static Instant parseInstant(String timestamp) {
        if (timestamp == null || timestamp.isBlank() || timestamp.startsWith("0001-01-01")) {
            return null;
        }
        try {
            return java.time.OffsetDateTime.parse(timestamp).toInstant();
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isNamedDocContainer(String[] names) {
        return names != null && Arrays.stream(names)
                .anyMatch(name -> name.replaceFirst("^/", "").startsWith(CONTAINER_NAME_PREFIX));
    }

    private void checkDockerAvailable() {
        if (dockerAvailable == null) {
            try {
                docker().pingCmd().exec();
                dockerAvailable = true;
            } catch (Exception e) {
                LOG.debugf("Docker availability check failed: %s", e.getMessage());
                dockerAvailable = false;
            }
            if (dockerAvailable) {
                LOG.info("Docker/Podman detected — documentation search is available");
            } else {
                LOG.info("Docker/Podman not available — documentation search will be disabled");
            }
        }
        if (!dockerAvailable) {
            throw new RuntimeException(
                    "Documentation search requires Docker or Podman, but neither is available. "
                            + "Install Docker (https://docs.docker.com/get-docker/) or Podman, "
                            + "then try again, or point agent-mcp.doc-search.pg-host at an existing doc-search "
                            + "database. All other Quarkus tools work without Docker.");
        }
    }

    static boolean supportsRagSql(String version) {
        if (version == null || version.isBlank()) {
            return true;
        }
        if (version.contains("SNAPSHOT")) {
            return true;
        }
        try {
            String[] parts = version.split("[.\\-]");
            int major = Integer.parseInt(parts[0]);
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            if (major > 3 || (major == 3 && minor > RAG_SQL_MIN_MINOR)) {
                return true;
            }
            if (major == 3 && minor == RAG_SQL_MIN_MINOR) {
                int patch = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;
                return patch >= RAG_SQL_MIN_PATCH_AT_36;
            }
            return false;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private void startGenericContainer(String versionKey) {
        LOG.infof("Starting pgvector container for Quarkus %s docs (%s)...", versionKey, image);
        DocContainer container = startOrAttach(versionKey, image, true);
        containers.put(versionKey, container);
        LOG.infof("pgvector container ready for Quarkus %s (mapped port: %d)", versionKey, container.pgPort());
    }

    private void startLegacyContainer(String versionKey, String quarkusVersion) {
        String tag = quarkusVersion != null ? quarkusVersion : defaultImageTag;

        try {
            startLegacyImage(versionKey, tag);
            return;
        } catch (Exception e) {
            if (tag.equals(defaultImageTag)) {
                throw new RuntimeException(
                        "Failed to start documentation container (" + imagePrefix + ":" + tag + "): "
                                + e.getMessage(), e);
            }
            LOG.warnf(e, "Failed to start documentation image %s:%s, falling back to %s:%s",
                    imagePrefix, tag, imagePrefix, defaultImageTag);
        }

        try {
            startLegacyImage(versionKey, defaultImageTag);
            fallbackVersions.add(versionKey);
            LOG.infof("Using '%s' docs instead of '%s' — docs may not exactly match your Quarkus version",
                    defaultImageTag, tag);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to start documentation container for Quarkus " + tag
                            + ". Tried version-specific image and fallback (" + defaultImageTag + "). "
                            + "Error: " + e.getMessage(),
                    e);
        }
    }

    private void startLegacyImage(String versionKey, String tag) {
        String fullImage = imagePrefix + ":" + tag;
        LOG.infof("Starting pgvector container with Quarkus %s docs (%s)...", versionKey, fullImage);
        DocContainer container = startOrAttach(versionKey, fullImage, false);
        containers.put(versionKey, container);
        LOG.infof("pgvector container ready for Quarkus %s (mapped port: %d)", versionKey, container.pgPort());
    }

    /**
     * Attaches to this version's container if one exists — started by an earlier or a concurrent
     * MCP server — and otherwise creates it. The container is started if it is not running, and
     * this blocks until PostgreSQL (and the embedding server, if the image has one) accept requests.
     */
    DocContainer startOrAttach(String versionKey, String fullImage, boolean hasEmbeddingServer) {
        String name = containerName(versionKey, fullImage);
        DockerClient client = docker();

        InspectContainerResponse info = inspect(name);
        if (info != null) {
            LOG.infof("Attaching to existing doc-search container %s", name);
        } else {
            pullIfMissing(fullImage);
            List<ExposedPort> ports = hasEmbeddingServer
                    ? List.of(ExposedPort.tcp(PG_PORT), ExposedPort.tcp(EMBEDDING_PORT))
                    : List.of(ExposedPort.tcp(PG_PORT));
            try {
                client.createContainerCmd(fullImage)
                        .withName(name)
                        .withEnv("POSTGRES_USER=" + pgUser, "POSTGRES_PASSWORD=" + pgPassword,
                                "POSTGRES_DB=" + pgDatabase)
                        .withLabels(Map.of(
                                "quarkus-agent-mcp", "doc-search",
                                "quarkus-agent-mcp.version", versionKey))
                        .withExposedPorts(ports)
                        .withHostConfig(HostConfig.newHostConfig().withPublishAllPorts(true))
                        .exec();
                LOG.infof("Created doc-search container %s", name);
            } catch (RuntimeException e) {
                // A concurrent MCP server may have created it first. Docker reports that as a 409
                // ConflictException, Podman as a 500, so check the name rather than the exception type.
                if (awaitContainer(name) == null) {
                    throw e;
                }
                LOG.infof("Doc-search container %s was created concurrently by another MCP server — attaching", name);
            }
            info = awaitContainer(name);
            if (info == null) {
                throw new IllegalStateException("Doc-search container " + name + " disappeared right after creation");
            }
        }

        if (!Boolean.TRUE.equals(info.getState().getRunning())) {
            try {
                client.startContainerCmd(info.getId()).exec();
            } catch (NotModifiedException e) {
                // Already started by a concurrent MCP server
            } catch (RuntimeException e) {
                throw startupFailure(name, "could not start it: " + e.getMessage(), e);
            }
            info = client.inspectContainerCmd(info.getId()).exec();
        }

        DocContainer container;
        try {
            container = new DocContainer(
                    info.getId(),
                    DockerClientFactory.instance().dockerHostIpAddress(),
                    hostPort(info, PG_PORT),
                    hasEmbeddingServer ? hostPort(info, EMBEDDING_PORT) : -1);
        } catch (IllegalStateException e) {
            // No published ports: the container exited right after starting
            throw startupFailure(name, e.getMessage(), e);
        }
        waitUntilReady(container, name);
        return container;
    }

    /**
     * A named container that will not start is left in place rather than deleted: this server
     * cannot tell a broken container from one another server is still initializing, or from a
     * Docker API hiccup. The message says how to remove it; the next warm-up retry recreates it.
     */
    private static IllegalStateException startupFailure(String name, String detail, Throwable cause) {
        return new IllegalStateException("Doc-search container " + name + " failed to start: " + detail
                + ". If this keeps happening, remove it with 'docker rm -f " + name
                + "' (or 'podman rm -f " + name + "') and it will be recreated.", cause);
    }

    String containerName(String versionKey, String fullImage) {
        return containerName(versionKey, fullImage, pgUser, pgPassword, pgDatabase);
    }

    /**
     * The container name for a version and its container config. The image and the Postgres
     * settings are hashed into it, so changing any of them (e.g. {@code pg-password}) gets a new
     * container instead of attaching to one that was initialized with the old values and would
     * reject every connection.
     */
    static String containerName(String versionKey, String fullImage, String user, String password,
            String database) {
        String config = String.join("\n", fullImage, user, password, database);
        return CONTAINER_NAME_PREFIX + versionKey.replaceAll("[^a-zA-Z0-9_.-]", "-")
                + "-" + sha256(config).substring(0, 8);
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(
                    MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required but unavailable", e);
        }
    }

    private InspectContainerResponse inspect(String nameOrId) {
        try {
            return docker().inspectContainerCmd(nameOrId).exec();
        } catch (NotFoundException e) {
            return null;
        }
    }

    /**
     * Inspects a container that should exist, allowing a few seconds for it to appear: Podman
     * reserves the name while another process is still creating the container, before it can
     * be inspected.
     */
    private InspectContainerResponse awaitContainer(String name) {
        for (int attempt = 0; attempt < 20; attempt++) {
            InspectContainerResponse info = inspect(name);
            if (info != null) {
                return info;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    private void pullIfMissing(String fullImage) {
        try {
            docker().inspectImageCmd(fullImage).exec();
            return;
        } catch (NotFoundException e) {
            // pull below
        }
        LOG.infof("Pulling image %s...", fullImage);
        DockerImageName imageName = DockerImageName.parse(fullImage);
        try {
            docker().pullImageCmd(imageName.getUnversionedPart())
                    .withTag(imageName.getVersionPart())
                    .exec(new PullImageResultCallback())
                    .awaitCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while pulling " + fullImage, e);
        }
    }

    private static int hostPort(InspectContainerResponse info, int containerPort) {
        Ports.Binding[] bindings = info.getNetworkSettings().getPorts().getBindings()
                .get(ExposedPort.tcp(containerPort));
        if (bindings == null || bindings.length == 0) {
            throw new IllegalStateException("Container " + info.getName() + " does not publish port " + containerPort);
        }
        return Arrays.stream(bindings)
                .map(Ports.Binding::getHostPortSpec)
                .filter(spec -> spec != null && !spec.isBlank())
                .mapToInt(Integer::parseInt)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Container " + info.getName() + " has no host port for " + containerPort));
    }

    private void waitUntilReady(DocContainer container, String name) {
        String embedding = container.embeddingPort() < 0 ? null
                : "http://" + container.host() + ":" + container.embeddingPort();
        awaitReady(name, container, embedding, () -> hasStopped(container),
                detail -> startupFailure(name, detail, null));
    }

    /**
     * Polls until PostgreSQL accepts TCP connections and, if given, the embedding server's health
     * check passes. The postgres entrypoint runs its init phase with TCP disabled, so a successful
     * connection means the final server is up.
     *
     * @param embeddingUrl the embedding server's base URL, or null if there is none
     * @param stopped      true once waiting is pointless because the database is gone
     * @param failure      builds the exception to throw from a description of what went wrong
     */
    private void awaitReady(String name, DocContainer container, String embeddingUrl, BooleanSupplier stopped,
            Function<String, RuntimeException> failure) {
        String jdbcUrl = "jdbc:postgresql://" + container.host() + ":" + container.pgPort() + "/" + pgDatabase;
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        long deadline = System.nanoTime() + startupTimeout.toNanos();
        String lastError = null;
        while (true) {
            try {
                Properties props = new Properties();
                props.setProperty("user", pgUser);
                props.setProperty("password", pgPassword);
                props.setProperty("connectTimeout", "5");
                try (Connection ignored = DriverManager.getConnection(jdbcUrl, props)) {
                    // connected
                }
                if (embeddingUrl == null) {
                    return;
                }
                HttpResponse<Void> response = http.send(
                        HttpRequest.newBuilder(URI.create(embeddingUrl + "/health"))
                                .timeout(Duration.ofSeconds(5)).build(),
                        HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) {
                    return;
                }
                lastError = "embedding server health check returned " + response.statusCode();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for " + name, e);
            } catch (Exception e) {
                lastError = e.getMessage();
            }
            if (stopped.getAsBoolean()) {
                throw failure.apply("it stopped during startup");
            }
            if (System.nanoTime() > deadline) {
                throw failure.apply("not ready within " + startupTimeout.toSeconds() + " s (" + lastError + ")");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for " + name, e);
            }
        }
    }

    /**
     * True only if Docker reports the container as not running. A failed inspect call is not
     * taken as a stop: startup keeps waiting until its deadline instead of giving up on a hiccup.
     */
    private boolean hasStopped(DocContainer container) {
        try {
            return !Boolean.TRUE.equals(docker().inspectContainerCmd(container.id()).exec().getState().getRunning());
        } catch (NotFoundException e) {
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void loadRagData(String versionKey, String quarkusVersion, String projectDir) {
        DocContainer container = containers.get(versionKey);
        LOG.infof("Loading RAG data for Quarkus %s (project=%s)...", versionKey, projectDir);
        long start = System.currentTimeMillis();
        ragSqlLoader.ensureLoaded(
                quarkusVersion, projectDir,
                container.host(), container.pgPort(),
                pgDatabase, pgUser, pgPassword);
        LOG.infof("Finished loading RAG data for Quarkus %s in %d ms", versionKey,
                System.currentTimeMillis() - start);
    }

    private void loadNonCoreRagData(String versionKey, String quarkusVersion, String projectDir) {
        if (projectDir == null) {
            return;
        }
        DocContainer container = containers.get(versionKey);
        if (container == null) {
            return;
        }
        try {
            ragSqlLoader.ensureLoaded(
                    quarkusVersion, projectDir,
                    container.host(), container.pgPort(),
                    pgDatabase, pgUser, pgPassword);
        } catch (Exception e) {
            LOG.warnf("Failed to load non-core extension docs: %s", e.getMessage());
        }
    }

    /**
     * Looks up the cached container without asking Docker: this sits on the per-query path.
     * Callers that then fail to reach it {@link #invalidate} it and call {@link #ensureRunning}.
     * If another search has just invalidated it, it is resolved again here rather than failing.
     */
    private DocContainer getContainer(String quarkusVersion) {
        String versionKey = quarkusVersion != null ? quarkusVersion : "default";
        DocContainer container = containers.get(versionKey);
        if (container == null) {
            ensureRunning(quarkusVersion, null);
            container = containers.get(versionKey);
        }
        if (container == null) {
            throw new IllegalStateException(
                    "pgvector container is not running for version " + versionKey + ". Call ensureRunning() first.");
        }
        return container;
    }

    /**
     * Releases container references without stopping them — containers persist across MCP
     * server restarts and may be in use by other MCP servers.
     */
    @PreDestroy
    void releaseContainerReferences() {
        for (var entry : containers.entrySet()) {
            LOG.infof("Releasing container reference for version %s (containerId: %s)",
                    entry.getKey(), entry.getValue().id());
        }
        containers.clear();
        DockerClient client = dockerClient;
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.debugf("Error closing Docker client: %s", e.getMessage());
            }
        }
    }
}
