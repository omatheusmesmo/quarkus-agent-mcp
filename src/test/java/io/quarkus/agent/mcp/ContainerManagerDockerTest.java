package io.quarkus.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.github.dockerjava.api.DockerClient;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.dockerclient.DockerClientProviderStrategy;

/**
 * Verifies that MCP servers share one doc-search container per version instead of each
 * starting their own, and that the container is not handed to a Ryuk reaper.
 */
class ContainerManagerDockerTest {

    private static final String IMAGE = "pgvector/pgvector:pg17";

    private final String versionKey = "test-" + UUID.randomUUID().toString().substring(0, 8);
    private DockerClient docker;

    @BeforeEach
    void setUp() throws InterruptedException {
        try {
            docker = DockerClientProviderStrategy.getClientForConfig(
                    DockerClientFactory.instance().getTransportConfig());
            docker.pingCmd().exec();
        } catch (RuntimeException e) {
            assumeTrue(false, "Skipped: no container runtime available");
        }
        // Some tests create containers directly, without startOrAttach pulling the image first
        try {
            docker.inspectImageCmd(IMAGE).exec();
        } catch (com.github.dockerjava.api.exception.NotFoundException e) {
            docker.pullImageCmd("pgvector/pgvector").withTag("pg17")
                    .exec(new com.github.dockerjava.api.command.PullImageResultCallback())
                    .awaitCompletion();
        }
    }

    @AfterEach
    void removeContainer() {
        if (docker != null) {
            var leftovers = docker.listContainersCmd().withShowAll(true)
                    .withLabelFilter(java.util.Map.of("quarkus-agent-mcp.version", versionKey))
                    .exec();
            for (var container : leftovers) {
                docker.removeContainerCmd(container.getId()).withForce(true).withRemoveVolumes(true).exec();
            }
        }
    }

    private static ContainerManager newManager() {
        ContainerManager manager = new ContainerManager();
        manager.pgUser = "quarkus";
        manager.pgPassword = "quarkus";
        manager.pgDatabase = "quarkus";
        return manager;
    }

    @Test
    void secondServerAttachesToTheSameContainer() {
        ContainerManager first = newManager();
        ContainerManager second = newManager();
        try {
            var a = first.startOrAttach(versionKey, IMAGE, false);
            var b = second.startOrAttach(versionKey, IMAGE, false);

            assertEquals(a.id(), b.id());
            assertEquals(a.pgPort(), b.pgPort());
            var labels = docker.inspectContainerCmd(a.id()).exec().getConfig().getLabels();
            assertEquals(versionKey, labels.get("quarkus-agent-mcp.version"));
            assertFalse(labels.containsKey(DockerClientFactory.TESTCONTAINERS_SESSION_ID_LABEL),
                    "Doc-search containers must not be reaped by Ryuk");
        } finally {
            first.releaseContainerReferences();
            second.releaseContainerReferences();
        }
    }

    @Test
    void concurrentServersShareOneContainer() throws Exception {
        ContainerManager first = newManager();
        ContainerManager second = newManager();
        try {
            var a = CompletableFuture.supplyAsync(() -> first.startOrAttach(versionKey, IMAGE, false));
            var b = CompletableFuture.supplyAsync(() -> second.startOrAttach(versionKey, IMAGE, false));

            assertEquals(a.get().id(), b.get().id());
            long matching = docker.listContainersCmd().withShowAll(true)
                    .withLabelFilter(java.util.Map.of("quarkus-agent-mcp.version", versionKey))
                    .exec().size();
            assertEquals(1, matching);
        } finally {
            first.releaseContainerReferences();
            second.releaseContainerReferences();
        }
    }

    @Test
    void stoppedContainerIsStartedAgain() {
        ContainerManager manager = newManager();
        try {
            var created = manager.startOrAttach(versionKey, IMAGE, false);
            docker.stopContainerCmd(created.id()).exec();

            var restarted = manager.startOrAttach(versionKey, IMAGE, false);

            assertEquals(created.id(), restarted.id());
            assertTrue(docker.inspectContainerCmd(restarted.id()).exec().getState().getRunning());
        } finally {
            manager.releaseContainerReferences();
        }
    }

    @Test
    void cleanupStopsIdleContainersAndRemovesThoseOfEarlierReleases() throws Exception {
        ContainerManager user = newManager();
        ContainerManager idle = newManager();
        idle.pgDatabase = "idle"; // a different config, so a second named container
        try {
            var inUse = user.startOrAttach(versionKey, IMAGE, false);
            user.containers.put(versionKey, inUse);
            var idleContainer = idle.startOrAttach(versionKey, IMAGE, false);
            // A stopped container of an earlier release
            String legacy = docker.createContainerCmd(IMAGE)
                    .withLabels(java.util.Map.of(
                            "quarkus-agent-mcp", "doc-search",
                            "quarkus-agent-mcp.version", versionKey))
                    .exec().getId();

            // Judged three days from now, so both containers look idle since they started
            user.cleanUpIdleContainers(java.util.Map.of("quarkus-agent-mcp.version", versionKey),
                    java.time.Instant.now().plus(java.time.Duration.ofDays(3)));

            assertTrue(docker.inspectContainerCmd(inUse.id()).exec().getState().getRunning(),
                    "The cleaning server's own container must be left alone");
            assertFalse(docker.inspectContainerCmd(idleContainer.id()).exec().getState().getRunning(),
                    "An idle named container is stopped");
            assertEquals(0, docker.listContainersCmd().withShowAll(true)
                    .withIdFilter(java.util.List.of(legacy)).exec().size(),
                    "A stopped container of an earlier release is removed");

            // A server that needs the stopped container again just starts it
            var revived = idle.startOrAttach(versionKey, IMAGE, false);
            assertEquals(idleContainer.id(), revived.id());
        } finally {
            user.releaseContainerReferences();
            idle.releaseContainerReferences();
        }
    }

    @Test
    void changedPostgresSettingsGetANewContainer() {
        ContainerManager first = newManager();
        ContainerManager second = newManager();
        second.pgPassword = "changed";
        try {
            var a = first.startOrAttach(versionKey, IMAGE, false);
            var b = second.startOrAttach(versionKey, IMAGE, false);

            assertFalse(a.id().equals(b.id()), "A container initialized with the old password must not be reused");
        } finally {
            first.releaseContainerReferences();
            second.releaseContainerReferences();
        }
    }

    @Test
    void brokenNamedContainerIsKeptAndTheErrorSaysHowToRemoveIt() {
        ContainerManager manager = newManager();
        String name = manager.containerName(versionKey, IMAGE);
        // Occupies the name with a container that exits straight away, like one with a broken data dir
        String broken = docker.createContainerCmd(IMAGE)
                .withName(name)
                .withCmd("false")
                .withLabels(java.util.Map.of("quarkus-agent-mcp.version", versionKey))
                .exec().getId();
        try {
            var failure = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.startOrAttach(versionKey, IMAGE, false));

            assertTrue(failure.getMessage().contains("docker rm -f " + name), failure.getMessage());
            assertEquals(broken, docker.inspectContainerCmd(name).exec().getId(),
                    "A server must never delete a named container it cannot start");
        } finally {
            manager.releaseContainerReferences();
        }
    }

    @Test
    void invalidateResolvesTheContainerAgain() {
        // A pre-3.36.1 version takes the legacy path; its per-version tag does not exist for
        // pgvector/pgvector, so it falls back to the pg17 tag
        String version = "3.20." + (1000 + new java.util.Random().nextInt(100000));
        ContainerManager manager = legacyManager();
        try {
            manager.ensureRunning(version, null);
            int actualPort = manager.getMappedPort(version);

            // Stale cache, as after another server restarted the container on new ports
            var cached = manager.containers.get(version);
            manager.containers.put(version, new ContainerManager.DocContainer(
                    cached.id(), cached.host(), actualPort + 1, cached.embeddingPort()));
            manager.invalidate(version);
            manager.ensureRunning(version, null);
            assertEquals(actualPort, manager.getMappedPort(version));

            // Invalidated by a concurrent search: a lookup resolves it again instead of failing
            manager.invalidate(version);
            assertEquals(actualPort, manager.getMappedPort(version));

            // Container removed underneath the server: resolving again recreates it
            docker.removeContainerCmd(cached.id()).withForce(true).withRemoveVolumes(true).exec();
            manager.invalidate(version);
            manager.ensureRunning(version, null);
            var recreated = manager.containers.get(version);
            assertFalse(cached.id().equals(recreated.id()));
            assertTrue(docker.inspectContainerCmd(recreated.id()).exec().getState().getRunning());
        } finally {
            manager.releaseContainerReferences();
            for (var container : docker.listContainersCmd().withShowAll(true)
                    .withLabelFilter(java.util.Map.of("quarkus-agent-mcp.version", version)).exec()) {
                docker.removeContainerCmd(container.getId()).withForce(true).withRemoveVolumes(true).exec();
            }
        }
    }

    private static ContainerManager legacyManager() {
        ContainerManager manager = newManager();
        manager.imagePrefix = "pgvector/pgvector";
        manager.defaultImageTag = "pg17";
        return manager;
    }

    @Test
    void cleanupRemovesANamedContainerThatNeverStarted() {
        ContainerManager manager = newManager();
        try {
            // e.g. its first start failed, then the config changed so nobody uses this name again
            String neverStarted = docker.createContainerCmd(IMAGE)
                    .withName(manager.containerName(versionKey, IMAGE))
                    .withLabels(java.util.Map.of(
                            "quarkus-agent-mcp", "doc-search",
                            "quarkus-agent-mcp.version", versionKey))
                    .exec().getId();

            manager.cleanUpIdleContainers(java.util.Map.of("quarkus-agent-mcp.version", versionKey),
                    java.time.Instant.now().plus(java.time.Duration.ofDays(15)));

            assertEquals(0, docker.listContainersCmd().withShowAll(true)
                    .withIdFilter(java.util.List.of(neverStarted)).exec().size());
        } finally {
            manager.releaseContainerReferences();
        }
    }

    @Test
    void existingDatabaseIsUsedWithoutStartingAContainer() throws Exception {
        // Someone else's doc-search database, and an embedding server that only answers health checks
        ContainerManager owner = newManager();
        var database = owner.startOrAttach(versionKey, IMAGE, false);
        var embedding = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("localhost", 0), 0);
        embedding.createContext("/health", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        embedding.start();
        String embeddingUrl = "http://localhost:" + embedding.getAddress().getPort();

        ContainerManager manager = ContainerManagerTest.externalManager(database.host(), database.pgPort());
        manager.externalEmbeddingUrl = java.util.Optional.of(embeddingUrl + "/");
        manager.readOnly = true;
        String otherVersion = versionKey + "-external";
        try {
            manager.ensureRunning(otherVersion, null);

            assertEquals(database.host(), manager.getHost(otherVersion));
            assertEquals(database.pgPort(), manager.getMappedPort(otherVersion));
            assertEquals(embeddingUrl, manager.getEmbeddingUrl());
            assertEquals(0, docker.listContainersCmd().withShowAll(true)
                    .withLabelFilter(java.util.Map.of("quarkus-agent-mcp.version", otherVersion))
                    .exec().size(), "No container may be started for an existing database");
            assertFalse(manager.dockerClientCreated(), "Docker must not be contacted for an existing database");

            // Reconnecting after a failed search resolves the same database again
            manager.invalidate(otherVersion);
            manager.ensureRunning(otherVersion, null);
            assertEquals(database.pgPort(), manager.getMappedPort(otherVersion));
        } finally {
            embedding.stop(0);
            manager.releaseContainerReferences();
            owner.releaseContainerReferences();
        }
    }
}

