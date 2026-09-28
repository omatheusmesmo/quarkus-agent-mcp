package io.quarkus.agent.mcp;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Exercises the load path against a real pgvector database. The reload behaviour is
 * all database state, so the SQL itself is what needs proving: a regenerated fragment
 * has to replace its rows rather than be skipped or duplicated.
 */
class RagSqlLoaderReloadTest {

    private static final DockerImageName PGVECTOR = DockerImageName.parse("pgvector/pgvector:pg17");
    private static final String USER = "quarkus";
    private static final String PASSWORD = "quarkus";
    private static final String DATABASE = "ragtest";

    private static GenericContainer<?> postgres;
    private static String jdbcUrl;
    private static final String user = USER;
    private static final String password = PASSWORD;

    @BeforeAll
    static void startContainer() {
        assumeTrue(containerRuntimeAvailable(), "Skipped: no container runtime available");
        postgres = new GenericContainer<>(PGVECTOR)
                .withExposedPorts(5432)
                .withEnv("POSTGRES_USER", USER)
                .withEnv("POSTGRES_PASSWORD", PASSWORD)
                .withEnv("POSTGRES_DB", DATABASE)
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
        postgres.start();
        jdbcUrl = "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/" + DATABASE;
    }

    @AfterAll
    static void stopContainer() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    private static boolean containerRuntimeAvailable() {
        try {
            return org.testcontainers.DockerClientFactory.instance().isDockerAvailable();
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** A fragment shaped like the aggregated core artifact: one DELETE name, many row sources. */
    private static String aggregatedSql(String... texts) {
        StringBuilder sb = new StringBuilder(
                "DELETE FROM rag_documents WHERE metadata->>'source' = 'quarkus-documentation';\n");
        int i = 0;
        for (String text : texts) {
            sb.append("INSERT INTO rag_documents (embedding_id, embedding, text, metadata) VALUES ('")
                    .append(new java.util.UUID(0, ++i))
                    .append("', '").append(zeroVector()).append("'::vector, '")
                    .append(text).append("', '{\"source\":\"quarkus-rest\"}'::jsonb);\n");
        }
        return sb.toString();
    }

    private static String zeroVector() {
        StringBuilder sb = new StringBuilder("[0");
        sb.append(",0".repeat(383));
        return sb.append("]").toString();
    }

    private long countDocuments() throws SQLException {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM rag_documents")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static List<RagSqlLoader.StaleFragment> fragment(String sql, String version) {
        RagSqlLoader.RagFragment f = new RagSqlLoader.RagFragment("quarkus-rest", sql);
        return List.of(new RagSqlLoader.StaleFragment(f, RagSqlLoader.fingerprint(version, sql)));
    }

    @Test
    void regeneratedFragmentReplacesItsRowsInsteadOfDuplicatingThem() throws SQLException {
        RagSqlLoader loader = new RagSqlLoader();

        String before = aggregatedSql("original chunk one", "original chunk two");
        assertTrue(loader.loadSql(jdbcUrl, user, password, fragment(before, "3.21.0"), "3.21.0"));
        assertEquals(2, countDocuments());

        // Same source, regenerated upstream with richer content
        String after = aggregatedSql("richer chunk one", "richer chunk two", "richer chunk three");
        assertTrue(loader.loadSql(jdbcUrl, user, password, fragment(after, "3.21.0"), "3.21.0"));

        assertEquals(3, countDocuments(), "Reload must replace the old rows, not add to them");

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT text FROM rag_documents ORDER BY text LIMIT 1")) {
            rs.next();
            assertTrue(rs.getString(1).startsWith("richer"), "Old content should be gone");
        }
    }

    @Test
    void fingerprintsSurviveARestartSoUnchangedFragmentsAreNotReloaded() throws SQLException {
        RagSqlLoader loader = new RagSqlLoader();
        String sql = aggregatedSql("stable chunk");

        loader.loadSql(jdbcUrl, user, password, fragment(sql, "3.21.0"), "3.21.0");

        // A fresh instance, as after an MCP server restart against a reused container
        Map<String, String> seen = new RagSqlLoader().queryLoadedFingerprints(jdbcUrl, user, password);

        assertEquals(RagSqlLoader.fingerprint("3.21.0", sql), seen.get("quarkus-rest"));
        assertEquals(RagSqlLoader.fingerprint("3.21.0", sql), seen.get("quarkus-documentation"),
                "Every source the fragment writes should be tracked, not just the first");
    }

    @Test
    void aContainerPopulatedBeforeFingerprintingIsReloadedOnceAndNotDuplicated() throws SQLException {
        RagSqlLoader loader = new RagSqlLoader();

        // A container reused from an older agent-mcp: rag_documents has rows, but nothing
        // recorded what produced them, because rag_sources did not exist yet.
        String sql = aggregatedSql("chunk one", "chunk two");
        assertTrue(loader.loadSql(jdbcUrl, user, password, fragment(sql, "3.21.0"), "3.21.0"));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE rag_sources");
        }

        Map<String, String> seen = new RagSqlLoader().queryLoadedFingerprints(jdbcUrl, user, password);
        assertTrue(seen.isEmpty(), "Nothing is known about the existing rows, so everything looks stale");

        // Reloading on that basis must land on the same row count, not double it
        assertTrue(loader.loadSql(jdbcUrl, user, password, fragment(sql, "3.21.0"), "3.21.0"));
        assertEquals(2, countDocuments());

        // ...and the second run now has fingerprints, so a third start would skip the work
        assertEquals(RagSqlLoader.fingerprint("3.21.0", sql),
                new RagSqlLoader().queryLoadedFingerprints(jdbcUrl, user, password).get("quarkus-rest"));
    }

    @Test
    void aReplacedDatabaseIsReloadedByTheSameLoader() throws SQLException {
        RagSqlLoader loader = new RagSqlLoader();
        String sql = aggregatedSql("chunk one", "chunk two");
        List<RagSqlLoader.RagFragment> fragments = List.of(new RagSqlLoader.RagFragment("quarkus-rest", sql));

        loader.loadStaleFragments("3.21.0", "3.21.0", fragments, jdbcUrl, user, password);
        assertEquals(2, countDocuments());

        // As if the container was removed and recreated empty while this server kept running
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE rag_documents, rag_sources");
        }

        loader.loadStaleFragments("3.21.0", "3.21.0", fragments, jdbcUrl, user, password);
        assertEquals(2, countDocuments(), "The loader must go by what the database holds, not by what it loaded before");
    }

    @Test
    void aHeldLoadLockFailsTheLoadInsteadOfRunningItUnlocked() throws SQLException {
        RagSqlLoader loader = new RagSqlLoader();
        loader.loadLockTimeout = java.time.Duration.ofSeconds(1);
        try (Connection holder = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = holder.createStatement()) {
            // Another server holding the lock mid-load
            stmt.execute("SELECT pg_advisory_lock(" + 0x71_6167_6e74_4d43L + ")");

            var ran = new java.util.concurrent.atomic.AtomicBoolean();
            var failure = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> loader.withLoadLock(jdbcUrl, user, password, () -> ran.set(true)));

            assertTrue(failure.getMessage().contains("try again later"), failure.getMessage());
            org.junit.jupiter.api.Assertions.assertFalse(ran.get(), "Loading unlocked would bring the deadlock back");
        }

        // Released when the holder's connection closed
        var ran = new java.util.concurrent.atomic.AtomicBoolean();
        loader.withLoadLock(jdbcUrl, user, password, () -> ran.set(true));
        assertTrue(ran.get());
    }

    @Test
    void serversWantingDifferentContentDoNotKeepOverwritingEachOther() throws SQLException {
        RagSqlLoader serverA = new RagSqlLoader();
        RagSqlLoader serverB = new RagSqlLoader();
        String contentA = aggregatedSql("extension docs, version A");
        String contentB = aggregatedSql("extension docs, version B");
        var fragmentsA = List.of(new RagSqlLoader.RagFragment("quarkus-rest", contentA));
        var fragmentsB = List.of(new RagSqlLoader.RagFragment("quarkus-rest", contentB));

        serverA.loadStaleFragments("3.21.0", "3.21.0", fragmentsA, jdbcUrl, user, password);
        serverB.loadStaleFragments("3.21.0", "3.21.0", fragmentsB, jdbcUrl, user, password);
        // A's next incremental check must leave B's content alone rather than reload its own
        serverA.loadStaleFragments("3.21.0", "3.21.0", fragmentsA, jdbcUrl, user, password);

        assertEquals(RagSqlLoader.fingerprint("3.21.0", contentB),
                new RagSqlLoader().queryLoadedFingerprints(jdbcUrl, user, password).get("quarkus-rest"));
    }
}
