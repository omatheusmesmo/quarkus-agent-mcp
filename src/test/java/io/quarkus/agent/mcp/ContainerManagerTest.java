package io.quarkus.agent.mcp;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ContainerManagerTest {

    @Test
    void supportsRagSqlForNullAndBlank() {
        assertTrue(ContainerManager.supportsRagSql(null));
        assertTrue(ContainerManager.supportsRagSql(""));
        assertTrue(ContainerManager.supportsRagSql("  "));
    }

    @Test
    void supportsRagSqlForSnapshots() {
        assertTrue(ContainerManager.supportsRagSql("999-SNAPSHOT"));
        assertTrue(ContainerManager.supportsRagSql("3.36.0-SNAPSHOT"));
        assertTrue(ContainerManager.supportsRagSql("3.35.0-SNAPSHOT"));
    }

    @Test
    void supportsRagSqlForModernVersions() {
        assertTrue(ContainerManager.supportsRagSql("3.36.1"));
        assertTrue(ContainerManager.supportsRagSql("3.36.2"));
        assertTrue(ContainerManager.supportsRagSql("3.37.0"));
        assertTrue(ContainerManager.supportsRagSql("3.40.0"));
        assertTrue(ContainerManager.supportsRagSql("4.0.0"));
    }

    @Test
    void supportsRagSqlReturnsFalseForOlderVersions() {
        assertFalse(ContainerManager.supportsRagSql("3.36.0"));
        assertFalse(ContainerManager.supportsRagSql("3.35.0"));
        assertFalse(ContainerManager.supportsRagSql("3.35.2"));
        assertFalse(ContainerManager.supportsRagSql("3.21.0"));
        assertFalse(ContainerManager.supportsRagSql("3.0.0"));
        assertFalse(ContainerManager.supportsRagSql("2.16.0"));
    }

    @Test
    void supportsRagSqlHandlesQualifiedVersions() {
        assertFalse(ContainerManager.supportsRagSql("3.36.0.Final"));
        assertFalse(ContainerManager.supportsRagSql("3.36.0.CR1"));
        assertTrue(ContainerManager.supportsRagSql("3.36.1.Final"));
        assertFalse(ContainerManager.supportsRagSql("3.35.0.Final"));
    }

    @Test
    void supportsRagSqlReturnsFalseForUnparseable() {
        assertFalse(ContainerManager.supportsRagSql("not-a-version"));
        assertFalse(ContainerManager.supportsRagSql("abc"));
    }

    // ── warm-up outcome ─────────────────────────────────────────────────────────

    @Test
    void warmupIsNeitherReadyNorDoneBeforeAnyOutcome() {
        ContainerManager manager = new ContainerManager();

        assertFalse(manager.isDefaultWarmupDone());
        assertFalse(manager.isDefaultReady());
        assertNull(manager.getDefaultWarmupError());
    }

    @Test
    void warmupSuccessIsReadyWithNoError() {
        ContainerManager manager = new ContainerManager();

        manager.recordWarmupSuccess();

        assertTrue(manager.isDefaultWarmupDone());
        assertTrue(manager.isDefaultReady());
        assertNull(manager.getDefaultWarmupError());
    }

    @Test
    void warmupFailureIsDoneButNotReady() {
        ContainerManager manager = new ContainerManager();

        assertTrue(manager.recordWarmupFailure("boom"));

        assertTrue(manager.isDefaultWarmupDone());
        assertFalse(manager.isDefaultReady());
        assertEquals("boom", manager.getDefaultWarmupError());
    }

    @Test
    void firstFailureWinsSoALaterOneCannotMaskIt() {
        ContainerManager manager = new ContainerManager();

        assertTrue(manager.recordWarmupFailure("the real cause"));
        assertFalse(manager.recordWarmupFailure("a later, vaguer symptom"));

        assertEquals("the real cause", manager.getDefaultWarmupError());
    }

    @Test
    void lateSuccessClearsAWatchdogTimeout() {
        ContainerManager manager = new ContainerManager();

        // The watchdog gave up, but interrupting the worker is best-effort and it finished anyway
        manager.recordWarmupFailure("Warm-up timed out after 300000 ms.");
        manager.recordWarmupSuccess();

        assertTrue(manager.isDefaultReady(), "A container that did come up must not report a stale timeout");
        assertNull(manager.getDefaultWarmupError());
    }

    private static final String PGVECTOR = "ghcr.io/quarkusio/quarkus-agent-pgvector:pg17";

    @Test
    void containerNameIsStableAndDockerSafe() {
        String name = ContainerManager.containerName("3.36.1", PGVECTOR, "quarkus", "quarkus", "quarkus");
        assertEquals(name, ContainerManager.containerName("3.36.1", PGVECTOR, "quarkus", "quarkus", "quarkus"));
        assertTrue(name.startsWith(ContainerManager.CONTAINER_NAME_PREFIX + "3.36.1-"), name);
        assertTrue(name.matches("[a-zA-Z0-9][a-zA-Z0-9_.-]+"), name);
        assertTrue(ContainerManager.containerName("999+local/x", "img", "u", "p", "d")
                .matches("[a-zA-Z0-9][a-zA-Z0-9_.-]+"));
    }

    @Test
    void containerNameDiffersPerImage() {
        assertNotEquals(
                ContainerManager.containerName("default", PGVECTOR, "quarkus", "quarkus", "quarkus"),
                ContainerManager.containerName("default", "pgvector/pgvector:pg17", "quarkus", "quarkus", "quarkus"));
    }

    @Test
    void containerNameDiffersPerPostgresSettings() {
        String base = ContainerManager.containerName("default", PGVECTOR, "quarkus", "quarkus", "quarkus");
        assertNotEquals(base, ContainerManager.containerName("default", PGVECTOR, "other", "quarkus", "quarkus"));
        assertNotEquals(base, ContainerManager.containerName("default", PGVECTOR, "quarkus", "secret", "quarkus"));
        assertNotEquals(base, ContainerManager.containerName("default", PGVECTOR, "quarkus", "quarkus", "other"));
    }

    private static final java.time.Instant NOW = java.time.Instant.parse("2026-09-28T12:00:00Z");
    private static final java.time.Duration STOP_AFTER = java.time.Duration.ofHours(24);
    private static final java.time.Duration REMOVE_AFTER = java.time.Duration.ofDays(14);

    private static ContainerManager.CleanupAction action(boolean named, boolean running, java.time.Duration idle) {
        return ContainerManager.cleanupAction(named, running, idle == null ? null : NOW.minus(idle), NOW,
                STOP_AFTER, REMOVE_AFTER);
    }

    @Test
    void namedContainersAreStoppedWhenIdleAndRemovedLongAfterStopping() {
        assertEquals(ContainerManager.CleanupAction.KEEP, action(true, true, java.time.Duration.ofHours(23)));
        assertEquals(ContainerManager.CleanupAction.STOP, action(true, true, java.time.Duration.ofHours(25)));
        assertEquals(ContainerManager.CleanupAction.KEEP, action(true, false, java.time.Duration.ofDays(13)));
        assertEquals(ContainerManager.CleanupAction.REMOVE, action(true, false, java.time.Duration.ofDays(15)));
    }

    @Test
    void containersOfEarlierReleasesAreRemovedOnceStoppedOrIdle() {
        assertEquals(ContainerManager.CleanupAction.REMOVE, action(false, false, java.time.Duration.ofMinutes(1)));
        assertEquals(ContainerManager.CleanupAction.REMOVE, action(false, false, null));
        assertEquals(ContainerManager.CleanupAction.KEEP, action(false, true, java.time.Duration.ofHours(23)));
        assertEquals(ContainerManager.CleanupAction.REMOVE, action(false, true, java.time.Duration.ofHours(25)));
    }

    @Test
    void containersWithUnknownIdleTimeAreKept() {
        assertEquals(ContainerManager.CleanupAction.KEEP, action(true, true, null));
        assertEquals(ContainerManager.CleanupAction.KEEP, action(true, false, null));
    }

    @Test
    void noWarmUpRetryWhileTheTimedOutAttemptIsStillRunning() throws Exception {
        ContainerManager manager = new ContainerManager();
        Thread stuck = Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException e) {
                // released below
            }
        });
        try {
            manager.warmupWorker = stuck;
            manager.recordWarmupFailure("Warm-up timed out after 300000 ms.");

            assertFalse(manager.retryWarmUpIfFailed(),
                    "A new attempt would queue behind the stuck one and could be overwritten by it");
            assertEquals("Warm-up timed out after 300000 ms.", manager.getDefaultWarmupError());
        } finally {
            stuck.interrupt();
            stuck.join();
        }
    }
}
