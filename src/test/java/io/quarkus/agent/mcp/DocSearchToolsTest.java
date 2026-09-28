package io.quarkus.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ConnectException;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class DocSearchToolsTest {

    @Test
    void connectionFailuresAreRetried() {
        // netty's connect failure, wrapped the way the embedding client surfaces it
        assertTrue(DocSearchTools.isConnectionFailure(
                new RuntimeException(new ConnectException("Connection refused: localhost/127.0.0.1:37285"))));
        // PgVectorEmbeddingStore wraps the JDBC failure; SQLState 08001 = unable to connect
        assertTrue(DocSearchTools.isConnectionFailure(
                new RuntimeException(new SQLException("Connection to localhost:5432 refused", "08001"))));
    }

    @Test
    void otherFailuresAreNotRetried() {
        assertFalse(DocSearchTools.isConnectionFailure(
                new IllegalStateException("Another MCP server has been loading documentation for over 4 minutes")));
        assertFalse(DocSearchTools.isConnectionFailure(new RuntimeException(new TimeoutException("embedding"))));
        assertFalse(DocSearchTools.isConnectionFailure(
                new RuntimeException(new SQLException("relation does not exist", "42P01"))));
    }
}
