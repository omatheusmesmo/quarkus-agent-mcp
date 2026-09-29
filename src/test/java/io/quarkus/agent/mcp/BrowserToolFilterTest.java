package io.quarkus.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkiverse.mcp.server.FilterContext;
import io.quarkiverse.mcp.server.ToolManager.ToolInfo;
import java.lang.reflect.Proxy;
import org.junit.jupiter.api.Test;

class BrowserToolFilterTest {

    @Test
    void browserToolIsListedByDefault() {
        BrowserToolFilter filter = new BrowserToolFilter();

        assertTrue(filter.test(tool("quarkus_browser"), (FilterContext) null));
    }

    @Test
    void disablingTheBrowserHidesOnlyTheBrowserTool() {
        BrowserToolFilter filter = new BrowserToolFilter();
        filter.browserEnabled = false;

        assertFalse(filter.test(tool("quarkus_browser"), (FilterContext) null));
        assertTrue(filter.test(tool("quarkus_start"), (FilterContext) null));
    }

    private static ToolInfo tool(String name) {
        return (ToolInfo) Proxy.newProxyInstance(ToolInfo.class.getClassLoader(), new Class<?>[] { ToolInfo.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("name")) {
                        return name;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
