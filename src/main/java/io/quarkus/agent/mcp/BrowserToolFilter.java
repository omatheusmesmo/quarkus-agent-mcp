package io.quarkus.agent.mcp;

import io.quarkiverse.mcp.server.FilterContext;
import io.quarkiverse.mcp.server.ToolFilter;
import io.quarkiverse.mcp.server.ToolManager.ToolInfo;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * Leaves {@code quarkus_browser} out of the tool list when {@code agent-mcp.tools.browser.enabled}
 * is false. It opens a browser on the host, which does nothing useful when the server runs headless
 * or remote, e.g. in a sandbox.
 */
@Singleton
public class BrowserToolFilter implements ToolFilter {

    static final String BROWSER_TOOL = "quarkus_browser";

    @ConfigProperty(name = "agent-mcp.tools.browser.enabled", defaultValue = "true")
    boolean browserEnabled = true;

    @Override
    public boolean test(ToolInfo tool, FilterContext context) {
        return browserEnabled || !BROWSER_TOOL.equals(tool.name());
    }
}
