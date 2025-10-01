# NiFi MCP Server (via Knox)

Model Context Protocol server providing read-only access to Apache NiFi via Apache Knox.

**Works with both NiFi 1.x and 2.x** - automatic version detection and adaptation.

## Features

- **Automatic version detection** - Detects NiFi 1.x vs 2.x and adapts behavior
- **Knox authentication** - Supports Bearer tokens, cookies, and passcode tokens for CDP deployments
- **Read-only by default** - Safe exploration of NiFi flows and configuration
- **7 MCP tools** for interacting with NiFi:
  - `get_nifi_version()` - Version and build information
  - `get_root_process_group()` - Root process group details
  - `list_processors(process_group_id)` - List processors in a process group
  - `list_connections(process_group_id)` - List connections in a process group
  - `get_bulletins(after_ms?)` - Recent bulletins and alerts
  - `list_parameter_contexts()` - Parameter contexts
  - `get_controller_services(process_group_id?)` - Controller services

## Quick Start

### For CDP NiFi deployments

Your NiFi API base URL will typically be:
```
https://<your-nifi-host>/nifi-2-dh/cdp-proxy/nifi-app/nifi-api
```

Get your Knox JWT token from the CDP UI and use it with the configurations below.

## Setup

### Option 1: Claude Desktop (Local)

1. **Clone and install:**
   ```bash
   git clone https://github.com/kevinbtalbert/nifi-mcp-server.git
   cd nifi-mcp-server
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -e .
   ```

2. **Configure Claude Desktop** - Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:
   ```json
    {
      "mcpServers": {
        "nifi-mcp-server": {
          "command": "/FULL/PATH/TO/NiFi-MCP-Server/.venv/bin/python",
          "args": [
            "-m",
            "nifi_mcp_server.server"
          ],
          "env": {
            "MCP_TRANSPORT": "stdio",
            "NIFI_API_BASE": "https://nifi-2-dh-management0.yourshere.cloudera.site/nifi-2-dh/cdp-proxy/nifi-app/nifi-api",
            "KNOX_TOKEN": "<your_knox_bearer_token>",
            "NIFI_READONLY": "true"
          }
        }
      }
    }
   ```

3. **Restart Claude Desktop** and start asking questions about your NiFi flows!

### Option 2: Direct Installation (Cloudera Agent Studio)

For use with Cloudera Agent Studio, use the `uvx` command:

```json
{
  "mcpServers": {
    "nifi-mcp-server": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/kevinbtalbert/nifi-mcp-server@main",
        "run-server"
      ],
      "env": {
        "MCP_TRANSPORT": "stdio",
        "NIFI_API_BASE": "https://nifi-2-dh-management0.yourshere.cloudera.site/nifi-2-dh/cdp-proxy/nifi-app/nifi-api",
        "KNOX_TOKEN": "<your_knox_bearer_token>",
        "NIFI_READONLY": "true"
      }
    }
  }
}
```

## Configuration Options

All configuration is done via environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `NIFI_API_BASE` | Yes* | Full NiFi API URL (e.g., `https://host/nifi-2-dh/cdp-proxy/nifi-app/nifi-api`) |
| `KNOX_TOKEN` | Yes* | Knox JWT token for authentication |
| `KNOX_GATEWAY_URL` | No | Knox gateway URL (alternative to `NIFI_API_BASE`) |
| `KNOX_COOKIE` | No | Alternative: provide full cookie string instead of token |
| `KNOX_PASSCODE_TOKEN` | No | Alternative: Knox passcode token (auto-exchanged for JWT) |
| `NIFI_READONLY` | No | Read-only mode (default: `true`) |
| `KNOX_VERIFY_SSL` | No | Verify SSL certificates (default: `true`) |
| `KNOX_CA_BUNDLE` | No | Path to CA certificate bundle |

\* Either `NIFI_API_BASE` or `KNOX_GATEWAY_URL` is required


For the NIFI_API_BASE, form using the url from Knox (less `-token`), and add the postfix `/nifi-app/nifi-api`
So, `https://nifi-2-dh-management0.yourdomain.cloudera.site/nifi-2-dh/cdp-proxy-token` becomes `https://nifi-2-dh-management0.yourdomain.cloudera.site/nifi-2-dh/cdp-proxy/nifi-app/nifi-api`

Get Knox Token from the Flow Management Datahub Knox instance:

![](/screenshots/knox-token-generation.png)


## Example Usage

Once configured, you can ask Claude questions like:

- "What version of NiFi am I running?"
- "List all processors in the root process group"
- "Show me recent bulletins"
- "What parameter contexts are configured?"
- "Tell me about the controller services"


**Using the example `"List all processors in the root process group"`, we see the following for the example NiFi Canvas:**

![](/screenshots/nifi-canvas-1.png)

![](/screenshots/nifi-readcanvas-1.png)


**Using the example, `"What version of NiFi am I running?"`, we see the following:**

![](/screenshots/nifi-version-check.png)


## License

Apache License 2.0
