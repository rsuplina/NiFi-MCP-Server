# NiFi MCP Server (via Knox)

Model Context Protocol server providing read-only access to Apache NiFi via Apache Knox.

**Works with both NiFi 1.x and 2.x** - version detection is automatic.

## Features

- **Version detection**: Automatically detects NiFi 1.x vs 2.x and adapts behavior
- **Read-only tools**:
  - `get_nifi_version()` - detect version and build info
  - `get_root_process_group()` - get root process group
  - `list_processors(process_group_id)` - list processors in a PG
  - `list_connections(process_group_id)` - list connections in a PG
  - `get_bulletins(after_ms?)` - get recent bulletins
  - `list_parameter_contexts()` - list parameter contexts
  - `get_controller_services(process_group_id?)` - get controller services
- **Knox auth**: Bearer token, Cookie, Passcode token, or Basic → token exchange
- **Transport**: stdio (HTTP/SSE can be added later)

## Configuration (env)

- Connection:
  - `KNOX_GATEWAY_URL` (e.g., `https://knox.example.com/gateway/default`)
  - `NIFI_API_BASE` (optional override)
- Auth (choose one):
  - `KNOX_TOKEN` or `KNOX_COOKIE` or `KNOX_PASSCODE_TOKEN`
  - or `KNOX_USER` + `KNOX_PASSWORD` (+ optional `KNOX_TOKEN_ENDPOINT`)
- TLS/HTTP:
  - `KNOX_VERIFY_SSL=true|false` or `KNOX_CA_BUNDLE=/path/to/ca.pem`
  - `HTTP_TIMEOUT_SECONDS`, `HTTP_MAX_RETRIES`
- Behavior:
  - `NIFI_READONLY=true` (default)

## Run (stdio)

With uv:

```bash
cd /Users/ktalbert/Documents/GitHub/NiFi-MCP-Server
uvx --from . run-server
```

Environment example:

```bash
export MCP_TRANSPORT=stdio
export KNOX_GATEWAY_URL="https://knox.example.com/gateway/default"
export KNOX_TOKEN="<your_knox_bearer_token>"
```

## Claude Desktop config

### Option 1: Local installation (Recommended)

First, clone and install locally:
```bash
git clone https://github.com/kevinbtalbert/nifi-mcp-server.git
cd nifi-mcp-server
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Then in `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "nifi-mcp-server": {
      "command": "/Users/ktalbert/Documents/GitHub/NiFi-MCP-Server/.venv/bin/python",
      "args": [
        "-m",
        "nifi_mcp_server.server"
      ],
      "env": {
        "MCP_TRANSPORT": "stdio",
        "NIFI_API_BASE": "https://nifi-2-dh-management0.cgsi-dem.prep-j1tk.a3.cloudera.site/nifi-2-dh/cdp-proxy/nifi-app/nifi-api",
        "KNOX_TOKEN": "<your_knox_bearer_token>",
        "NIFI_READONLY": "true"
      }
    }
  }
}
```

### Option 2: Using uvx (For use by Cloudera Agent Studio)

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

## Next steps

- Add connections/controller services/provenance tools
- Optional start/stop actions behind allowlist
- HTTP/SSE transports
