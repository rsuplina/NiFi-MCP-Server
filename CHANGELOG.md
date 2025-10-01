# Changelog

## v0.2.0 - Flow Building and Management

### New Features
- ✅ **23 total MCP tools** (13 read-only + 10 write operations)
- ✅ **Flow discovery tools**:
  - `get_processor_types()` - Discover available processors
  - `search_flow(query)` - Search for components
  - `get_processor_details(processor_id)` - Detailed processor info
  - `get_connection_details(connection_id)` - Connection details
  - `list_input_ports(process_group_id)` - Input ports
  - `list_output_ports(process_group_id)` - Output ports
- ✅ **Write operations** (when `NIFI_READONLY=false`):
  - `start_processor()` / `stop_processor()` - Processor lifecycle
  - `create_processor()` - Build new processors
  - `update_processor_config()` - Modify processor settings
  - `delete_processor()` - Remove processors
  - `create_connection()` / `delete_connection()` - Manage connections
  - `enable_controller_service()` / `disable_controller_service()` - Service management
- ✅ **Safety by default** - Write operations only enabled with explicit `NIFI_READONLY=false`

### Improvements
- Added HTTP PUT, POST, DELETE methods to client
- Clear documentation of read-only vs write operations
- Examples for flow building and management

## v0.1.0 - Initial Release

### Features
- ✅ **NiFi 1.x and 2.x support** with automatic version detection
- ✅ **Knox authentication** via JWT tokens (Bearer/Cookie/Passcode)
- ✅ **CDP NiFi compatibility** - Tested with CDP Data Hub NiFi 2.3.0
- ✅ **7 read-only MCP tools**:
  - `get_nifi_version()` - Version and build information
  - `get_root_process_group()` - Root process group details
  - `list_processors(process_group_id)` - List processors
  - `list_connections(process_group_id)` - List connections
  - `get_bulletins(after_ms?)` - Bulletins and alerts
  - `list_parameter_contexts()` - Parameter contexts
  - `get_controller_services(process_group_id?)` - Controller services
- ✅ **stdio transport** for Claude Desktop and other MCP clients
- ✅ **Sensitive data redaction** for safe LLM consumption
- ✅ **Retry logic** with exponential backoff

### CDP-Specific Features
- Cookie-based authentication (`hadoop-jwt`) for CDP deployments
- Automatic API path detection for `/cdp-proxy/` endpoints
- Support for Knox passcode token auto-exchange

### Tested Against
- NiFi 2.3.0.4.2.1.400-24 on CDP Data Hub
- Knox SSO with JWT authentication
- CDP proxy path: `/nifi-2-dh/cdp-proxy/nifi-app/nifi-api`
