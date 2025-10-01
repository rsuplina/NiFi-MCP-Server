from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import anyio

from .config import ServerConfig
from .auth import KnoxAuthFactory
from .client import NiFiClient


# Lazy import of MCP to give a clear error if the dependency is missing
try:
	from mcp.server import FastMCP
	from mcp.server.stdio import stdio_server
except Exception as e:  # pragma: no cover
	raise RuntimeError(
		"The 'mcp' package is required. Install with: pip install mcp"
	) from e


def _redact_sensitive(obj: Any, max_items: int = 200) -> Any:
	"""Redact common sensitive fields and truncate large collections for LLMs."""
	redact_keys = {"password", "passcode", "token", "secret", "kerberosKeytab", "sslKeystorePasswd"}
	if isinstance(obj, dict):
		redacted: Dict[str, Any] = {}
		for k, v in obj.items():
			if k.lower() in redact_keys:
				redacted[k] = "***REDACTED***"
			else:
				redacted[k] = _redact_sensitive(v, max_items)
		return redacted
	if isinstance(obj, list):
		if len(obj) > max_items:
			return [_redact_sensitive(x, max_items) for x in obj[:max_items]] + [
				{"truncated": True, "omitted_count": len(obj) - max_items}
			]
		return [_redact_sensitive(x, max_items) for x in obj]
	return obj


def build_client(config: ServerConfig) -> NiFiClient:
	verify = config.build_verify()
	nifi_base = config.build_nifi_base()
	auth = KnoxAuthFactory(
		gateway_url=config.knox_gateway_url,
		token=config.knox_token,
		cookie=config.knox_cookie,
		user=config.knox_user,
		password=config.knox_password,
		token_endpoint=config.knox_token_endpoint,
		passcode_token=config.knox_passcode_token,
		verify=verify,
	)
	session = auth.build_session()
	return NiFiClient(
		nifi_base,
		session,
		timeout_seconds=config.timeout_seconds,
		proxy_context_path=config.proxy_context_path,
	)


def create_server(nifi: NiFiClient, readonly: bool) -> FastMCP:
	app = FastMCP("nifi-mcp-server")

	@app.tool()
	async def get_nifi_version() -> Dict[str, Any]:
		"""Get NiFi version and build information. Works with both NiFi 1.x and 2.x."""
		data = nifi.get_version_info()
		version_tuple = nifi.get_version_tuple()
		is_2x = nifi.is_nifi_2x()
		return {
			"version_info": _redact_sensitive(data),
			"parsed_version": f"{version_tuple[0]}.{version_tuple[1]}.{version_tuple[2]}",
			"is_nifi_2x": is_2x,
			"major_version": version_tuple[0],
		}

	@app.tool()
	async def get_root_process_group() -> Dict[str, Any]:
		"""Return the root process group (read-only). Works with both NiFi 1.x and 2.x."""
		data = nifi.get_root_process_group()
		return _redact_sensitive(data)

	@app.tool()
	async def list_processors(process_group_id: str) -> Dict[str, Any]:
		"""List processors in a process group (read-only). Works with both NiFi 1.x and 2.x."""
		data = nifi.list_processors(process_group_id)
		return _redact_sensitive(data)

	@app.tool()
	async def list_connections(process_group_id: str) -> Dict[str, Any]:
		"""List connections in a process group (read-only). Works with both NiFi 1.x and 2.x."""
		data = nifi.list_connections(process_group_id)
		return _redact_sensitive(data)

	@app.tool()
	async def get_bulletins(after_ms: Optional[int] = None) -> Dict[str, Any]:
		"""Get recent bulletins since a timestamp in ms (read-only). Works with both NiFi 1.x and 2.x."""
		data = nifi.get_bulletins(after_ms)
		return _redact_sensitive(data)

	@app.tool()
	async def list_parameter_contexts() -> Dict[str, Any]:
		"""List parameter contexts (read-only). Works with both NiFi 1.x and 2.x. Note: schema may differ slightly between versions."""
		data = nifi.list_parameter_contexts()
		return _redact_sensitive(data)

	@app.tool()
	async def get_controller_services(process_group_id: Optional[str] = None) -> Dict[str, Any]:
		"""Get controller services (read-only). If process_group_id is None, returns controller-level services. Works with both NiFi 1.x and 2.x."""
		data = nifi.get_controller_services(process_group_id)
		return _redact_sensitive(data)

	return app


async def run_stdio() -> None:
	# For FastMCP, prefer the built-in stdio runner
	config = ServerConfig()
	nifi = build_client(config)
	server = create_server(nifi, readonly=config.readonly)
	# run() is synchronous; call the async flavor directly
	await server.run_stdio_async()


def main() -> None:
	transport = os.getenv("MCP_TRANSPORT", "stdio").lower()
	if transport != "stdio":
		# Defer to FastMCP synchronous run helper for other transports when added
		config = ServerConfig()
		nifi = build_client(config)
		server = create_server(nifi, readonly=config.readonly)
		server.run(transport=transport)
		return
	anyio.run(run_stdio)


if __name__ == "__main__":
	main()


