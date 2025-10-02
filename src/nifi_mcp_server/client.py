from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List
from functools import lru_cache

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


class NiFiError(Exception):
	pass


class NiFiClient:
	def __init__(self, base_url: str, session: requests.Session, timeout_seconds: int = 30, proxy_context_path: Optional[str] = None):
		self.base_url = base_url.rstrip("/")
		self.session = session
		self.timeout = timeout_seconds
		self._version_info: Optional[Tuple[int, int, int]] = None
		self.proxy_context_path = proxy_context_path
		
		# Add CDP proxy headers if configured
		if self.proxy_context_path:
			self.session.headers.update({'X-ProxyContextPath': self.proxy_context_path})

	def _url(self, path: str) -> str:
		return f"{self.base_url}/{path.lstrip('/')}"

	@retry(
		retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
		wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
		stop=stop_after_attempt(3),
		reraise=True,
	)
	def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
		resp = self.session.get(self._url(path), params=params, timeout=self.timeout)
		if resp.status_code == 401:
			raise requests.HTTPError("Unauthorized", response=resp)
		if resp.status_code == 403:
			raise requests.HTTPError("Forbidden", response=resp)
		resp.raise_for_status()
		return resp.json()

	@retry(
		retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
		wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
		stop=stop_after_attempt(3),
		reraise=True,
	)
	def _put(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
		resp = self.session.put(self._url(path), json=data, timeout=self.timeout)
		if resp.status_code == 401:
			raise requests.HTTPError("Unauthorized", response=resp)
		if resp.status_code == 403:
			raise requests.HTTPError("Forbidden", response=resp)
		resp.raise_for_status()
		return resp.json()

	@retry(
		retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
		wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
		stop=stop_after_attempt(3),
		reraise=True,
	)
	def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
		resp = self.session.post(self._url(path), json=data, timeout=self.timeout)
		if resp.status_code == 401:
			raise requests.HTTPError("Unauthorized", response=resp)
		if resp.status_code == 403:
			raise requests.HTTPError("Forbidden", response=resp)
		resp.raise_for_status()
		return resp.json()

	@retry(
		retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout)),
		wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
		stop=stop_after_attempt(3),
		reraise=True,
	)
	def _delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
		resp = self.session.delete(self._url(path), params=params, timeout=self.timeout)
		if resp.status_code == 401:
			raise requests.HTTPError("Unauthorized", response=resp)
		if resp.status_code == 403:
			raise requests.HTTPError("Forbidden", response=resp)
		resp.raise_for_status()
		return resp.json() if resp.content else {}

	def get_version_info(self) -> Dict[str, Any]:
		"""Get NiFi version and build information."""
		return self._get("flow/about")

	def get_version_tuple(self) -> Tuple[int, int, int]:
		"""Get NiFi version as (major, minor, patch) tuple for version detection."""
		if self._version_info is None:
			try:
				about = self.get_version_info()
				version_str = about.get("about", {}).get("version", "1.0.0")
				# Parse version string like "2.0.0" or "1.23.2"
				parts = version_str.split(".")[:3]
				self._version_info = tuple(int(p) for p in parts)
			except Exception:
				# Default to 1.x if detection fails
				self._version_info = (1, 0, 0)
		return self._version_info

	def is_nifi_2x(self) -> bool:
		"""Check if this is NiFi 2.x or later."""
		major, _, _ = self.get_version_tuple()
		return major >= 2

	def get_root_process_group(self) -> Dict[str, Any]:
		return self._get("flow/process-groups/root")

	def get_process_group(self, pg_id: str) -> Dict[str, Any]:
		return self._get(f"flow/process-groups/{pg_id}")
	
	def create_process_group(self, parent_id: str, name: str, position_x: float = 0.0, position_y: float = 0.0) -> Dict[str, Any]:
		"""Create a process group within a parent process group. Requires NIFI_READONLY=false."""
		return self._post(
			f"process-groups/{parent_id}/process-groups",
			{
				"revision": {"version": 0},
				"component": {
					"name": name,
					"position": {"x": position_x, "y": position_y}
				}
			}
		)
	
	def update_process_group(self, pg_id: str, version: int, name: str) -> Dict[str, Any]:
		"""Update process group name. Requires NIFI_READONLY=false."""
		return self._put(
			f"process-groups/{pg_id}",
			{
				"revision": {"version": version},
				"component": {
					"id": pg_id,
					"name": name
				}
			}
		)
	
	def delete_process_group(self, pg_id: str, version: int, disconnected_ack: bool = False) -> Dict[str, Any]:
		"""Delete a process group. Requires NIFI_READONLY=false.
		
		Note: Process group must be empty (no processors, connections, or child groups).
		"""
		return self._delete(
			f"process-groups/{pg_id}",
			params={"version": version, "disconnectedNodeAcknowledged": str(disconnected_ack).lower()}
		)

	def list_processors(self, pg_id: str) -> Dict[str, Any]:
		return self._get(f"process-groups/{pg_id}/processors")

	def list_connections(self, pg_id: str) -> Dict[str, Any]:
		return self._get(f"process-groups/{pg_id}/connections")

	def get_processor(self, processor_id: str) -> Dict[str, Any]:
		return self._get(f"processors/{processor_id}")

	def get_bulletins(self, since_ms: Optional[int] = None) -> Dict[str, Any]:
		params = {"after": since_ms} if since_ms else None
		return self._get("flow/bulletin-board", params=params)

	def list_parameter_contexts(self) -> Dict[str, Any]:
		"""List parameter contexts (both 1.x and 2.x, schema may differ slightly)."""
		return self._get("flow/parameter-contexts")
	
	def get_parameter_context(self, context_id: str) -> Dict[str, Any]:
		"""Get a specific parameter context with its parameters."""
		return self._get(f"parameter-contexts/{context_id}")
	
	def create_parameter_context(self, name: str, description: str = "", parameters: List[Dict[str, Any]] = None) -> Dict[str, Any]:
		"""Create a parameter context. Requires NIFI_READONLY=false.
		
		Parameters should be a list of dicts with 'name', 'value', 'sensitive', and optional 'description'.
		Example: [{"name": "db.host", "value": "localhost", "sensitive": False}]
		"""
		param_list = []
		if parameters:
			for param in parameters:
				param_entry = {
					"parameter": {
						"name": param.get("name"),
						"value": param.get("value"),
						"sensitive": param.get("sensitive", False)
					}
				}
				if param.get("description"):
					param_entry["parameter"]["description"] = param["description"]
				param_list.append(param_entry)
		
		return self._post(
			"parameter-contexts",
			{
				"revision": {"version": 0},
				"component": {
					"name": name,
					"description": description,
					"parameters": param_list
				}
			}
		)
	
	def update_parameter_context(self, context_id: str, version: int, name: str = None, description: str = None, parameters: List[Dict[str, Any]] = None) -> Dict[str, Any]:
		"""Update a parameter context. Requires NIFI_READONLY=false."""
		component = {"id": context_id}
		if name:
			component["name"] = name
		if description is not None:
			component["description"] = description
		if parameters:
			param_list = []
			for param in parameters:
				param_entry = {
					"parameter": {
						"name": param.get("name"),
						"value": param.get("value"),
						"sensitive": param.get("sensitive", False)
					}
				}
				if param.get("description"):
					param_entry["parameter"]["description"] = param["description"]
				param_list.append(param_entry)
			component["parameters"] = param_list
		
		return self._put(
			f"parameter-contexts/{context_id}",
			{"revision": {"version": version}, "component": component}
		)
	
	def delete_parameter_context(self, context_id: str, version: int) -> Dict[str, Any]:
		"""Delete a parameter context. Requires NIFI_READONLY=false.
		
		Note: Context must not be referenced by any process groups.
		"""
		return self._delete(
			f"parameter-contexts/{context_id}",
			params={"version": version}
		)

	def get_controller_services(self, pg_id: Optional[str] = None) -> Dict[str, Any]:
		"""Get controller services. If pg_id is None, gets controller-level services."""
		if pg_id:
			return self._get(f"flow/process-groups/{pg_id}/controller-services")
		return self._get("flow/controller/controller-services")

	# ===== Additional Read-Only Methods =====
	
	def get_processor_types(self) -> Dict[str, Any]:
		"""Get available processor types."""
		return self._get("flow/processor-types")
	
	def search_flow(self, query: str) -> Dict[str, Any]:
		"""Search the flow for components matching a query."""
		return self._get("flow/search-results", params={"q": query})
	
	def get_connection(self, connection_id: str) -> Dict[str, Any]:
		"""Get details about a specific connection."""
		return self._get(f"connections/{connection_id}")
	
	def get_input_ports(self, pg_id: str) -> Dict[str, Any]:
		"""Get input ports for a process group."""
		return self._get(f"process-groups/{pg_id}/input-ports")
	
	def get_output_ports(self, pg_id: str) -> Dict[str, Any]:
		"""Get output ports for a process group."""
		return self._get(f"process-groups/{pg_id}/output-ports")
	
	def create_input_port(self, pg_id: str, name: str, position_x: float = 0.0, position_y: float = 0.0) -> Dict[str, Any]:
		"""Create an input port for receiving data from parent process group. Requires NIFI_READONLY=false."""
		return self._post(
			f"process-groups/{pg_id}/input-ports",
			{
				"revision": {"version": 0},
				"component": {
					"name": name,
					"position": {"x": position_x, "y": position_y}
				}
			}
		)
	
	def create_output_port(self, pg_id: str, name: str, position_x: float = 0.0, position_y: float = 0.0) -> Dict[str, Any]:
		"""Create an output port for sending data to parent process group. Requires NIFI_READONLY=false."""
		return self._post(
			f"process-groups/{pg_id}/output-ports",
			{
				"revision": {"version": 0},
				"component": {
					"name": name,
					"position": {"x": position_x, "y": position_y}
				}
			}
		)
	
	def update_input_port(self, port_id: str, version: int, name: str, state: str = None) -> Dict[str, Any]:
		"""Update input port. Requires NIFI_READONLY=false."""
		component = {"id": port_id, "name": name}
		if state:
			component["state"] = state
		return self._put(
			f"input-ports/{port_id}",
			{"revision": {"version": version}, "component": component}
		)
	
	def update_output_port(self, port_id: str, version: int, name: str, state: str = None) -> Dict[str, Any]:
		"""Update output port. Requires NIFI_READONLY=false."""
		component = {"id": port_id, "name": name}
		if state:
			component["state"] = state
		return self._put(
			f"output-ports/{port_id}",
			{"revision": {"version": version}, "component": component}
		)
	
	def delete_input_port(self, port_id: str, version: int, disconnected_ack: bool = False) -> Dict[str, Any]:
		"""Delete an input port. Requires NIFI_READONLY=false."""
		return self._delete(
			f"input-ports/{port_id}",
			params={"version": version, "disconnectedNodeAcknowledged": str(disconnected_ack).lower()}
		)
	
	def delete_output_port(self, port_id: str, version: int, disconnected_ack: bool = False) -> Dict[str, Any]:
		"""Delete an output port. Requires NIFI_READONLY=false."""
		return self._delete(
			f"output-ports/{port_id}",
			params={"version": version, "disconnectedNodeAcknowledged": str(disconnected_ack).lower()}
		)
	
	def start_input_port(self, port_id: str, version: int) -> Dict[str, Any]:
		"""Start an input port to enable data flow. Requires NIFI_READONLY=false."""
		return self._put(
			f"input-ports/{port_id}/run-status",
			{
				"revision": {"version": version},
				"state": "RUNNING"
			}
		)
	
	def stop_input_port(self, port_id: str, version: int) -> Dict[str, Any]:
		"""Stop an input port to disable data flow. Requires NIFI_READONLY=false."""
		return self._put(
			f"input-ports/{port_id}/run-status",
			{
				"revision": {"version": version},
				"state": "STOPPED"
			}
		)
	
	def start_output_port(self, port_id: str, version: int) -> Dict[str, Any]:
		"""Start an output port to enable data flow. Requires NIFI_READONLY=false."""
		return self._put(
			f"output-ports/{port_id}/run-status",
			{
				"revision": {"version": version},
				"state": "RUNNING"
			}
		)
	
	def stop_output_port(self, port_id: str, version: int) -> Dict[str, Any]:
		"""Stop an output port to disable data flow. Requires NIFI_READONLY=false."""
		return self._put(
			f"output-ports/{port_id}/run-status",
			{
				"revision": {"version": version},
				"state": "STOPPED"
			}
		)
	
	def apply_parameter_context_to_process_group(self, pg_id: str, pg_version: int, context_id: str) -> Dict[str, Any]:
		"""Apply a parameter context to a process group. Requires NIFI_READONLY=false.
		
		This enables the process group and its processors to use parameters from the context.
		"""
		return self._put(
			f"process-groups/{pg_id}",
			{
				"revision": {"version": pg_version},
				"component": {
					"id": pg_id,
					"parameterContext": {
						"id": context_id
					}
				}
			}
		)

	# ===== Write Methods (require NIFI_READONLY=false) =====
	
	def start_processor(self, processor_id: str, version: int) -> Dict[str, Any]:
		"""Start a processor. Requires NIFI_READONLY=false."""
		return self._put(
			f"processors/{processor_id}/run-status",
			{"revision": {"version": version}, "state": "RUNNING", "disconnectedNodeAcknowledged": False}
		)
	
	def stop_processor(self, processor_id: str, version: int) -> Dict[str, Any]:
		"""Stop a processor. Requires NIFI_READONLY=false."""
		return self._put(
			f"processors/{processor_id}/run-status",
			{"revision": {"version": version}, "state": "STOPPED", "disconnectedNodeAcknowledged": False}
		)
	
	def create_processor(
		self,
		pg_id: str,
		processor_type: str,
		name: str,
		position_x: float = 0.0,
		position_y: float = 0.0
	) -> Dict[str, Any]:
		"""Create a new processor. Requires NIFI_READONLY=false."""
		return self._post(
			f"process-groups/{pg_id}/processors",
			{
				"revision": {"version": 0},
				"component": {
					"type": processor_type,
					"name": name,
					"position": {"x": position_x, "y": position_y}
				}
			}
		)
	
	def update_processor(
		self,
		processor_id: str,
		version: int,
		config: Dict[str, Any]
	) -> Dict[str, Any]:
		"""Update processor configuration. Requires NIFI_READONLY=false."""
		return self._put(
			f"processors/{processor_id}",
			{
				"revision": {"version": version},
				"component": config
			}
		)
	
	def delete_processor(self, processor_id: str, version: int, disconnected_ack: bool = False) -> Dict[str, Any]:
		"""Delete a processor. Requires NIFI_READONLY=false."""
		return self._delete(
			f"processors/{processor_id}",
			params={"version": version, "disconnectedNodeAcknowledged": str(disconnected_ack).lower()}
		)
	
	def create_connection(
		self,
		pg_id: str,
		source_id: str,
		source_type: str,
		destination_id: str,
		destination_type: str,
		relationships: list[str]
	) -> Dict[str, Any]:
		"""Create a connection between components. Requires NIFI_READONLY=false."""
		return self._post(
			f"process-groups/{pg_id}/connections",
			{
				"revision": {"version": 0},
				"component": {
					"source": {"id": source_id, "groupId": pg_id, "type": source_type},
					"destination": {"id": destination_id, "groupId": pg_id, "type": destination_type},
					"selectedRelationships": relationships
				}
			}
		)
	
	def delete_connection(self, connection_id: str, version: int, disconnected_ack: bool = False) -> Dict[str, Any]:
		"""Delete a connection. Requires NIFI_READONLY=false.
		
		Note: Connections with flowfiles in the queue cannot be deleted.
		Use empty_connection_queue() first if needed.
		"""
		return self._delete(
			f"connections/{connection_id}",
			params={"version": version, "disconnectedNodeAcknowledged": str(disconnected_ack).lower()}
		)
	
	def empty_connection_queue(self, connection_id: str) -> Dict[str, Any]:
		"""Drop all flowfiles from a connection's queue. Requires NIFI_READONLY=false.
		
		Warning: This permanently deletes flowfiles. Use with caution.
		"""
		return self._post(
			f"flowfile-queues/{connection_id}/drop-requests",
			{}
		)
	
	def enable_controller_service(self, service_id: str, version: int) -> Dict[str, Any]:
		"""Enable a controller service. Requires NIFI_READONLY=false."""
		return self._put(
			f"controller-services/{service_id}/run-status",
			{"revision": {"version": version}, "state": "ENABLED"}
		)
	
	def disable_controller_service(self, service_id: str, version: int) -> Dict[str, Any]:
		"""Disable a controller service. Requires NIFI_READONLY=false."""
		return self._put(
			f"controller-services/{service_id}/run-status",
			{"revision": {"version": version}, "state": "DISABLED"}
		)
	
	def create_controller_service(self, pg_id: str, service_type: str, name: str) -> Dict[str, Any]:
		"""Create a controller service in a process group. Requires NIFI_READONLY=false."""
		return self._post(
			f"process-groups/{pg_id}/controller-services",
			{
				"revision": {"version": 0},
				"component": {
					"type": service_type,
					"name": name
				}
			}
		)
	
	def update_controller_service(self, service_id: str, version: int, properties: Dict[str, str]) -> Dict[str, Any]:
		"""Update controller service properties. Requires NIFI_READONLY=false."""
		return self._put(
			f"controller-services/{service_id}",
			{
				"revision": {"version": version},
				"component": {
					"id": service_id,
					"properties": properties
				}
			}
		)
	
	def get_controller_service(self, service_id: str) -> Dict[str, Any]:
		"""Get controller service details including properties and state."""
		return self._get(f"controller-services/{service_id}")
	
	def delete_controller_service(self, service_id: str, version: int, disconnected_ack: bool = False) -> Dict[str, Any]:
		"""Delete a controller service. Requires NIFI_READONLY=false.
		
		Note: Service must be disabled and not referenced by any processors.
		"""
		return self._delete(
			f"controller-services/{service_id}",
			params={"version": version, "disconnectedNodeAcknowledged": str(disconnected_ack).lower()}
		)
	
	# ===== Helper Methods for Common Patterns =====
	
	def get_processor_state(self, processor_id: str) -> str:
		"""Get just the state of a processor (RUNNING, STOPPED, etc.) without full details.
		
		Returns the state as a string: RUNNING, STOPPED, DISABLED, etc.
		"""
		proc = self.get_processor(processor_id)
		return proc['component']['state']
	
	def get_connection_queue_size(self, connection_id: str) -> Dict[str, int]:
		"""Get queue size for a connection (flowfile count and byte count).
		
		Returns dict with 'flowFilesQueued' and 'bytesQueued'.
		"""
		conn = self.get_connection(connection_id)
		snapshot = conn['status']['aggregateSnapshot']
		return {
			'flowFilesQueued': snapshot.get('flowFilesQueued', 0),
			'bytesQueued': snapshot.get('bytesQueued', 0)
		}
	
	def is_connection_empty(self, connection_id: str) -> bool:
		"""Check if a connection has an empty queue (safe to delete).
		
		Returns True if no flowfiles are queued, False otherwise.
		"""
		queue_size = self.get_connection_queue_size(connection_id)
		return queue_size['flowFilesQueued'] == 0
	
	def get_process_group_summary(self, pg_id: str) -> Dict[str, Any]:
		"""Get summary statistics for a process group.
		
		Returns counts of processors (by state), connections, and queued flowfiles.
		"""
		pg = self.get_process_group(pg_id)
		flow = pg['processGroupFlow']['flow']
		
		processors = flow.get('processors', [])
		connections = flow.get('connections', [])
		
		# Count processors by state
		state_counts = {}
		for proc in processors:
			state = proc.get('component', {}).get('state', 'UNKNOWN')
			state_counts[state] = state_counts.get(state, 0) + 1
		
		# Sum queued flowfiles
		total_queued = 0
		total_bytes = 0
		for conn in connections:
			snapshot = conn.get('status', {}).get('aggregateSnapshot', {})
			total_queued += snapshot.get('flowFilesQueued', 0)
			total_bytes += snapshot.get('bytesQueued', 0)
		
		return {
			'processorCount': len(processors),
			'processorStates': state_counts,
			'connectionCount': len(connections),
			'totalFlowFilesQueued': total_queued,
			'totalBytesQueued': total_bytes
		}


