from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List
from functools import lru_cache
import time
import logging

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


class NiFiError(Exception):
	"""Base exception for NiFi API errors with detailed error information."""
	def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
		self.status_code = status_code
		self.response_body = response_body
		super().__init__(message)
	
	def __str__(self):
		msg = super().__str__()
		if self.status_code:
			msg = f"[{self.status_code}] {msg}"
		if self.response_body:
			msg = f"{msg}\n\nNiFi API Response:\n{self.response_body}"
		return msg


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

	def test_connection(self) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
		"""
		Test connection to NiFi and validate authentication.
		
		Returns:
			Tuple of (success: bool, message: str, version_info: Optional[Dict])
			
		Raises:
			RuntimeError: If connection fails with detailed error message
		"""
		logger.info("🧪 Testing NiFi connection...")
		
		try:
			start = time.time()
			test_resp = self.session.get(
				f"{self.base_url}/flow/about", 
				timeout=10, 
				verify=self.session.verify
			)
			elapsed = time.time() - start
			
			if test_resp.status_code == 401:
				error_msg = f"❌ NiFi connection FAILED: Authentication error (401) - Check KNOX_USER/KNOX_PASSWORD/KNOX_TOKEN (took {elapsed:.2f}s)"
				logger.error(error_msg)
				raise RuntimeError("Authentication failed - invalid credentials")
				
			elif test_resp.status_code == 403:
				error_msg = f"❌ NiFi connection FAILED: Access forbidden (403) - Credentials valid but insufficient permissions (took {elapsed:.2f}s)"
				logger.error(error_msg)
				raise RuntimeError("Access forbidden - insufficient permissions")
				
			elif test_resp.status_code != 200:
				error_msg = f"❌ NiFi connection FAILED: HTTP {test_resp.status_code} (took {elapsed:.2f}s) - Response: {test_resp.text[:200]}"
				logger.error(error_msg)
				raise RuntimeError(f"NiFi API returned HTTP {test_resp.status_code}")
				
			else:
				about = test_resp.json()
				version = about.get("about", {}).get("version", "unknown")
				success_msg = f"✅ NiFi connection successful! Connected to NiFi {version} (took {elapsed:.2f}s)"
				logger.info(success_msg)
				return True, success_msg, about
				
		except RuntimeError:
			# Re-raise authentication/permission errors
			raise
		except Exception as e:
			error_msg = f"❌ NiFi connection FAILED: {e} - Check NIFI_API_BASE and network connectivity"
			logger.error(error_msg)
			raise RuntimeError(f"Failed to connect to NiFi: {e}")

	@retry(
		retry=retry_if_exception_type((NiFiError, requests.HTTPError, requests.ConnectionError, requests.Timeout)),
		wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
		stop=stop_after_attempt(3),
		reraise=True,
	)
	def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
		resp = self.session.get(self._url(path), params=params, timeout=self.timeout)
		if not resp.ok:
			# Capture detailed error information from NiFi API
			error_body = resp.text if resp.text else "(empty response)"
			raise NiFiError(
				f"GET {path} failed: {resp.reason}",
				status_code=resp.status_code,
				response_body=error_body
			)
		return resp.json()

	@retry(
		retry=retry_if_exception_type((NiFiError, requests.HTTPError, requests.ConnectionError, requests.Timeout)),
		wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
		stop=stop_after_attempt(3),
		reraise=True,
	)
	def _put(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
		resp = self.session.put(self._url(path), json=data, timeout=self.timeout)
		if not resp.ok:
			# Capture detailed error information from NiFi API
			error_body = resp.text if resp.text else "(empty response)"
			raise NiFiError(
				f"PUT {path} failed: {resp.reason}",
				status_code=resp.status_code,
				response_body=error_body
			)
		return resp.json()

	@retry(
		retry=retry_if_exception_type((NiFiError, requests.HTTPError, requests.ConnectionError, requests.Timeout)),
		wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
		stop=stop_after_attempt(3),
		reraise=True,
	)
	def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
		resp = self.session.post(self._url(path), json=data, timeout=self.timeout)
		if not resp.ok:
			# Capture detailed error information from NiFi API
			error_body = resp.text if resp.text else "(empty response)"
			raise NiFiError(
				f"POST {path} failed: {resp.reason}",
				status_code=resp.status_code,
				response_body=error_body
			)
		return resp.json()

	@retry(
		retry=retry_if_exception_type((NiFiError, requests.HTTPError, requests.ConnectionError, requests.Timeout)),
		wait=wait_exponential(multiplier=0.5, min=0.5, max=5),
		stop=stop_after_attempt(3),
		reraise=True,
	)
	def _delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
		resp = self.session.delete(self._url(path), params=params, timeout=self.timeout)
		if not resp.ok:
			# Capture detailed error information from NiFi API
			error_body = resp.text if resp.text else "(empty response)"
			raise NiFiError(
				f"DELETE {path} failed: {resp.reason}",
				status_code=resp.status_code,
				response_body=error_body
			)
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
	
	def find_controller_services_by_type(self, pg_id: Optional[str], service_type: str) -> List[Dict[str, Any]]:
		"""Find all controller services of a specific type in a process group.
		
		Helps avoid 409 conflicts by checking if service already exists before creating.
		
		Args:
			pg_id: Process group ID (None for controller-level services)
			service_type: Fully qualified service type (e.g., "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer")
		
		Returns:
			List of matching controller services with their details.
		"""
		services_response = self.get_controller_services(pg_id)
		services = services_response.get("controllerServices", [])
		
		matches = []
		for svc in services:
			component = svc.get("component", {})
			if component.get("type") == service_type:
				matches.append(svc)
		
		return matches
	
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
	
	def start_all_processors_in_group(self, pg_id: str) -> Dict[str, Any]:
		"""Start all processors in a process group. Requires NIFI_READONLY=false.
		
		This is a bulk operation that starts all processors at once, much faster than starting individually.
		"""
		processors = self.list_processors(pg_id)
		results = {"started": [], "failed": [], "already_running": []}
		
		for proc in processors.get("processors", []):
			proc_id = proc["id"]
			proc_state = proc.get("status", {}).get("runStatus", "STOPPED")
			version = proc.get("revision", {}).get("version", 0)
			
			if proc_state == "Running":
				results["already_running"].append({"id": proc_id, "name": proc.get("component", {}).get("name")})
			else:
				try:
					self.start_processor(proc_id, version)
					results["started"].append({"id": proc_id, "name": proc.get("component", {}).get("name")})
				except Exception as e:
					results["failed"].append({"id": proc_id, "name": proc.get("component", {}).get("name"), "error": str(e)})
		
		return results
	
	def stop_all_processors_in_group(self, pg_id: str) -> Dict[str, Any]:
		"""Stop all processors in a process group. Requires NIFI_READONLY=false.
		
		This is a bulk operation that stops all processors at once, much faster than stopping individually.
		"""
		processors = self.list_processors(pg_id)
		results = {"stopped": [], "failed": [], "already_stopped": []}
		
		for proc in processors.get("processors", []):
			proc_id = proc["id"]
			proc_state = proc.get("status", {}).get("runStatus", "STOPPED")
			version = proc.get("revision", {}).get("version", 0)
			
			if proc_state == "Stopped":
				results["already_stopped"].append({"id": proc_id, "name": proc.get("component", {}).get("name")})
			else:
				try:
					self.stop_processor(proc_id, version)
					results["stopped"].append({"id": proc_id, "name": proc.get("component", {}).get("name")})
				except Exception as e:
					results["failed"].append({"id": proc_id, "name": proc.get("component", {}).get("name"), "error": str(e)})
		
		return results
	
	def enable_all_controller_services_in_group(self, pg_id: str) -> Dict[str, Any]:
		"""Enable all controller services in a process group scope. Requires NIFI_READONLY=false.
		
		This is a bulk operation that enables all services at once.
		"""
		services = self.get_controller_services(pg_id)
		results = {"enabled": [], "failed": [], "already_enabled": []}
		
		for svc in services.get("controllerServices", []):
			svc_id = svc["id"]
			svc_state = svc.get("component", {}).get("state", "DISABLED")
			version = svc.get("revision", {}).get("version", 0)
			
			if svc_state == "ENABLED":
				results["already_enabled"].append({"id": svc_id, "name": svc.get("component", {}).get("name")})
			else:
				try:
					self.enable_controller_service(svc_id, version)
					results["enabled"].append({"id": svc_id, "name": svc.get("component", {}).get("name")})
				except Exception as e:
					results["failed"].append({"id": svc_id, "name": svc.get("component", {}).get("name"), "error": str(e)})
		
		return results
	
	def get_flow_health_status(self, pg_id: str) -> Dict[str, Any]:
		"""Get comprehensive health status of a process group and all its components.
		
		Returns detailed health information including:
		- Processor status (running/stopped/invalid)
		- Controller service status (enabled/disabled/invalid)
		- Connection queue status
		- Active bulletins/errors
		- Overall health assessment
		"""
		health = {
			"processGroupId": pg_id,
			"processors": {"total": 0, "running": 0, "stopped": 0, "invalid": 0, "disabled": 0},
			"controllerServices": {"total": 0, "enabled": 0, "disabled": 0, "invalid": 0},
			"connections": {"total": 0, "with_queued_data": 0, "backpressure": 0},
			"bulletins": [],
			"errors": [],
			"overallHealth": "HEALTHY"
		}
		
		# Get processors
		processors = self.list_processors(pg_id)
		for proc in processors.get("processors", []):
			health["processors"]["total"] += 1
			run_status = proc.get("status", {}).get("runStatus", "STOPPED")
			
			if run_status == "Running":
				health["processors"]["running"] += 1
			elif run_status == "Stopped":
				health["processors"]["stopped"] += 1
			elif run_status == "Invalid":
				health["processors"]["invalid"] += 1
				health["errors"].append(f"Processor '{proc.get('component', {}).get('name')}' is invalid")
			elif run_status == "Disabled":
				health["processors"]["disabled"] += 1
		
		# Get controller services
		try:
			services = self.get_controller_services(pg_id)
			for svc in services.get("controllerServices", []):
				health["controllerServices"]["total"] += 1
				state = svc.get("component", {}).get("state", "DISABLED")
				
				if state == "ENABLED":
					health["controllerServices"]["enabled"] += 1
				elif state == "DISABLED":
					health["controllerServices"]["disabled"] += 1
				elif "INVALID" in state or "ERROR" in state:
					health["controllerServices"]["invalid"] += 1
					health["errors"].append(f"Service '{svc.get('component', {}).get('name')}' is invalid")
		except Exception:
			pass  # Services might not be accessible at all levels
		
		# Get connections
		connections = self.list_connections(pg_id)
		for conn in connections.get("connections", []):
			health["connections"]["total"] += 1
			status = conn.get("status", {})
			queued_count = status.get("aggregateSnapshot", {}).get("flowFilesQueued", 0)
			
			if queued_count > 0:
				health["connections"]["with_queued_data"] += 1
			
			# Check backpressure
			if status.get("aggregateSnapshot", {}).get("percentUseCount", 0) > 80:
				health["connections"]["backpressure"] += 1
				health["errors"].append(f"Connection has backpressure (>80% full)")
		
		# Get recent bulletins
		try:
			bulletins_data = self.get_bulletins()
			recent_bulletins = bulletins_data.get("bulletinBoard", {}).get("bulletins", [])
			for bulletin in recent_bulletins[:10]:  # Last 10
				health["bulletins"].append({
					"level": bulletin.get("bulletin", {}).get("level"),
					"message": bulletin.get("bulletin", {}).get("message"),
					"timestamp": bulletin.get("bulletin", {}).get("timestamp")
				})
				if bulletin.get("bulletin", {}).get("level") in ["ERROR", "WARN"]:
					health["errors"].append(bulletin.get("bulletin", {}).get("message"))
		except Exception:
			pass
		
		# Determine overall health
		if health["processors"]["invalid"] > 0 or health["controllerServices"]["invalid"] > 0:
			health["overallHealth"] = "UNHEALTHY"
		elif health["connections"]["backpressure"] > 0 or len(health["errors"]) > 0:
			health["overallHealth"] = "DEGRADED"
		else:
			health["overallHealth"] = "HEALTHY"
		
		return health
	
	def terminate_processor(self, processor_id: str, version: int) -> Dict[str, Any]:
		"""Forcefully terminate all threads for a stuck processor. Requires NIFI_READONLY=false.
		
		⚠️ WARNING: This is a last-resort operation for processors that won't stop normally.
		Use only when a processor is stuck and not responding to normal stop commands.
		"""
		# First try to stop normally
		try:
			self.stop_processor(processor_id, version)
			return {"status": "stopped_normally", "message": "Processor stopped without needing termination"}
		except Exception:
			pass
		
		# If normal stop fails, terminate threads
		return self._delete(
			f"processors/{processor_id}/threads",
			params={"version": version}
		)


