from __future__ import annotations

from typing import Any, Dict, Optional, Tuple
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

	def get_controller_services(self, pg_id: Optional[str] = None) -> Dict[str, Any]:
		"""Get controller services. If pg_id is None, gets controller-level services."""
		if pg_id:
			return self._get(f"flow/process-groups/{pg_id}/controller-services")
		return self._get("flow/controller/controller-services")


