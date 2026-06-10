import logging
import os
from typing import Any, Dict, List, Optional

from chask_foundation.api.api_manager import ApiManager
from chask_foundation.backend.models import OrchestrationEvent

logger = logging.getLogger()
logger.setLevel(logging.INFO)


ALLOWED_KINDS = {"person", "system"}

_canvas_api_manager: Optional[ApiManager] = None


def _get_canvas_api_manager() -> ApiManager:
    global _canvas_api_manager
    if _canvas_api_manager is not None:
        return _canvas_api_manager
    base_domain = os.getenv("BASE_DOMAIN")
    if not base_domain:
        raise ValueError("BASE_DOMAIN is required for chask_api calls")
    if not (base_domain.startswith("http://") or base_domain.startswith("https://")):
        base_domain = f"https://{base_domain}"
    base_url = f"{base_domain.rstrip('/')}/api/v2/canvas"
    manager = ApiManager(base_url=base_url)

    @manager.register("link_canvas_participant", "link-canvas-participant", "POST")
    def _link_canvas_participant(**payload: Any) -> Dict[str, Any]:
        return {"json": payload}

    _canvas_api_manager = manager
    return _canvas_api_manager


class FunctionBackend:
    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event
        logger.info(
            "Initialized LinkCanvasParticipantFn for org: %s",
            orchestration_event.organization.organization_id,
        )

    def process_request(self) -> str:
        orchestration_session_uuid = self.orchestration_event.orchestration_session_uuid
        if not orchestration_session_uuid:
            return "No orchestration session was found in this event, so the participant was not linked."

        tool_args = self._extract_tool_args()
        kind = self._normalize_kind(tool_args.get("kind"))
        if not kind:
            return "No valid participant kind was provided, so nothing was linked."

        system_name = self._normalize_optional_text(tool_args.get("system_name"))
        if kind == "system" and not system_name:
            return "No system_name was provided for the system participant, so nothing was linked."

        payload: Dict[str, Any] = {
            "kind": kind,
            "orchestration_session_uuid": str(orchestration_session_uuid),
        }
        self._set_if_present(payload, "system_name", system_name)
        self._set_if_present(payload, "role", self._normalize_optional_text(tool_args.get("role")))
        self._set_if_present(payload, "lane", self._normalize_optional_text(tool_args.get("lane")))
        self._set_if_present(payload, "summary", self._normalize_optional_text(tool_args.get("summary")))

        contributed_node_ids = self._normalize_string_list(tool_args.get("contributed_node_ids"))
        if contributed_node_ids:
            payload["contributed_node_ids"] = contributed_node_ids

        logger.info(
            "Linking canvas participant kind=%s system=%s session=%s nodes=%s",
            kind,
            system_name,
            orchestration_session_uuid,
            contributed_node_ids,
        )
        try:
            _get_canvas_api_manager().call(
                "link_canvas_participant",
                access_token=self.orchestration_event.access_token,
                organization_id=str(self.orchestration_event.organization.organization_id),
                timeout=30,
                **payload,
            )
        except Exception as exc:
            if "Orchestration session is not linked to a user" in str(exc):
                logger.warning("Participant endpoint could not resolve session user: %s", exc)
                return "The canvas participant was not linked because this orchestration session is not linked to a user."
            raise

        if kind == "system":
            return "Linked the system to the canvas."
        return "Linked the confirmed participant to the canvas."

    def _extract_tool_args(self) -> Dict[str, Any]:
        extra_params = self.orchestration_event.extra_params or {}
        tool_calls = extra_params.get("tool_calls") or []
        if not tool_calls:
            logger.warning("No tool calls found in orchestration event")
            return {}
        return tool_calls[0].get("args") or {}

    def _normalize_kind(self, value: Any) -> Optional[str]:
        kind = self._normalize_optional_text(value)
        if kind not in ALLOWED_KINDS:
            logger.warning("Unsupported participant kind %r", kind)
            return None
        return kind

    def _normalize_string_list(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list):
            logger.warning("Expected contributed_node_ids list, got %s", type(value).__name__)
            return []
        result = []
        for item in value:
            text = self._normalize_optional_text(item)
            if text:
                result.append(text)
        return result

    def _normalize_optional_text(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _set_if_present(self, payload: Dict[str, Any], key: str, value: Any) -> None:
        if value is not None:
            payload[key] = value
