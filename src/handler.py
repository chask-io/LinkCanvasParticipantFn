import json
import logging
from typing import Any, Dict

from api.orchestrator_requests import orchestrator_api_manager
from backend import FunctionBackend
from chask_foundation.backend.models import OrchestrationEvent

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def parse_event(event: Dict[str, Any]) -> OrchestrationEvent:
    if isinstance(event, str):
        event = json.loads(event)
    if "body" in event:
        body = event["body"]
        event = json.loads(body) if isinstance(body, str) else body
    orchestration_event_data = event.get("orchestration_event")
    if not orchestration_event_data:
        raise ValueError("Missing 'orchestration_event' in Lambda event")
    return OrchestrationEvent.model_validate(orchestration_event_data)


def send_response_to_orchestrator(
    orchestration_event: OrchestrationEvent, message: str, is_error: bool = False
) -> bool:
    try:
        response_event = orchestration_event.model_copy(deep=True)
        original_extra_params = orchestration_event.extra_params or {}
        tool_call = (original_extra_params.get("tool_calls") or [{}])[0]

        response_event.event_type = "function_call_response"
        response_event.source = "agent"
        response_event.target = "orchestrator"
        response_event.prompt = message
        response_event.extra_params = dict(response_event.extra_params or {})

        if original_extra_params.get("is_test"):
            response_event.extra_params["is_test"] = True
            if original_extra_params.get("test_execution_uuid"):
                response_event.extra_params["test_execution_uuid"] = original_extra_params["test_execution_uuid"]
        if original_extra_params.get("is_node_test"):
            response_event.extra_params["is_node_test"] = True
            for key in ("node_test_execution_uuid", "pipeline_id", "node_id"):
                if original_extra_params.get(key):
                    response_event.extra_params[key] = original_extra_params[key]

        response_event.extra_params.update(
            {
                "tool_call_id": tool_call.get("id"),
                "tool_name": tool_call.get("name"),
                "is_error": is_error,
            }
        )

        response = orchestrator_api_manager.call(
            "forward_oe_to_kafka",
            orchestration_event=response_event.model_dump(),
            topic="orchestrator",
            access_token=response_event.access_token,
            organization_id=response_event.organization.organization_id,
        )
        return bool(response and response.get("status") == "success")
    except Exception as exc:
        logger.error("Failed to send response to orchestrator: %s", exc)
        return False


def notify_agent_available(orchestration_event: OrchestrationEvent) -> None:
    try:
        extra_params = orchestration_event.extra_params or {}
        if extra_params.get("is_test") or extra_params.get("is_node_test"):
            return

        evolve_response = orchestrator_api_manager.call(
            "evolve_event",
            parent_event_uuid=str(orchestration_event.event_id),
            event_type="agent_available",
            source="agent",
            target="agent_manager",
            prompt="",
            extra_params={},
            access_token=orchestration_event.access_token,
            organization_id=orchestration_event.organization.organization_id,
        )
        if evolve_response.get("status_code") not in (200, 201):
            raise RuntimeError(f"Failed to evolve event: {evolve_response}")
        evolved_uuid = evolve_response.get("uuid")
        if not evolved_uuid:
            raise RuntimeError("API response missing uuid for evolved event")

        agent_event = orchestration_event.model_copy(deep=True)
        agent_event.event_id = evolved_uuid
        agent_event.event_type = "agent_available"
        agent_event.source = "agent"
        agent_event.target = "agent_manager"
        agent_event.prompt = ""
        agent_event.extra_params = evolve_response.get("extra_params", {})

        orchestrator_api_manager.call(
            "forward_oe_to_kafka",
            orchestration_event=agent_event.model_dump(),
            topic="agent_manager",
            access_token=agent_event.access_token,
            organization_id=agent_event.organization.organization_id,
        )
    except Exception as exc:
        logger.error("Failed to notify agent available (non-fatal): %s", exc)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    orchestration_event = None
    request_id = context.aws_request_id if context else "unknown"
    try:
        logger.info("[%s] Lambda invoked", request_id)
        orchestration_event = parse_event(event)
        result = FunctionBackend(orchestration_event).process_request()
        response_sent = send_response_to_orchestrator(orchestration_event, result, is_error=False)
        return success_response({"message": result, "request_id": request_id}, response_sent)
    except ValueError as exc:
        logger.error("Validation error: %s", exc, exc_info=True)
        response_sent = (
            send_response_to_orchestrator(orchestration_event, f"Validation error: {exc}", True)
            if orchestration_event
            else False
        )
        return error_response(f"Validation error: {exc}", response_sent, 400)
    except Exception as exc:
        logger.error("Lambda error: %s", exc, exc_info=True)
        response_sent = (
            send_response_to_orchestrator(orchestration_event, f"Lambda error: {exc}", True)
            if orchestration_event
            else False
        )
        return error_response(f"Lambda error: {exc}", response_sent, 500)
    finally:
        if orchestration_event:
            notify_agent_available(orchestration_event)


def success_response(result: Dict[str, Any], response_event_sent: bool = False) -> Dict[str, Any]:
    return {
        "statusCode": 200,
        "body": {
            "status": "ok",
            "result": result,
            "response_event_sent": response_event_sent,
        },
    }


def error_response(
    error_message: str, response_event_sent: bool = False, status_code: int = 500
) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "body": {
            "status": "error",
            "error": error_message,
            "response_event_sent": response_event_sent,
        },
    }
