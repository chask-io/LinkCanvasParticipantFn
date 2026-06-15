from types import SimpleNamespace

from backend.function_logic import FunctionBackend


def test_sender_customer_uuid_is_forwarded_to_participant_link(monkeypatch):
    captured = {}

    class FakeManager:
        def call(self, name, **kwargs):
            captured["name"] = name
            captured["kwargs"] = kwargs
            return {"uuid": "participant-link-1"}

    monkeypatch.setattr("backend.function_logic._get_canvas_api_manager", lambda: FakeManager())
    oe = SimpleNamespace(
        orchestration_session_uuid="session-1",
        access_token="token",
        organization=SimpleNamespace(organization_id="org-1"),
        extra_params={
            "sender_organization_customer_uuid": "customer-boss",
            "design_context": {
                "canvas_uuid": "canvas-1",
                "project_uuid": "project-1",
            },
            "tool_calls": [
                {
                    "args": {
                        "kind": "person",
                        "role": "Approver",
                        "lane": "lane_management",
                        "summary": "Approves high-discount requests.",
                        "contributed_node_ids": ["node-approval"],
                    }
                }
            ],
        },
    )

    result = FunctionBackend(oe).process_request()

    assert result == "Linked the confirmed participant to the canvas."
    payload = captured["kwargs"]
    assert payload["organization_customer_uuid"] == "customer-boss"
    assert payload["canvas_uuid"] == "canvas-1"
    assert payload["project_uuid"] == "project-1"
    assert payload["orchestration_session_uuid"] == "session-1"
