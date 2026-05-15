# Delta Agent API Contract v1

Base path: `/api/v1`

This API is a command interface over a temporal execution engine. It is deterministic, session-scoped, replayable through DeltaShots, and version-safe for state/artifacts.

## API design principles

1. Everything is session-scoped.
2. Every state mutation returns a DeltaShot reference.
3. Async operations (workflow queue + streaming) are first-class.
4. State is versioned and never blindly overwritten.
5. Artifacts are versioned, not replaced.

## Authentication and authorization

When auth middleware is enabled, clients must provide API credentials using one of:

- `Authorization: Bearer <api_key>`
- `x-api-key: <api_key>`

Role permissions:

- **reader**: read-only API routes (`GET`/`HEAD`)
- **writer**: reader + mutation routes (`POST`)
- **admin**: writer + `/debug/*` endpoints

Auth middleware operational environment:

- `DELTA_AGENT_AUTH_REQUIRED` (default `false`)
- `DELTA_AGENT_AUTH_DISABLED` (default `false`)
- `DELTA_AGENT_API_KEY` / `DELTA_AGENT_API_KEYS` (writer keys)
- `DELTA_AGENT_READONLY_API_KEY` / `DELTA_AGENT_READONLY_API_KEYS` (reader keys)
- `DELTA_AGENT_ADMIN_API_KEY` / `DELTA_AGENT_ADMIN_API_KEYS` (admin keys)
- `PCW_API_KEY` (compatibility writer key)

## Global response contract

### Success envelope

Object responses:

```json
{
  "...payload_fields": "...",
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

Collection responses emitted by the runtime are wrapped as:

```json
{
  "value": [],
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

### Error envelope

```json
{
  "error": {
    "code": "SESSION_LOCKED",
    "message": "Session is currently locked",
    "retryable": true
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 1
  }
}
```

---

## 1) Workspace API

- `POST /workspaces`
- `GET /workspaces/{workspaceId}`
- `GET /workspaces/{workspaceId}/sessions?limit=20&cursor=<timestamp>`

Create workspace request:

```json
{
  "name": "OneManBusinessOS"
}
```

Create workspace response:

```json
{
  "workspace_id": "ws_123",
  "created_at": 1775453669005,
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

---

## 2) Session API (core interaction)

- `POST /sessions`
- `POST /sessions/{sessionId}/messages` (primary entrypoint)
- `GET /sessions/{sessionId}/state`
- `GET /sessions/{sessionId}/messages?limit=50&cursor=<message_id>`

Create session request:

```json
{
  "workspace_id": "ws_123",
  "name": "Landing Page Build"
}
```

Primary deterministic mutation request:

```json
{
  "content": "Create a landing page for my product",
  "mode": "chat",
  "metadata": {
    "priority": "normal"
  }
}
```

Primary deterministic mutation response:

```json
{
  "message": {
    "id": "msg_789",
    "role": "agent",
    "content": "...generated response..."
  },
  "deltashot": {
    "id": "ds_001",
    "timestamp": 1775453669005
  },
  "state": {
    "version": 12,
    "summary": {
      "goal": "Landing page",
      "step": "draft_complete"
    }
  },
  "artifacts": [
    {
      "artifact_id": "art_22",
      "version": 3,
      "type": "code"
    }
  ],
  "workflow": {
    "active": true,
    "step": "refine"
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

---

## 3) DeltaShots API (time control)

- `GET /sessions/{sessionId}/deltashots`
- `GET /deltashots/{deltashotId}`
- `POST /sessions/{sessionId}/rollback`

DeltaShot detail response:

```json
{
  "id": "ds_001",
  "timestamp": 1775453669005,
  "type": "STATE_UPDATE",
  "diff": {},
  "hash": "abc...",
  "prev_hash": "def...",
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

Rollback request:

```json
{
  "target_deltashot_id": "ds_0007",
  "mode": "hard"
}
```

Rollback response:

```json
{
  "status": "rolled_back",
  "current_state_version": 8,
  "new_deltashot_id": "ds_rollback_01",
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

---

## 4) Artifact API (live outputs)

- `POST /artifacts`
- `GET /artifacts/{artifactId}`
- `GET /artifacts/{artifactId}/versions`
- `GET /artifacts/{artifactId}/versions/{version}`

Artifact write request:

```json
{
  "session_id": "sess_456",
  "type": "code",
  "content": "...",
  "metadata": {
    "label": "Landing Page HTML"
  }
}
```

---

## 5) Workflow API (automation layer)

- `POST /workflows/start`
- `POST /workflows/execute-next`
- `POST /workflows/notion/execute-next`
- `GET /workflows/{workflowId}/state`
- `POST /workflows/{workflowId}/step`

Workflow start request:

```json
{
  "session_id": "sess_456",
  "workflow_id": "client_outreach",
  "input": {
    "target": "SaaS founders"
  }
}
```

Background worker pull response (`POST /workflows/execute-next`):

```json
{
  "executed": true,
  "result": {
    "workflow_id": "client_outreach",
    "session_id": "sess_456",
    "step": "start",
    "deltashot_id": "ds_120",
    "state_version": 21
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

If no job is available:

```json
{
  "executed": false,
  "result": null,
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 1
  }
}
```

A Notion queue pull (`POST /workflows/notion/execute-next`) returns:

```json
{
  "executed": true,
  "result": {
    "session_id": "sess_456",
    "artifacts_count": 2,
    "summary_preview": "Here is the generated update..."
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 8
  }
}
```

Operational note: Notion outbound sync is integration-gated at runtime (`NOTION_SYNC_ENABLED=true` plus token/parent configuration). When disabled or incomplete, the job is still consumed and logged to keep queue flow deterministic.

---

## 6) Agent control API

- `POST /sessions/{sessionId}/agent`
- `GET /sessions/{sessionId}/agents/logs`

Force agent request:

```json
{
  "agent": "claude",
  "reason": "force reasoning model"
}
```

---

## 7) System / debug API

- `GET /sessions/{sessionId}/trace`
- `POST /debug/sessions/{sessionId}/branches/{branchId}/audit`
- `GET /health`

Execution trace response shape:

```json
{
  "messages": [],
  "deltashots": [],
  "state_transitions": [],
  "artifacts": [],
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

---

## 8) Branching API (temporal forking extension)

- `POST /sessions/{sessionId}/branch`
- `GET /sessions/{sessionId}/branches`
- `POST /sessions/{sessionId}/branch/switch`
- `POST /sessions/{sessionId}/branch/merge`

Create branch request:

```json
{
  "from_deltashot_id": "ds_010",
  "label": "Try aggressive marketing angle",
  "mode": "soft"
}
```

Create branch response:

```json
{
  "branch": {
    "branch_id": "br_789",
    "parent_deltashot_id": "ds_010",
    "created_at": 1775453669005
  },
  "state": {
    "version": 10,
    "forked": true
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

### Branching invariant

A branch never mutates another branch's event stream.

---

## 9) Streaming API (real-time execution extension)

- `POST /sessions/{sessionId}/messages/stream`
- Header: `Idempotency-Key: req_123` (optional, supported)
- Response content type: `text/event-stream`

Request:

```json
{
  "content": "Build me a landing page",
  "mode": "execution",
  "stream": true,
  "branch": "br_789"
}
```

SSE event contract:

1. `ack` -> `{"request_id":"req_123"}`
2. `token` -> `{"delta":"Here","accumulated":"Here"}`
3. `message` -> `{"message_id":"msg_1","content":"..."}`
4. `deltashot` -> `{"id":"ds_22","type":"STATE_UPDATE"}`
5. `artifact` -> `{"artifact_id":"art_1","version":2,"type":"code"}`
6. `workflow` -> `{"step":"refine","status":"started"}`
7. `done` -> `{"status":"complete"}`
8. `error` -> `{"code":"AGENT_FAILURE","retryable":true}`

Streaming design requirements:

- token stream is buffered; state commits only at completion
- supports idempotent replay via `Idempotency-Key`
- branch-aware execution via `branch` field

---

## Optional future extensions

- `POST /sessions/{sessionId}/batch` (deterministic multi-command transaction)
