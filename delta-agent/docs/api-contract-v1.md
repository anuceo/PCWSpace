# Delta Agent API Contract v1

Base path: `/api/v1`

This contract is deterministic, session-scoped, and replay-aware.

## Design guarantees

1. All core mutations are session-scoped.
2. Every stateful mutation returns a DeltaShot reference.
3. Async workflow execution is first-class.
4. State transitions are explicit and versioned.
5. Artifacts are versioned, never overwritten.

## Global response shape

Successful responses include payload fields at top level plus:

```json
{
  "...payload fields": "...",
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

Errors:

```json
{
  "error": {
    "code": "SESSION_LOCKED",
    "message": "session lock unavailable",
    "retryable": true
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 1
  }
}
```

## Endpoints

### Workspaces

- `POST /workspaces`
- `GET /workspaces/{workspaceId}`
- `GET /workspaces/{workspaceId}/sessions?limit=20&cursor=<timestamp>`

### Sessions

- `POST /sessions`
- `POST /sessions/{sessionId}/messages`
- `GET /sessions/{sessionId}/state`
- `GET /sessions/{sessionId}/messages?limit=50&cursor=<message_id>`

### DeltaShots

- `GET /sessions/{sessionId}/deltashots`
- `GET /deltashots/{deltashotId}`
- `POST /sessions/{sessionId}/rollback`

### Artifacts

- `POST /artifacts`
- `GET /artifacts/{artifactId}`
- `GET /artifacts/{artifactId}/versions`
- `GET /artifacts/{artifactId}/versions/{version}`

### Workflows

- `POST /workflows/start`
- `POST /workflows/execute-next`
- `GET /workflows/{workflowId}/state`
- `POST /workflows/{workflowId}/step`

### Agent Control

- `POST /sessions/{sessionId}/agent`
- `GET /sessions/{sessionId}/agents/logs`

### System / Debug

- `GET /sessions/{sessionId}/trace`
- `GET /health`

## Deterministic primary mutation response

`POST /sessions/{sessionId}/messages`

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
