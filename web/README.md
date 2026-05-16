# PCWSpace Web Dashboard

React + TypeScript + Vite web interface for PCWSpace.

## Setup

```bash
npm install
npm run dev
```

Opens on `http://localhost:3000` with proxy to the API server on port 8000.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `VITE_PCW_URL` | Override API base URL (empty = same origin via proxy) | (empty) |
| `VITE_PCW_API_KEY` | API key for authentication | `dev-insecure` |

## Pages

- **Dashboard** — Create workspaces and sessions
- **Session** — Chat with AI agents, manage artifacts, fork/close sessions
- **Artifacts** — View artifact content and version history
- **Workflows** — Start and monitor multi-step AI workflows
- **Timeline** — DeltaShot history, replay state, fork sessions
