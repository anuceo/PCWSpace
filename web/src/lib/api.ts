const BASE_URL = import.meta.env.VITE_PCW_URL || '';
const API_KEY = import.meta.env.VITE_PCW_API_KEY || 'dev-insecure';

async function request<T = unknown>(path: string, options?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': API_KEY,
      ...options?.headers,
    },
  });

  const contentType = res.headers.get('content-type') || '';
  if (!contentType.includes('application/json')) {
    const text = await res.text();
    throw new Error(text || `HTTP ${res.status}`);
  }

  const json = await res.json();
  if (!json.ok && json.error) {
    throw new Error(json.error);
  }
  return json.data ?? json;
}

export const api = {
  health: () => fetch(`${BASE_URL}/health`).then(r => r.json()),

  // Workspaces
  createWorkspace: (name: string) =>
    request<any>('/api/v1/workspaces', { method: 'POST', body: JSON.stringify({ name }) }),

  // Sessions
  createSession: (workspace_id: string) =>
    request<any>('/api/v1/sessions', { method: 'POST', body: JSON.stringify({ workspace_id }) }),
  getSession: (id: string) => request<any>(`/api/v1/sessions/${id}`),
  closeSession: (id: string) =>
    request<any>(`/api/v1/sessions/${id}/close`, { method: 'POST' }),
  getSessionArtifacts: (id: string) => request<any[]>(`/api/v1/sessions/${id}/artifacts`),

  // Agent
  callAgent: (session_id: string, message: string, agent?: string, system_prompt?: string) =>
    request<any>(`/api/v1/sessions/${session_id}/agent`, {
      method: 'POST',
      body: JSON.stringify({ message, agent, system_prompt }),
    }),

  // Artifacts
  createArtifact: (session_id: string, name: string, artifact_type: string, content: string) =>
    request<any>('/api/v1/artifacts', {
      method: 'POST',
      body: JSON.stringify({ session_id, name, artifact_type, content }),
    }),
  getArtifact: (id: string) => request<any>(`/api/v1/artifacts/${id}`),
  createVersion: (id: string, content: string) =>
    request<any>(`/api/v1/artifacts/${id}/versions`, {
      method: 'POST',
      body: JSON.stringify({ content }),
    }),
  listVersions: (id: string) => request<string[]>(`/api/v1/artifacts/${id}/versions`),

  // Workflows
  listDefinitions: () => request<any[]>('/api/v1/workflow-definitions'),
  startWorkflow: (definition_name: string, session_id: string, input: object) =>
    request<any>('/api/v1/workflows', {
      method: 'POST',
      body: JSON.stringify({ definition_name, session_id, input }),
    }),
  getWorkflow: (id: string) => request<any>(`/api/v1/workflows/${id}`),

  // Timeline
  getDeltashotCount: (session_id: string) =>
    request<any>(`/api/v1/sessions/${session_id}/deltashots/count`),
  getRollbackPoints: (session_id: string) =>
    request<any[]>(`/api/v1/sessions/${session_id}/rollback-points`),
  replay: (session_id: string, sequence: number) =>
    request<any>(`/api/v1/sessions/${session_id}/replay`, {
      method: 'POST',
      body: JSON.stringify({ sequence }),
    }),
  forkSession: (session_id: string, fork_at_sequence: number) =>
    request<any>(`/api/v1/sessions/${session_id}/fork`, {
      method: 'POST',
      body: JSON.stringify({ fork_at_sequence }),
    }),
};
