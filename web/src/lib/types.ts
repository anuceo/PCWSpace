export interface Workspace {
  workspace_id: string;
  name: string;
  active_session_id: string | null;
  created_at: string;
  metadata: Record<string, unknown>;
}

export interface Session {
  session_id: string;
  workspace_id: string;
  status: 'active' | 'closed';
  messages: unknown[];
  state_pointer: string | null;
  workflow_id: string | null;
  created_at: string;
  closed_at: string | null;
  metadata: Record<string, unknown>;
}

export interface Artifact {
  artifact_id: string;
  name: string;
  artifact_type: 'doc' | 'code' | 'design' | 'dataset';
  content: string;
  content_hash: string;
  version: number;
  parent_version_id: string | null;
  linked_session: string | null;
  agent_type: string | null;
  deltashot_id: string | null;
  created_at: string;
  metadata: Record<string, unknown>;
}

export interface AgentResult {
  response: string;
  agent_type: string;
  input_tokens: number;
  output_tokens: number;
  shot_id: string;
}

export interface WorkflowDef {
  name: string;
  description: string;
}

export interface WorkflowState {
  workflow_id: string;
  workflow_def_id: string;
  session_id: string;
  status: 'running' | 'completed' | 'failed';
  current_step: string;
  step_status: string;
  retry_count: number;
  step_results: Record<string, unknown>;
  context: Record<string, unknown>;
  error: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
}

export interface DeltaShotCount {
  session_id: string;
  count: number;
}

export interface HealthResponse {
  status: 'ok' | 'degraded';
  redis: boolean;
  version: string;
}
