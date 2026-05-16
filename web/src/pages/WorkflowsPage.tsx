import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api } from '../lib/api';
import { Workflow, Play, Eye, RefreshCw } from 'lucide-react';

export default function WorkflowsPage() {
  const [sessionId, setSessionId] = useState('');
  const [selectedDef, setSelectedDef] = useState('');
  const [inputJson, setInputJson] = useState('{}');
  const [workflowId, setWorkflowId] = useState('');
  const [workflow, setWorkflow] = useState<any>(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const { data: definitions } = useQuery({
    queryKey: ['workflow-definitions'],
    queryFn: api.listDefinitions,
  });

  const startWorkflow = async () => {
    if (!sessionId || !selectedDef) return;
    setLoading(true);
    setError('');
    try {
      const input = JSON.parse(inputJson);
      const result = await api.startWorkflow(selectedDef, sessionId, input);
      setWorkflowId(result.workflow_id);
      setWorkflow(result);
    } catch (e: any) {
      setError(e.message);
    }
    setLoading(false);
  };

  const refreshWorkflow = async () => {
    if (!workflowId) return;
    setLoading(true);
    try {
      const result = await api.getWorkflow(workflowId);
      setWorkflow(result);
    } catch (e: any) {
      setError(e.message);
    }
    setLoading(false);
  };

  const loadWorkflow = async () => {
    if (!workflowId.trim()) return;
    setLoading(true);
    setError('');
    try {
      const result = await api.getWorkflow(workflowId);
      setWorkflow(result);
    } catch (e: any) {
      setError(e.message);
    }
    setLoading(false);
  };

  return (
    <div>
      <h1 style={{ fontSize: '1.75rem', fontWeight: 700, marginBottom: 8 }}>Workflows</h1>
      <p style={{ color: 'var(--text-muted)', marginBottom: '1.5rem' }}>
        Orchestrate multi-step AI workflows with automatic retry and dead-letter handling.
      </p>

      {error && <div style={errorStyle}>{error}</div>}

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1.5rem' }}>
        {/* Start Workflow */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}><Play size={16} /> Start Workflow</h3>

          <label style={labelStyle}>Session ID</label>
          <input style={inputStyle} placeholder="Session ID..." value={sessionId} onChange={e => setSessionId(e.target.value)} />

          <label style={labelStyle}>Template</label>
          <select style={inputStyle} value={selectedDef} onChange={e => setSelectedDef(e.target.value)}>
            <option value="">Select a template...</option>
            {Array.isArray(definitions) && definitions.map((d: any) => (
              <option key={d.name} value={d.name}>{d.name} — {d.description}</option>
            ))}
          </select>

          <label style={labelStyle}>Input (JSON)</label>
          <textarea style={{ ...inputStyle, minHeight: 80, fontFamily: 'monospace', fontSize: '0.8rem' }} value={inputJson} onChange={e => setInputJson(e.target.value)} />

          <button style={{ ...btnStyle, marginTop: 12 }} onClick={startWorkflow} disabled={loading || !sessionId || !selectedDef}>
            <Play size={14} /> Start
          </button>
        </div>

        {/* View Workflow */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}><Eye size={16} /> Workflow Status</h3>

          <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
            <input style={{ ...inputStyle, flex: 1 }} placeholder="Workflow ID..." value={workflowId} onChange={e => setWorkflowId(e.target.value)} onKeyDown={e => e.key === 'Enter' && loadWorkflow()} />
            <button style={btnStyle} onClick={loadWorkflow}><Eye size={14} /></button>
            {workflow && <button style={btnStyle} onClick={refreshWorkflow}><RefreshCw size={14} /></button>}
          </div>

          {workflow && (
            <div style={statusCardStyle}>
              <div style={statusRowStyle}>
                <span style={statusLabelStyle}>Status</span>
                <span style={{ color: statusColor(workflow.status), fontWeight: 600 }}>{workflow.status}</span>
              </div>
              <div style={statusRowStyle}>
                <span style={statusLabelStyle}>Current Step</span>
                <span style={{ color: 'var(--cyan)' }}>{workflow.current_step}</span>
              </div>
              <div style={statusRowStyle}>
                <span style={statusLabelStyle}>Step Status</span>
                <span>{workflow.step_status}</span>
              </div>
              <div style={statusRowStyle}>
                <span style={statusLabelStyle}>Retries</span>
                <span>{workflow.retry_count}</span>
              </div>
              {workflow.error && (
                <div style={{ marginTop: 8, padding: '0.5rem', borderRadius: 6, background: '#3b111133', border: '1px solid var(--error)', fontSize: '0.75rem', color: 'var(--error)' }}>
                  {workflow.error}
                </div>
              )}
              {workflow.completed_at && (
                <div style={statusRowStyle}>
                  <span style={statusLabelStyle}>Completed</span>
                  <span>{workflow.completed_at.slice(0, 19)}</span>
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Definitions */}
      <div style={{ ...cardStyle, marginTop: '1.5rem' }}>
        <h3 style={cardTitleStyle}><Workflow size={16} /> Available Templates</h3>
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12 }}>
          {Array.isArray(definitions) && definitions.map((d: any) => (
            <div key={d.name} style={{ padding: '0.75rem', borderRadius: 8, background: 'var(--bg)', border: '1px solid var(--border)' }}>
              <div style={{ fontWeight: 600, fontSize: '0.9rem', marginBottom: 4 }}>{d.name}</div>
              <div style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>{d.description}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function statusColor(status: string): string {
  switch (status) {
    case 'running': return 'var(--success)';
    case 'completed': return 'var(--cyan)';
    case 'failed': return 'var(--error)';
    default: return 'var(--text-muted)';
  }
}

const cardStyle: React.CSSProperties = { background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12, padding: '1.5rem' };
const cardTitleStyle: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 8, fontSize: '1rem', fontWeight: 600, marginBottom: '1rem' };
const labelStyle: React.CSSProperties = { display: 'block', fontSize: '0.75rem', color: 'var(--text-muted)', marginBottom: 4, marginTop: 12 };
const inputStyle: React.CSSProperties = { width: '100%', padding: '0.5rem 0.75rem', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--bg)', color: 'var(--text)', outline: 'none' };
const btnStyle: React.CSSProperties = { padding: '0.5rem 0.75rem', borderRadius: 6, background: 'var(--primary)', color: '#fff', fontWeight: 500, fontSize: '0.8rem', display: 'flex', alignItems: 'center', gap: 4 };
const errorStyle: React.CSSProperties = { background: '#3b111155', border: '1px solid var(--error)', borderRadius: 8, padding: '0.75rem 1rem', marginBottom: '1rem', color: 'var(--error)', fontSize: '0.875rem' };
const statusCardStyle: React.CSSProperties = { background: 'var(--bg)', borderRadius: 8, padding: '1rem', border: '1px solid var(--border)' };
const statusRowStyle: React.CSSProperties = { display: 'flex', justifyContent: 'space-between', padding: '0.35rem 0', fontSize: '0.85rem' };
const statusLabelStyle: React.CSSProperties = { color: 'var(--text-muted)' };
