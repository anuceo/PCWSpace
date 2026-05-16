import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { api } from '../lib/api';
import { Plus, ArrowRight, MessageSquare } from 'lucide-react';

export default function Dashboard() {
  const [workspaceName, setWorkspaceName] = useState('');
  const [workspaceId, setWorkspaceId] = useState('');
  const [sessionId, setSessionId] = useState('');
  const [sessions, setSessions] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const createWorkspace = async () => {
    if (!workspaceName.trim()) return;
    setLoading(true);
    setError('');
    try {
      const ws = await api.createWorkspace(workspaceName);
      setWorkspaceId(ws.workspace_id);
      setWorkspaceName('');
    } catch (e: any) {
      setError(e.message);
    }
    setLoading(false);
  };

  const createSession = async () => {
    if (!workspaceId) return;
    setLoading(true);
    setError('');
    try {
      const s = await api.createSession(workspaceId);
      setSessions(prev => [s, ...prev]);
      setSessionId(s.session_id);
    } catch (e: any) {
      setError(e.message);
    }
    setLoading(false);
  };

  const loadSession = async () => {
    if (!sessionId.trim()) return;
    navigate(`/session/${sessionId}`);
  };

  return (
    <div>
      <h1 style={h1Style}>Dashboard</h1>
      <p style={{ color: 'var(--text-muted)', marginBottom: '2rem' }}>
        Create workspaces and sessions to get started.
      </p>

      {error && <div style={errorStyle}>{error}</div>}

      <div style={gridStyle}>
        {/* Create Workspace */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}>
            <Plus size={16} /> New Workspace
          </h3>
          <div style={inputGroupStyle}>
            <input
              style={inputStyle}
              placeholder="Workspace name..."
              value={workspaceName}
              onChange={e => setWorkspaceName(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && createWorkspace()}
            />
            <button style={btnStyle} onClick={createWorkspace} disabled={loading}>
              Create
            </button>
          </div>
          {workspaceId && (
            <div style={resultStyle}>
              <span style={{ color: 'var(--success)' }}>✓</span> Workspace: <code>{workspaceId.slice(0, 8)}...</code>
            </div>
          )}
        </div>

        {/* Create Session */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}>
            <MessageSquare size={16} /> New Session
          </h3>
          {workspaceId ? (
            <>
              <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: 12 }}>
                In workspace: <code>{workspaceId.slice(0, 8)}...</code>
              </p>
              <button style={btnStyle} onClick={createSession} disabled={loading}>
                Start Session
              </button>
            </>
          ) : (
            <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>
              Create a workspace first
            </p>
          )}
          {sessions.length > 0 && (
            <div style={{ marginTop: 12 }}>
              {sessions.map(s => (
                <div
                  key={s.session_id}
                  style={sessionItemStyle}
                  onClick={() => navigate(`/session/${s.session_id}`)}
                >
                  <MessageSquare size={14} />
                  <span>{s.session_id.slice(0, 8)}...</span>
                  <ArrowRight size={14} style={{ marginLeft: 'auto' }} />
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Open Existing Session */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}>
            <ArrowRight size={16} /> Open Session
          </h3>
          <div style={inputGroupStyle}>
            <input
              style={inputStyle}
              placeholder="Session ID..."
              value={sessionId}
              onChange={e => setSessionId(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && loadSession()}
            />
            <button style={btnStyle} onClick={loadSession}>
              Open
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

const h1Style: React.CSSProperties = { fontSize: '1.75rem', fontWeight: 700, marginBottom: 8 };
const gridStyle: React.CSSProperties = { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(320px, 1fr))', gap: '1.5rem' };
const cardStyle: React.CSSProperties = { background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12, padding: '1.5rem' };
const cardTitleStyle: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 8, fontSize: '1rem', fontWeight: 600, marginBottom: '1rem' };
const inputGroupStyle: React.CSSProperties = { display: 'flex', gap: 8 };
const inputStyle: React.CSSProperties = { flex: 1, padding: '0.5rem 0.75rem', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--bg)', color: 'var(--text)', outline: 'none' };
const btnStyle: React.CSSProperties = { padding: '0.5rem 1rem', borderRadius: 6, background: 'var(--primary)', color: '#fff', fontWeight: 500, fontSize: '0.875rem' };
const resultStyle: React.CSSProperties = { marginTop: 12, fontSize: '0.85rem', display: 'flex', alignItems: 'center', gap: 6 };
const errorStyle: React.CSSProperties = { background: '#3b111155', border: '1px solid var(--error)', borderRadius: 8, padding: '0.75rem 1rem', marginBottom: '1.5rem', color: 'var(--error)', fontSize: '0.875rem' };
const sessionItemStyle: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 8, padding: '0.5rem 0.75rem', borderRadius: 6, cursor: 'pointer', fontSize: '0.85rem', color: 'var(--text-muted)', background: 'var(--bg)', marginBottom: 4 };
