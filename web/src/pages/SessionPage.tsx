import { useState, useRef, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api } from '../lib/api';
import { Send, FileText, GitBranch, X, Plus, Bot, User } from 'lucide-react';

interface Message {
  role: 'user' | 'assistant' | 'system';
  content: string;
  agent?: string;
  tokens?: { input: number; output: number };
  shotId?: string;
}

export default function SessionPage() {
  const { id } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [agent, setAgent] = useState<string>('auto');
  const [loading, setLoading] = useState(false);
  const [showArtifactModal, setShowArtifactModal] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const { data: session } = useQuery({
    queryKey: ['session', id],
    queryFn: () => api.getSession(id!),
    enabled: !!id,
  });

  const { data: artifacts, refetch: refetchArtifacts } = useQuery({
    queryKey: ['session-artifacts', id],
    queryFn: () => api.getSessionArtifacts(id!),
    enabled: !!id,
  });

  const { data: deltaCount } = useQuery({
    queryKey: ['deltashot-count', id],
    queryFn: () => api.getDeltashotCount(id!),
    enabled: !!id,
    refetchInterval: 5000,
  });

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const sendMessage = async () => {
    if (!input.trim() || !id || loading) return;
    const userMsg = input;
    setInput('');
    setMessages(prev => [...prev, { role: 'user', content: userMsg }]);
    setLoading(true);

    try {
      const agentOverride = agent === 'auto' ? undefined : agent;
      const result = await api.callAgent(id, userMsg, agentOverride);
      setMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: result.response,
          agent: result.agent_type,
          tokens: { input: result.input_tokens, output: result.output_tokens },
          shotId: result.shot_id,
        },
      ]);
      queryClient.invalidateQueries({ queryKey: ['deltashot-count', id] });
    } catch (e: any) {
      setMessages(prev => [...prev, { role: 'system', content: `Error: ${e.message}` }]);
    }
    setLoading(false);
  };

  const closeSession = async () => {
    if (!id) return;
    try {
      await api.closeSession(id);
      queryClient.invalidateQueries({ queryKey: ['session', id] });
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setMessages(prev => [...prev, { role: 'system', content: `Close error: ${msg}` }]);
    }
  };

  const forkSession = async () => {
    if (!id) return;
    try {
      const count = deltaCount?.count ?? 0;
      const result = await api.forkSession(id, count);
      navigate(`/session/${result.session_id}`);
    } catch (e: any) {
      setMessages(prev => [...prev, { role: 'system', content: `Fork error: ${e.message}` }]);
    }
  };

  return (
    <div style={{ display: 'flex', height: 'calc(100vh - 4rem)', gap: '1rem' }}>
      {/* Main chat area */}
      <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
        {/* Header */}
        <div style={headerStyle}>
          <div>
            <h2 style={{ fontSize: '1.1rem', fontWeight: 600 }}>
              Session <code style={{ color: 'var(--cyan)' }}>{id?.slice(0, 8)}...</code>
            </h2>
            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', display: 'flex', gap: 16 }}>
              <span>Status: <b style={{ color: session?.status === 'active' ? 'var(--success)' : 'var(--warning)' }}>{session?.status || '...'}</b></span>
              <span>DeltaShots: {deltaCount?.count ?? 0}</span>
              {session?.metadata?.forked_from && <span>⑂ Forked</span>}
            </div>
          </div>
          <div style={{ display: 'flex', gap: 8 }}>
            <button style={smallBtnStyle} onClick={forkSession} title="Fork session">
              <GitBranch size={14} /> Fork
            </button>
            <button style={smallBtnStyle} onClick={() => navigate(`/timeline/${id}`)}>
              Timeline
            </button>
            {session?.status === 'active' && (
              <button style={{ ...smallBtnStyle, background: 'var(--error)' }} onClick={closeSession}>
                <X size={14} /> Close
              </button>
            )}
          </div>
        </div>

        {/* Messages */}
        <div style={messagesStyle}>
          {messages.length === 0 && (
            <div style={{ textAlign: 'center', color: 'var(--text-muted)', padding: '3rem' }}>
              <Bot size={48} style={{ opacity: 0.3, marginBottom: 12 }} />
              <p>Send a message to start working with the AI agent.</p>
              <p style={{ fontSize: '0.8rem', marginTop: 8 }}>
                Tasks are automatically routed: general → Claude, code → DeepSeek
              </p>
            </div>
          )}
          {messages.map((msg, i) => (
            <div key={i} style={msgStyle(msg.role)}>
              <div style={msgIconStyle}>
                {msg.role === 'user' ? <User size={16} /> : msg.role === 'assistant' ? <Bot size={16} /> : null}
              </div>
              <div style={{ flex: 1 }}>
                <pre style={msgContentStyle}>{msg.content}</pre>
                {msg.tokens && (
                  <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: 4 }}>
                    {msg.agent} · {msg.tokens.input} in / {msg.tokens.output} out · shot: {msg.shotId?.slice(0, 8)}
                  </div>
                )}
              </div>
            </div>
          ))}
          {loading && (
            <div style={{ ...msgStyle('assistant'), opacity: 0.6 }}>
              <div style={msgIconStyle}><Bot size={16} /></div>
              <div style={{ color: 'var(--text-muted)' }}>Thinking...</div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>

        {/* Input */}
        <div style={inputAreaStyle}>
          <select
            style={selectStyle}
            value={agent}
            onChange={e => setAgent(e.target.value)}
          >
            <option value="auto">Auto-route</option>
            <option value="claude">Claude</option>
            <option value="deepseek">DeepSeek</option>
          </select>
          <input
            style={chatInputStyle}
            placeholder={session?.status === 'active' ? 'Type your message...' : 'Session is closed'}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && !e.shiftKey && sendMessage()}
            disabled={session?.status !== 'active' || loading}
          />
          <button style={sendBtnStyle} onClick={sendMessage} disabled={session?.status !== 'active' || loading}>
            <Send size={18} />
          </button>
        </div>
      </div>

      {/* Sidebar - Artifacts */}
      <div style={sidebarStyle}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem' }}>
          <h3 style={{ fontSize: '0.9rem', fontWeight: 600 }}>Artifacts</h3>
          <button style={smallBtnStyle} onClick={() => setShowArtifactModal(true)}>
            <Plus size={14} />
          </button>
        </div>

        {Array.isArray(artifacts) && artifacts.length > 0 ? (
          artifacts.map((a: any) => (
            <div key={a.artifact_id} style={artifactItemStyle}>
              <FileText size={14} style={{ flexShrink: 0, color: a.artifact_type === 'code' ? 'var(--cyan)' : 'var(--primary)' }} />
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ fontSize: '0.8rem', fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {a.name}
                </div>
                <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>
                  v{a.version} · {a.artifact_type}
                </div>
              </div>
            </div>
          ))
        ) : (
          <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)' }}>No artifacts yet</p>
        )}
      </div>

      {showArtifactModal && (
        <ArtifactModal
          sessionId={id!}
          onClose={() => { setShowArtifactModal(false); refetchArtifacts(); }}
        />
      )}
    </div>
  );
}

function ArtifactModal({ sessionId, onClose }: { sessionId: string; onClose: () => void }) {
  const [name, setName] = useState('');
  const [type, setType] = useState('doc');
  const [content, setContent] = useState('');
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  const save = async () => {
    if (!name || !content) return;
    setSaving(true);
    setError('');
    try {
      await api.createArtifact(sessionId, name, type, content);
      onClose();
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e));
    }
    setSaving(false);
  };

  return (
    <div style={modalOverlay} onClick={onClose}>
      <div style={modalStyle} onClick={e => e.stopPropagation()}>
        <h3 style={{ marginBottom: '1rem' }}>Create Artifact</h3>
        {error && <div style={{ color: 'var(--error)', fontSize: '0.8rem', marginBottom: 8 }}>{error}</div>}
        <input style={{ ...inputFieldStyle, marginBottom: 8 }} placeholder="Name" value={name} onChange={e => setName(e.target.value)} />
        <select style={{ ...inputFieldStyle, marginBottom: 8 }} value={type} onChange={e => setType(e.target.value)}>
          <option value="doc">Document</option>
          <option value="code">Code</option>
          <option value="design">Design</option>
          <option value="dataset">Dataset</option>
        </select>
        <textarea style={{ ...inputFieldStyle, minHeight: 150, resize: 'vertical' }} placeholder="Content..." value={content} onChange={e => setContent(e.target.value)} />
        <div style={{ display: 'flex', gap: 8, marginTop: 12, justifyContent: 'flex-end' }}>
          <button style={{ ...smallBtnStyle, background: 'var(--bg-hover)' }} onClick={onClose}>Cancel</button>
          <button style={smallBtnStyle} onClick={save} disabled={saving}>{saving ? 'Saving...' : 'Create'}</button>
        </div>
      </div>
    </div>
  );
}

const headerStyle: React.CSSProperties = { display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.75rem 1rem', background: 'var(--bg-card)', borderRadius: 8, marginBottom: '0.75rem' };
const messagesStyle: React.CSSProperties = { flex: 1, overflowY: 'auto', padding: '0.5rem', display: 'flex', flexDirection: 'column', gap: 8 };
const msgStyle = (role: string): React.CSSProperties => ({ display: 'flex', gap: 10, padding: '0.75rem', borderRadius: 8, background: role === 'user' ? 'var(--bg-card)' : role === 'system' ? '#3b111133' : 'transparent', border: role === 'system' ? '1px solid var(--error)' : 'none' });
const msgIconStyle: React.CSSProperties = { width: 28, height: 28, borderRadius: '50%', background: 'var(--bg-hover)', display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 };
const msgContentStyle: React.CSSProperties = { whiteSpace: 'pre-wrap', fontFamily: 'inherit', fontSize: '0.875rem', lineHeight: 1.6, margin: 0 };
const inputAreaStyle: React.CSSProperties = { display: 'flex', gap: 8, padding: '0.75rem', background: 'var(--bg-card)', borderRadius: 8, marginTop: '0.75rem' };
const selectStyle: React.CSSProperties = { padding: '0.5rem', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--bg)', color: 'var(--text)', fontSize: '0.8rem' };
const chatInputStyle: React.CSSProperties = { flex: 1, padding: '0.6rem 0.75rem', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--bg)', color: 'var(--text)', outline: 'none' };
const sendBtnStyle: React.CSSProperties = { padding: '0.5rem 0.75rem', borderRadius: 6, background: 'var(--primary)', color: '#fff', display: 'flex', alignItems: 'center' };
const smallBtnStyle: React.CSSProperties = { padding: '0.4rem 0.7rem', borderRadius: 6, background: 'var(--primary)', color: '#fff', fontSize: '0.75rem', fontWeight: 500, display: 'flex', alignItems: 'center', gap: 4 };
const sidebarStyle: React.CSSProperties = { width: 240, background: 'var(--bg-card)', borderRadius: 8, padding: '1rem', overflowY: 'auto' };
const artifactItemStyle: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 8, padding: '0.5rem', borderRadius: 6, background: 'var(--bg)', marginBottom: 4, cursor: 'pointer' };
const modalOverlay: React.CSSProperties = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.7)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 1000 };
const modalStyle: React.CSSProperties = { background: 'var(--bg-card)', borderRadius: 12, padding: '1.5rem', width: 480, maxHeight: '80vh', overflow: 'auto' };
const inputFieldStyle: React.CSSProperties = { width: '100%', padding: '0.5rem 0.75rem', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--bg)', color: 'var(--text)', outline: 'none' };
