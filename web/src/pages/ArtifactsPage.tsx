import { useState } from 'react';
import { api } from '../lib/api';
import { FileText, Code, Search, History } from 'lucide-react';

export default function ArtifactsPage() {
  const [artifactId, setArtifactId] = useState('');
  const [artifact, setArtifact] = useState<any>(null);
  const [versions, setVersions] = useState<string[]>([]);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const loadArtifact = async () => {
    if (!artifactId.trim()) return;
    setLoading(true);
    setError('');
    try {
      const a = await api.getArtifact(artifactId);
      setArtifact(a);
      const v = await api.listVersions(artifactId);
      setVersions(Array.isArray(v) ? v : []);
    } catch (e: any) {
      setError(e.message);
      setArtifact(null);
    }
    setLoading(false);
  };

  const loadVersion = async (versionId: string) => {
    setLoading(true);
    try {
      const a = await api.getArtifact(versionId);
      setArtifact(a);
    } catch (e: any) {
      setError(e.message);
    }
    setLoading(false);
  };

  return (
    <div>
      <h1 style={{ fontSize: '1.75rem', fontWeight: 700, marginBottom: 8 }}>Artifacts</h1>
      <p style={{ color: 'var(--text-muted)', marginBottom: '1.5rem' }}>
        View and explore versioned artifacts by ID.
      </p>

      <div style={{ display: 'flex', gap: 8, marginBottom: '1.5rem' }}>
        <input
          style={inputStyle}
          placeholder="Enter artifact ID..."
          value={artifactId}
          onChange={e => setArtifactId(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && loadArtifact()}
        />
        <button style={btnStyle} onClick={loadArtifact} disabled={loading}>
          <Search size={16} /> Load
        </button>
      </div>

      {error && <div style={errorStyle}>{error}</div>}

      {artifact && (
        <div style={{ display: 'flex', gap: '1.5rem' }}>
          {/* Content */}
          <div style={{ flex: 1 }}>
            <div style={cardStyle}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
                {artifact.artifact_type === 'code' ? <Code size={18} color="var(--cyan)" /> : <FileText size={18} color="var(--primary)" />}
                <h3 style={{ fontSize: '1rem', fontWeight: 600 }}>{artifact.name}</h3>
                <span style={badgeStyle}>{artifact.artifact_type}</span>
                <span style={badgeStyle}>v{artifact.version}</span>
              </div>

              <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginBottom: 12, display: 'flex', gap: 16 }}>
                <span>Hash: <code>{artifact.content_hash?.slice(0, 16)}...</code></span>
                <span>Created: {artifact.created_at?.slice(0, 19)}</span>
              </div>

              <pre style={contentStyle}>{artifact.content}</pre>
            </div>
          </div>

          {/* Version sidebar */}
          {versions.length > 0 && (
            <div style={{ width: 240 }}>
              <div style={cardStyle}>
                <h4 style={{ fontSize: '0.85rem', fontWeight: 600, marginBottom: 12, display: 'flex', alignItems: 'center', gap: 6 }}>
                  <History size={14} /> Versions ({versions.length})
                </h4>
                {versions.map((vid, i) => (
                  <div
                    key={vid}
                    onClick={() => loadVersion(vid)}
                    style={{
                      ...versionItemStyle,
                      background: vid === artifact.artifact_id ? 'var(--bg-hover)' : 'transparent',
                      borderColor: vid === artifact.artifact_id ? 'var(--primary)' : 'var(--border)',
                    }}
                  >
                    <span style={{ fontWeight: 500 }}>v{i + 1}</span>
                    <code style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>{vid.slice(0, 8)}</code>
                    {i === versions.length - 1 && <span style={{ fontSize: '0.65rem', color: 'var(--success)' }}>latest</span>}
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

const inputStyle: React.CSSProperties = { flex: 1, padding: '0.6rem 0.75rem', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--bg-card)', color: 'var(--text)', outline: 'none', maxWidth: 500 };
const btnStyle: React.CSSProperties = { padding: '0.6rem 1rem', borderRadius: 6, background: 'var(--primary)', color: '#fff', fontWeight: 500, fontSize: '0.875rem', display: 'flex', alignItems: 'center', gap: 6 };
const cardStyle: React.CSSProperties = { background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12, padding: '1.25rem' };
const badgeStyle: React.CSSProperties = { fontSize: '0.7rem', padding: '2px 8px', borderRadius: 4, background: 'var(--bg-hover)', color: 'var(--text-muted)' };
const contentStyle: React.CSSProperties = { whiteSpace: 'pre-wrap', fontFamily: "'JetBrains Mono', monospace", fontSize: '0.82rem', lineHeight: 1.6, background: 'var(--bg)', padding: '1rem', borderRadius: 8, overflow: 'auto', maxHeight: '60vh', margin: 0 };
const errorStyle: React.CSSProperties = { background: '#3b111155', border: '1px solid var(--error)', borderRadius: 8, padding: '0.75rem 1rem', marginBottom: '1rem', color: 'var(--error)', fontSize: '0.875rem' };
const versionItemStyle: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 8, padding: '0.5rem 0.6rem', borderRadius: 6, border: '1px solid var(--border)', marginBottom: 4, cursor: 'pointer', fontSize: '0.8rem' };
