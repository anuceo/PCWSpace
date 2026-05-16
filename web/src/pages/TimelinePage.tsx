import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api } from '../lib/api';
import { GitBranch, RotateCcw, Clock, Layers } from 'lucide-react';

export default function TimelinePage() {
  const { sessionId } = useParams<{ sessionId: string }>();
  const navigate = useNavigate();
  const [replaySeq, setReplaySeq] = useState('0');
  const [forkSeq, setForkSeq] = useState('0');
  const [replayResult, setReplayResult] = useState<any>(null);
  const [error, setError] = useState('');

  const { data: deltaCount } = useQuery({
    queryKey: ['deltashot-count', sessionId],
    queryFn: () => api.getDeltashotCount(sessionId!),
    enabled: !!sessionId,
  });

  const { data: rollbackPoints } = useQuery({
    queryKey: ['rollback-points', sessionId],
    queryFn: () => api.getRollbackPoints(sessionId!),
    enabled: !!sessionId,
  });

  const doReplay = async () => {
    if (!sessionId) return;
    setError('');
    try {
      const result = await api.replay(sessionId, parseInt(replaySeq));
      setReplayResult(result);
    } catch (e: any) {
      setError(e.message);
    }
  };

  const doFork = async () => {
    if (!sessionId) return;
    setError('');
    try {
      const result = await api.forkSession(sessionId, parseInt(forkSeq));
      navigate(`/session/${result.session_id}`);
    } catch (e: any) {
      setError(e.message);
    }
  };

  return (
    <div>
      <h1 style={{ fontSize: '1.75rem', fontWeight: 700, marginBottom: 8 }}>
        Timeline
      </h1>
      <p style={{ color: 'var(--text-muted)', marginBottom: '1.5rem' }}>
        Session <code style={{ color: 'var(--cyan)' }}>{sessionId?.slice(0, 12)}...</code> — DeltaShot history, replay, and branching.
      </p>

      {error && <div style={errorStyle}>{error}</div>}

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: '1.5rem' }}>
        {/* DeltaShot Info */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}><Layers size={16} /> DeltaShots</h3>
          <div style={statStyle}>
            <span style={{ fontSize: '2rem', fontWeight: 700, color: 'var(--primary)' }}>
              {deltaCount?.count ?? 0}
            </span>
            <span style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>events recorded</span>
          </div>
          <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: 12 }}>
            Each DeltaShot is an encrypted, SHA-256 hash-chained diff stored in Redis Streams. Tamper-evident and replayable.
          </p>
        </div>

        {/* Rollback Points */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}><Clock size={16} /> Rollback Points</h3>
          {Array.isArray(rollbackPoints) && rollbackPoints.length > 0 ? (
            <div style={{ maxHeight: 200, overflowY: 'auto' }}>
              {rollbackPoints.map((p: any, i: number) => (
                <div key={i} style={pointStyle}>
                  <div style={{ width: 8, height: 8, borderRadius: '50%', background: 'var(--primary)', flexShrink: 0 }} />
                  <div>
                    <div style={{ fontSize: '0.8rem', fontWeight: 500 }}>
                      Seq {p.sequence ?? i} — {p.action ?? 'state_change'}
                    </div>
                    <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>{p.timestamp ?? ''}</div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>
              No rollback points yet. They appear when agent calls create DeltaShots.
            </p>
          )}
        </div>

        {/* Replay */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}><RotateCcw size={16} /> Replay State</h3>
          <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: 12 }}>
            Reconstruct session state at any point in history.
          </p>
          <div style={{ display: 'flex', gap: 8 }}>
            <input
              style={inputStyle}
              type="number"
              min={0}
              value={replaySeq}
              onChange={e => setReplaySeq(e.target.value)}
              placeholder="Sequence #"
            />
            <button style={btnStyle} onClick={doReplay}>Replay</button>
          </div>
          {replayResult && (
            <pre style={resultStyle}>{JSON.stringify(replayResult, null, 2)}</pre>
          )}
        </div>

        {/* Fork */}
        <div style={cardStyle}>
          <h3 style={cardTitleStyle}><GitBranch size={16} /> Fork Session</h3>
          <p style={{ fontSize: '0.8rem', color: 'var(--text-muted)', marginBottom: 12 }}>
            Create a branch to explore alternative paths without affecting the original.
          </p>
          <div style={{ display: 'flex', gap: 8 }}>
            <input
              style={inputStyle}
              type="number"
              min={0}
              value={forkSeq}
              onChange={e => setForkSeq(e.target.value)}
              placeholder="Fork at sequence..."
            />
            <button style={btnStyle} onClick={doFork}>
              <GitBranch size={14} /> Fork
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

const cardStyle: React.CSSProperties = { background: 'var(--bg-card)', border: '1px solid var(--border)', borderRadius: 12, padding: '1.5rem' };
const cardTitleStyle: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 8, fontSize: '1rem', fontWeight: 600, marginBottom: '1rem' };
const statStyle: React.CSSProperties = { display: 'flex', alignItems: 'baseline', gap: 8 };
const pointStyle: React.CSSProperties = { display: 'flex', alignItems: 'center', gap: 10, padding: '0.5rem 0', borderBottom: '1px solid var(--border)' };
const inputStyle: React.CSSProperties = { flex: 1, padding: '0.5rem 0.75rem', borderRadius: 6, border: '1px solid var(--border)', background: 'var(--bg)', color: 'var(--text)', outline: 'none' };
const btnStyle: React.CSSProperties = { padding: '0.5rem 0.75rem', borderRadius: 6, background: 'var(--primary)', color: '#fff', fontWeight: 500, fontSize: '0.8rem', display: 'flex', alignItems: 'center', gap: 4 };
const errorStyle: React.CSSProperties = { background: '#3b111155', border: '1px solid var(--error)', borderRadius: 8, padding: '0.75rem 1rem', marginBottom: '1rem', color: 'var(--error)', fontSize: '0.875rem' };
const resultStyle: React.CSSProperties = { marginTop: 12, padding: '0.75rem', borderRadius: 6, background: 'var(--bg)', fontSize: '0.75rem', overflow: 'auto', maxHeight: 150, fontFamily: 'monospace', whiteSpace: 'pre-wrap', margin: '12px 0 0 0' };
