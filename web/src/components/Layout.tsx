import { Outlet, NavLink } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api } from '../lib/api';
import { Activity, FileText, Workflow } from 'lucide-react';

export default function Layout() {
  const { data: health } = useQuery({
    queryKey: ['health'],
    queryFn: api.health,
    refetchInterval: 10000,
  });

  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      <nav style={navStyle}>
        <div style={logoStyle}>
          <Activity size={24} color="var(--primary)" />
          <span style={{ fontWeight: 700, fontSize: '1.1rem' }}>PCWSpace</span>
        </div>

        <div style={linksStyle}>
          <NavItem to="/" icon={<Activity size={18} />} label="Dashboard" />
          <NavItem to="/artifacts" icon={<FileText size={18} />} label="Artifacts" />
          <NavItem to="/workflows" icon={<Workflow size={18} />} label="Workflows" />
        </div>

        <div style={statusStyle}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <div style={{
              width: 8, height: 8, borderRadius: '50%',
              background: health?.redis ? 'var(--success)' : 'var(--error)',
            }} />
            <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
              {health?.status === 'ok' ? 'All systems operational' : 'Degraded'}
            </span>
          </div>
          <span style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>
            v{health?.version || '?'}
          </span>
        </div>
      </nav>

      <main style={mainStyle}>
        <Outlet />
      </main>
    </div>
  );
}

function NavItem({ to, icon, label }: { to: string; icon: React.ReactNode; label: string }) {
  return (
    <NavLink
      to={to}
      style={({ isActive }) => ({
        ...navItemStyle,
        background: isActive ? 'var(--bg-hover)' : 'transparent',
        color: isActive ? 'var(--text)' : 'var(--text-muted)',
      })}
    >
      {icon}
      <span>{label}</span>
    </NavLink>
  );
}

const navStyle: React.CSSProperties = {
  width: 220,
  background: 'var(--bg-card)',
  borderRight: '1px solid var(--border)',
  display: 'flex',
  flexDirection: 'column',
  padding: '1rem 0',
};

const logoStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 10,
  padding: '0.5rem 1.25rem 1.5rem',
  borderBottom: '1px solid var(--border)',
  marginBottom: '1rem',
};

const linksStyle: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  gap: 2,
  flex: 1,
};

const navItemStyle: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 10,
  padding: '0.6rem 1.25rem',
  borderRadius: 6,
  margin: '0 0.5rem',
  fontSize: '0.875rem',
  transition: 'all 0.15s',
};

const statusStyle: React.CSSProperties = {
  padding: '1rem 1.25rem',
  borderTop: '1px solid var(--border)',
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
};

const mainStyle: React.CSSProperties = {
  flex: 1,
  padding: '2rem',
  overflowY: 'auto',
};
