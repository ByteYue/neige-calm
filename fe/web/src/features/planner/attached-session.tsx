import type { AttachedOpenCodeSession } from '../../../../core/domain/conversation.ts';
import styles from './attached-session.module.css';

/** Read-only identity and liveness declared by the connection owner. */
export function AttachedSessionNotice({ session }: { session: AttachedOpenCodeSession }) {
  const status = session.status === 'running' ? 'Observing the running OpenCode session.'
    : session.status === 'idle' ? 'Ready to continue the original OpenCode session.'
      : session.status === 'unavailable' ? 'The OpenCode connection is unavailable. History is retained.'
        : 'OpenCode progress is not confirmed. Reconnecting will not resend messages.';
  return <div className={styles.notice} role="note" aria-label="Connected OpenCode session">
    <strong>OpenCode{session.model === null ? '' : ` · ${session.model}`}</strong>
    <span>{session.label} · {session.session_id}</span>
    <span>Working directory: {session.directory}</span>
    <span>{status}{!session.can_submit && session.status === 'running' ? ' Continue after the current operation finishes.' : ''}</span>
  </div>;
}
