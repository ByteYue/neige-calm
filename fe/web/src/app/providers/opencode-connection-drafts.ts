import type { Conversation } from '../../../../core/domain/conversation.ts';
import type { OpenCodeConnectRequest } from '../../../../core/domain/opencode-connections.ts';
import { mintIdempotencyKey } from '../router/idempotency-key.ts';

export type ConnectionDraft = Readonly<{
  connectionId: string; sessionId: string; key: string; busy: boolean; error: string | null;
  request: OpenCodeConnectRequest | null; connected: Conversation | null;
}>;

export function createConnectionDraftStore() {
  const drafts = new Map<string, ConnectionDraft>();
  const listeners = new Set<() => void>();
  const notify = () => { for (const listener of listeners) listener(); };
  return {
    subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    get: (trackId: string) => drafts.get(trackId) ?? null,
    update: (trackId: string, patch: Partial<ConnectionDraft>) => {
      const current = drafts.get(trackId) ?? { connectionId: '', sessionId: '', key: mintIdempotencyKey(),
        busy: false, error: null, request: null, connected: null };
      drafts.set(trackId, { ...current, ...patch }); notify();
    },
    forget: (trackId: string) => { drafts.delete(trackId); notify(); },
  };
}
