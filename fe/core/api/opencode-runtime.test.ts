import { expect, it } from 'vitest';
import { cardSchema, runtimeKindSchema } from './schemas.js';

it('decodes the declared OpenCode executor runtime on an attached conversation card', () => {
  const card = cardSchema.parse({
    id: 'attached-card', track_id: 'operations-track', title: 'Original session',
    kind: 'codex', sort: 1, payload: { harness_profile: 'plain_chat' },
    runtime: {
      worker_session_id: 'attached-worker', kind: 'opencode', provider: 'opencode',
      status: 'idle', session_id: 'ses_original',
    },
    deletable: true, created_at: 1, updated_at: 1,
  });
  expect(card.runtime).toMatchObject({ kind: 'opencode', provider: 'opencode', session_id: 'ses_original' });
});

it('continues rejecting undeclared runtime kinds', () => {
  expect(runtimeKindSchema.safeParse('external-agent').success).toBe(false);
});
