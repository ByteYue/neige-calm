import { describe, expect, it } from 'vitest';
import { connectOpenCodeSessionOperation, openCodeConnectionsOperation } from './opencode-connections.js';
import { attachedOpenCodeSessionSchema, plannerRunOperation, trackConversationsOperation } from './conversation.js';

describe('existing OpenCode connection contract', () => {
  it('connects with registration and native ID, without sending a message or credentials', () => {
    const operation = connectOpenCodeSessionOperation('track one', { connection_id: 'ops', session_id: 'ses_existing' }, 'same-key');
    expect(operation.path).toBe('/api/tracks/track%20one/opencode-conversations');
    expect(operation.headers).toEqual({ 'Idempotency-Key': 'same-key' });
    expect(operation.body).toEqual({ connection_id: 'ops', session_id: 'ses_existing' });
    expect(openCodeConnectionsOperation().responseSchema.parse({ connections: [{ id: 'ops', label: 'Operations', directory: '/srv/etl' }] }))
      .toEqual({ connections: [{ id: 'ops', label: 'Operations', directory: '/srv/etl' }] });
  });

  it('keeps the OpenCode identity when an existing connection is listed after restart', () => {
    expect(trackConversationsOperation('w').responseSchema.parse([{
      id: 'card', trackId: 'w', title: 'ETL progress', kind: 'track-opencode', state: 'idle', updatedAt: 1, lastTurnCompletedAt: null,
    }])[0]?.kind).toBe('track-opencode');
  });

  it('requires native liveness and operation capabilities when attached metadata is supplied', () => {
    const bound = { connection_id: 'ops', label: 'Operations', session_id: 'ses_existing', directory: '/srv/etl',
      model: 'deepseek/flash', status: 'running', can_submit: false, can_stop: false };
    expect(attachedOpenCodeSessionSchema.safeParse(bound).success).toBe(true);
    expect(attachedOpenCodeSessionSchema.safeParse({ ...bound, can_submit: undefined }).success).toBe(false);
    expect(attachedOpenCodeSessionSchema.safeParse({ ...bound, status: 'done' }).success).toBe(false);
    expect(plannerRunOperation('card').responseSchema.parse({ card_id: 'card', model: null, reasoning_effort: null, blocked_reason: null,
      attached_session: bound, supports_steer: false }).attached_session).toEqual(bound);
  });

  it('rejects a run response that omitted its required steering capability', () => {
    expect(plannerRunOperation('card').responseSchema.safeParse({
      card_id: 'card', model: null, reasoning_effort: null, blocked_reason: null,
    }).success).toBe(false);
  });
});
