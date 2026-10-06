import { expect, it } from 'vitest';
import { createCardRegistry } from '../registry.js';
import { partitionTrackCards } from './headless-filter.js';
import { PLAIN_CHAT_CARD_ENTRY } from './plain-chat.js';
import { registerAvailableBuiltinCards } from './register.js';

it.each([{}, { opencode_attachment: { connection_id: 'ops', generation: 1, port: 4096, directory: '/srv/etl', session_id: 'ses_original' } }])(
  'resolves an ordinary or attached plain chat through its headless owner (%j)', metadata => {
    const registry = createCardRegistry();
    registerAvailableBuiltinCards(registry);
    const card = { id: 'plain', track_id: 'track', kind: 'codex', title: 'ETL', sort: 1,
      payload: { schemaVersion: 1, harness_profile: 'plain_chat', ...metadata },
      deletable: true, created_at: 1, updated_at: 1 };
    expect(registry.resolve(card)).toEqual({ type: 'plain-chat', id: card.id });
    expect(registry.get('plain-chat')).toBe(PLAIN_CHAT_CARD_ENTRY);
    expect(partitionTrackCards(registry, [card])).toEqual({ visible: [], unknown: [] });
    expect(PLAIN_CHAT_CARD_ENTRY.create.mode).toBe('kernel-minted-only');
  },
);

it('leaves other kinds and conversation profiles to their own adapters', () => {
  for (const card of [
    { id: 'a', kind: 'codex', payload: { harness_profile: 'assistant' } },
    { id: 'b', kind: 'codex', payload: { planner_harness: true } },
    { id: 'c', kind: 'terminal', payload: { harness_profile: 'plain_chat' } },
    { id: 'd', kind: 'codex', payload: {} },
  ]) expect(PLAIN_CHAT_CARD_ENTRY.fromKernel(card)).toBeNull();
});
