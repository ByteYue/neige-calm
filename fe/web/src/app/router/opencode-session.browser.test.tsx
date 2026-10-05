import '../../styles/entry.css';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider, createMemoryHistory } from '@tanstack/react-router';
import { cleanup, render } from '@testing-library/react';
import { afterEach, expect, it } from 'vitest';
import { page } from 'vitest/browser';
import type { ApiRequest, ApiTransportResponse } from '../../../../core/api/types.ts';
import { createUnauthorizedChannel } from '../../../../core/api/unauthorized.ts';
import { ThemeProvider } from '../theme/public.tsx';
import { createAppRouter } from './public.tsx';
import { bootTestCardRuntime } from './test-card-runtime.ts';

afterEach(async () => { cleanup(); await page.viewport(1280, 720); });
const SESSION = 'ses_f044556f2fferyPFhTwDGFJz97';
const DIRECTORY = '/home/operator/very-long-project-directory/operations/etl-and-audit';
const ROW = { id: 'external', trackId: 'w', title: 'iFood progress', kind: 'track-opencode', state: 'running', updatedAt: 2, lastTurnCompletedAt: null };

function mount(existing = false, nativeStatus: () => 'running' | 'idle' | 'unavailable' = () => 'running') {
  const requests: ApiRequest[] = [];
  let connected = existing;
  const ok = (body: unknown): ApiTransportResponse => ({ status: 200, statusText: 'OK', body });
  const area = { id: 'a', name: 'Operations', color: '#000', sort: 1, kind: 'user', created_at: 1, updated_at: 1 };
  const track = { id: 'w', area_id: 'a', title: 'ETL', sort: 1, cwd: '/srv/neige', created_at: 1, updated_at: 1 };
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const transport = { send(request: ApiRequest) { return Promise.resolve().then(() => {
    requests.push(request);
    if (request.path === '/api/areas') return ok([area]);
    if (request.path === '/api/areas/a/tracks') return ok([track]);
    if (request.path === '/api/tracks/w') return ok({ track, cards: connected ? [{ id: ROW.id, track_id: 'w', kind: 'codex', title: ROW.title, sort: 1, role: 'worker',
      payload: { schemaVersion: 1, harness_profile: 'plain_chat', opencode_attachment: { connection_id: 'ops',
        generation: 1, port: 4096, directory: DIRECTORY, session_id: SESSION } },
      deletable: true, created_at: 1, updated_at: 2 }] : [], overlays: [], can_close: true, can_reopen: false });
    if (request.path === '/api/overlays?entity_kind=track') return ok([]);
    if (request.path === '/api/tracks/w/conversations') return ok(connected ? [ROW] : []);
    if (request.path === '/api/opencode/connections') return ok({ connections: [{ id: 'ops', label: 'Crawler operations', directory: DIRECTORY }] });
    if (request.path === '/api/tracks/w/opencode-conversations') { connected = true; return { ...ok(ROW), status: 201 }; }
    if (request.path.endsWith('/planner/run')) return ok({ card_id: ROW.id, worker_session_id: 'r', phase: nativeStatus() === 'running' ? 'turn_running' : 'idle',
      model: 'deepseek/flash', reasoning_effort: null, blocked_reason: null, running_turn: null, supports_steer: false,
      attached_session: { connection_id: 'ops', label: 'Crawler operations', session_id: SESSION, directory: DIRECTORY,
        model: 'deepseek/flash', status: nativeStatus(), can_submit: nativeStatus() === 'idle', can_stop: false } });
    if (request.path.endsWith('/harness/live')) return ok({ turn_id: null, items: [] });
    if (request.path.includes('/harness/items')) return ok([{
      id: 1, worker_session_id: 'r', card_id: ROW.id, track_id: 'w', thread_id: SESSION, turn_id: null,
      turn_error_text: null, item_uuid: 'output', item_type: 'agentMessage', method: 'item/completed',
      params: JSON.stringify({ item: { id: 'output', type: 'agentMessage', text: 'Original iFood progress output' } }), created_at_ms: 1,
    }]);
    if (request.path.startsWith('/api/models')) return ok({ models: [], default: { model: null, reasoning_effort: null,
      supported_reasoning_efforts: null }, default_source: 'unknown', source: 'unavailable', fetched_at_ms: 1 });
    if (request.path === '/api/settings') return ok({});
    return ok([]);
  }); } };
  const router = createAppRouter({ transport, client, unauthorized: createUnauthorizedChannel({ enqueue: task => task() }),
    cards: bootTestCardRuntime(), onSignOut: () => undefined });
  router.update({ history: createMemoryHistory({ initialEntries: ['/track/w'] }) });
  render(<QueryClientProvider client={client}><ThemeProvider><RouterProvider router={router} /></ThemeProvider></QueryClientProvider>);
  return requests;
}

it.each([1280, 390])('connects and observes original history with a readable native identity (%ipx)', async width => {
  await page.viewport(width, 844);
  const requests = mount();
  if (width < 600) {
    await page.getByRole('button', { name: 'Track actions' }).click();
    await page.getByRole('menuitem', { name: 'Conversations', exact: true }).click();
  }
  await page.getByRole('button', { name: 'Connect OpenCode session' }).click();
  await page.getByRole('button', { name: 'Connection: Choose connection' }).click();
  await page.getByRole('menuitem', { name: 'Crawler operations' }).click();
  await page.getByRole('textbox', { name: /^OpenCode session ID/ }).fill(SESSION);
  await page.getByRole('button', { name: 'Connect session', exact: true }).click();
  await expect.element(page.getByText('Original iFood progress output')).toBeVisible();
  const identity = page.getByRole('note', { name: 'Connected OpenCode session' });
  await expect.element(identity).toHaveTextContent(SESSION);
  await expect.element(identity).toHaveTextContent(DIRECTORY);
  await expect.element(identity).toHaveTextContent('Observing');
  await expect.element(page.getByRole('button', { name: 'Stop', exact: true })).not.toBeInTheDocument();
  await expect.element(page.getByRole('button', { name: 'Say it now' })).not.toBeInTheDocument();
  expect(document.documentElement.scrollWidth).toBe(width);
  expect((await identity.findElement()).getBoundingClientRect().right).toBeLessThanOrEqual(width);
  expect(requests.filter(request => request.path === '/api/tracks/w/opencode-conversations')).toHaveLength(1);
  expect(requests.find(request => request.path === '/api/tracks/w/opencode-conversations')?.body)
    .toEqual({ connection_id: 'ops', session_id: SESSION });
  expect(requests.filter(request => request.path.endsWith('/planner/input'))).toHaveLength(0);
});


it.each([1280, 390])('reopens a persisted native card through Conversations without a dead card row (%ipx)', async width => {
  await page.viewport(width, 844);
  const requests = mount(true);
  if (width < 600) {
    await page.getByRole('button', { name: 'Track actions' }).click();
    await page.getByRole('menuitem', { name: 'Conversations', exact: true }).click();
  }
  await page.getByRole('button', { name: /^Conversation iFood progress/ }).click();
  await expect.element(page.getByText('Original iFood progress output')).toBeVisible();
  expect(document.querySelector('[data-nc-card-inventory] [data-nc-row="external"]')).toBeNull();
  await expect.element(page.getByRole('note', { name: 'Connected OpenCode session' })).toHaveTextContent(SESSION);
  expect(document.documentElement.scrollWidth).toBe(width);
  expect(requests.filter(request => request.method === 'POST')).toHaveLength(0);
});


it.each(['unavailable', 'running'] as const)('refreshes silent %s to ready without changing history or reloading', async initial => {
  let status: 'unavailable' | 'running' | 'idle' = initial;
  const requests = mount(true, () => status);
  await page.getByRole('button', { name: /^Conversation iFood progress/ }).click();
  await expect.element(page.getByText('Original iFood progress output')).toBeVisible();
  await expect.element(page.getByRole('combobox', { name: 'Message' })).toHaveAttribute('contenteditable', 'false');
  await expect.element(page.getByRole('note', { name: 'Connected OpenCode session' }))
    .toHaveTextContent(initial === 'running' ? 'Observing' : 'unavailable');
  const historyReads = requests.filter(request => request.path.includes('/harness/items')).length;
  status = 'idle';
  await expect.element(page.getByRole('combobox', { name: 'Message' })).toHaveAttribute('contenteditable', 'true');
  await expect.element(page.getByRole('note', { name: 'Connected OpenCode session' })).toHaveTextContent('Ready to continue');
  expect(requests.filter(request => request.path.includes('/harness/items'))).toHaveLength(historyReads);
  expect(requests.filter(request => request.method === 'POST')).toHaveLength(0);
}, 10000);
