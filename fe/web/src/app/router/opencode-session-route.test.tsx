// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider, createMemoryHistory } from '@tanstack/react-router';
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import type { ApiRequest, ApiTransportResponse } from '../../../../core/api/types.ts';
import { createUnauthorizedChannel } from '../../../../core/api/unauthorized.ts';
import { ThemeProvider } from '../theme/public.tsx';
import { createAppRouter } from './public.tsx';
import { bootTestCardRuntime } from './test-card-runtime.ts';

beforeEach(() => { vi.stubGlobal('scrollTo', vi.fn()); });
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
const AREA = { id: 'a', name: 'Operations', color: '#000', sort: 1, kind: 'user', created_at: 1, updated_at: 1 };
const TRACK = { id: 'w', area_id: 'a', title: 'ETL', sort: 1, cwd: '/srv/neige', created_at: 1, updated_at: 1 };
const ROW = { id: 'attached', trackId: 'w', title: 'iFood progress', kind: 'track-opencode', state: 'idle', updatedAt: 2, lastTurnCompletedAt: null };
const BOUND = { connection_id: 'ops', label: 'Crawler operations', session_id: 'ses_existing', directory: '/srv/crawler',
  model: 'deepseek/flash', status: 'idle', can_submit: true, can_stop: false };
const CONNECT = '/api/tracks/w/opencode-conversations';
const ok = (body: unknown): ApiTransportResponse => ({ status: 200, statusText: 'OK', body });

function mount(options: { bound?: Partial<typeof BOUND> | null; existing?: boolean;
  connect?: (request: ApiRequest) => Promise<ApiTransportResponse> } = {}) {
  const requests: ApiRequest[] = [];
  let connected = options.existing === true;
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const transport = { async send(request: ApiRequest) {
    requests.push(request);
    if (request.path === '/api/areas') return ok([AREA]);
    if (request.path === '/api/areas/a/tracks') return ok([TRACK]);
    if (request.path === '/api/overlays?entity_kind=track') return ok([]);
    if (request.path === '/api/tracks/w') return ok({ track: TRACK, cards: [], overlays: [], can_close: true, can_reopen: false });
    if (request.path === '/api/tracks/w/conversations') return ok(connected ? [ROW] : []);
    if (request.path === '/api/opencode/connections') return ok({ connections: [{ id: 'ops', label: BOUND.label, directory: BOUND.directory }] });
    if (request.path === CONNECT) {
      if (options.connect) return options.connect(request);
      connected = true; return { ...ok(ROW), status: 201 };
    }
    if (request.path.endsWith('/planner/run')) return ok({ card_id: ROW.id, worker_session_id: 'runtime',
      phase: options.bound?.status === 'running' ? 'turn_running' : 'idle', model: 'deepseek/flash', reasoning_effort: null,
      blocked_reason: null, supports_steer: false,
      ...(options.bound === null ? {} : { attached_session: { ...BOUND, ...options.bound } }),
      pending: options.bound?.status === 'running' ? [{ entry_id: 'entry', text: 'Queued elsewhere', rev: 1, queued_at_ms: 2 }] : [],
    });
    if (request.path.includes('/harness/items')) return ok([{
      id: 1, worker_session_id: 'runtime', card_id: ROW.id, track_id: 'w', thread_id: 'ses_existing',
      turn_id: null, turn_error_text: null, item_uuid: 'original', item_type: 'agentMessage', method: 'item/completed',
      params: JSON.stringify({ item: { id: 'original', type: 'agentMessage', text: 'Original ETL output' } }), created_at_ms: 1,
    }]);
    if (request.path.endsWith('/planner/input')) return ok({ card_id: ROW.id, worker_session_id: 'runtime' });
    if (request.path.startsWith('/api/models')) return ok({ models: [], default: { model: null, reasoning_effort: null,
      supported_reasoning_efforts: null }, default_source: 'unknown', source: 'unavailable', fetched_at_ms: 1 });
    if (request.path === '/api/settings') return ok({});
    return ok([]);
  } };
  const router = createAppRouter({ transport, client, unauthorized: createUnauthorizedChannel({ enqueue: task => task() }),
    cards: bootTestCardRuntime(), onSignOut: () => undefined });
  router.update({ history: createMemoryHistory({ initialEntries: ['/track/w'] }) });
  render(<QueryClientProvider client={client}><ThemeProvider><RouterProvider router={router} /></ThemeProvider></QueryClientProvider>);
  return { requests, router, client };
}

async function connectThroughPage() {
  fireEvent.click((await screen.findAllByRole('button', { name: 'Connect OpenCode session' }))[0]);
  const dialog = await screen.findByRole('dialog', { name: 'Connect OpenCode session' });
  fireEvent.click(await within(dialog).findByRole('button', { name: 'Connection: Choose connection' }));
  fireEvent.click(await screen.findByRole('menuitem', { name: BOUND.label }));
  fireEvent.change(within(dialog).getByRole('textbox', { name: /^OpenCode session ID/ }), { target: { value: 'ses_existing' } });
  fireEvent.click(within(dialog).getByRole('button', { name: 'Connect session' }));
}

it('connects through the Track panel and opens original history without sending a prompt', async () => {
  const { requests } = mount();
  await connectThroughPage();
  await screen.findByText('Original ETL output');
  expect(screen.getByRole('note', { name: 'Connected OpenCode session' }).textContent).toContain('ses_existing');
  expect(screen.getByRole('note', { name: 'Connected OpenCode session' }).textContent).toContain('/srv/crawler');
  const connects = requests.filter(request => request.path === CONNECT);
  expect(connects).toHaveLength(1);
  expect(connects[0]?.body).toEqual({ connection_id: 'ops', session_id: 'ses_existing' });
  expect(connects[0]?.headers?.['Idempotency-Key']).toBeTruthy();
  expect(requests.filter(request => request.path.endsWith('/planner/input'))).toHaveLength(0);
});

it('reopens a persisted native conversation without connecting or sending again', async () => {
  const { requests } = mount({ existing: true });
  fireEvent.click((await screen.findAllByRole('button', { name: /iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  expect(screen.getByRole('note', { name: 'Connected OpenCode session' }).textContent).toContain('ses_existing');
  expect(requests.filter(request => request.method === 'POST')).toHaveLength(0);
  fireEvent.click(screen.getByRole('button', { name: 'Close conversation' }));
  expect(requests.filter(request => request.method === 'DELETE' || request.path.endsWith('/planner/interrupt'))).toHaveLength(0);
});

it('observes a foreign running turn without sending, stopping, steering or changing model', async () => {
  const { requests } = mount({ existing: true, bound: { status: 'running', can_submit: false } });
  fireEvent.click((await screen.findAllByRole('button', { name: /iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  expect(screen.getByRole('note', { name: 'Connected OpenCode session' }).textContent).toContain('Observing');
  expect(screen.getByRole('combobox', { name: 'Message' }).getAttribute('contenteditable')).toBe('false');
  expect(screen.queryByRole('button', { name: 'Stop', exact: true })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Say it now' })).toBeNull();
  expect(screen.getByRole('button', { name: /^Model:/ }).hasAttribute('disabled')).toBe(true);
  expect(requests.filter(request => request.method === 'POST')).toHaveLength(0);
});


it('continues an idle bound conversation through the existing input endpoint', async () => {
  const { requests } = mount({ existing: true });
  fireEvent.click((await screen.findAllByRole('button', { name: /iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  const field = screen.getByRole('combobox', { name: 'Message' });
  expect(field.getAttribute('contenteditable')).toBe('true');
  field.textContent = 'Check ETL progress';
  fireEvent.input(field);
  fireEvent.keyDown(field, { key: 'Enter' });
  await waitFor(() => expect(requests.filter(request => request.path.endsWith('/planner/input'))).toHaveLength(1));
  const submitted = requests.find(request => request.path.endsWith('/planner/input'));
  expect(submitted?.path).toBe('/api/cards/attached/planner/input');
  expect(submitted?.body).toMatchObject({ text: 'Check ETL progress' });
  expect(requests.filter(request => request.path === CONNECT)).toHaveLength(0);
});

it('blocks continuation when the bound native-session metadata has not been read', async () => {
  mount({ existing: true, bound: null });
  fireEvent.click((await screen.findAllByRole('button', { name: /iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  expect(screen.getByRole('combobox', { name: 'Message' }).getAttribute('contenteditable')).toBe('false');
  expect(screen.queryByRole('button', { name: 'Stop', exact: true })).toBeNull();
});

it('retains the exact connection request and key after a lost response and Track navigation', async () => {
  let attempts = 0;
  const { requests, router } = mount({ connect: () => {
    attempts += 1;
    return Promise.resolve(attempts === 1 ? { status: 504, statusText: 'Timeout', body: { error: 'Connection acknowledgement lost' } }
      : { status: 201, statusText: 'Created', body: ROW });
  } });
  await connectThroughPage();
  await screen.findByText('Connection acknowledgement lost');
  await act(async () => { await router.navigate({ to: '/' }); });
  await act(async () => { await router.navigate({ to: '/track/$trackId', params: { trackId: 'w' } }); });
  fireEvent.click((await screen.findAllByRole('button', { name: 'Connect OpenCode session' }))[0]);
  const dialog = await screen.findByRole('dialog', { name: 'Connect OpenCode session' });
  expect(within(dialog).getByRole('textbox', { name: /^OpenCode session ID/ }).hasAttribute('disabled')).toBe(true);
  fireEvent.click(within(dialog).getByRole('button', { name: 'Retry connection' }));
  await waitFor(() => expect(requests.filter(request => request.path === CONNECT)).toHaveLength(2));
  const [first, second] = requests.filter(request => request.path === CONNECT);
  expect(second?.body).toEqual(first?.body);
  expect(second?.headers).toEqual(first?.headers);
  expect(requests.filter(request => request.path.endsWith('/planner/input'))).toHaveLength(0);
});
