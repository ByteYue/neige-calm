// @vitest-environment jsdom
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider, createMemoryHistory } from '@tanstack/react-router';
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import type { ApiRequest, ApiTransportResponse } from '../../../../core/api/types.ts';
import { createUnauthorizedChannel } from '../../../../core/api/unauthorized.ts';
import { queryKeys } from '../providers/queries.ts';
import { ThemeProvider } from '../theme/public.tsx';
import { createAppRouter } from './public.tsx';
import { bootTestCardRuntime } from './test-card-runtime.ts';

beforeEach(() => { vi.stubGlobal('scrollTo', vi.fn()); });
afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals(); });
const AREA = { id: 'a', name: 'Operations', color: '#000', sort: 1, kind: 'user', created_at: 1, updated_at: 1 };
const TRACK = { id: 'w', area_id: 'a', title: 'ETL', sort: 1, cwd: '/srv/neige', created_at: 1, updated_at: 1 };
const ROW = { id: 'attached', trackId: 'w', title: 'iFood progress', kind: 'track-opencode', state: 'idle', updatedAt: 2, lastTurnCompletedAt: null };
const BOUND = { connection_id: 'ops', label: 'Crawler operations', session_id: 'ses_existing', directory: '/srv/crawler',
  model: 'deepseek/flash', status: 'idle', can_submit: true, can_stop: false };
const NATIVE_CARD = { id: ROW.id, track_id: 'w', kind: 'codex', title: ROW.title, sort: 1, role: 'worker',
  payload: { schemaVersion: 1, harness_profile: 'plain_chat', opencode_attachment: { connection_id: 'ops',
    generation: 1, port: 4096, directory: BOUND.directory, session_id: BOUND.session_id } },
  deletable: true, created_at: 1, updated_at: 2 };
const CONNECT = '/api/tracks/w/opencode-conversations';
const ok = (body: unknown): ApiTransportResponse => ({ status: 200, statusText: 'OK', body });

function mount(options: { bound?: Partial<typeof BOUND> | null; existing?: boolean;
  runRead?: () => ApiTransportResponse; history?: readonly unknown[];
  input?: (request: ApiRequest) => Promise<ApiTransportResponse>;
  connect?: (request: ApiRequest) => Promise<ApiTransportResponse> } = {}) {
  const requests: ApiRequest[] = [];
  let connected = options.existing === true;
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const transport = { async send(request: ApiRequest) {
    requests.push(request);
    if (request.path === '/api/areas') return ok([AREA]);
    if (request.path === '/api/areas/a/tracks') return ok([TRACK]);
    if (request.path === '/api/overlays?entity_kind=track') return ok([]);
    if (request.path === '/api/tracks/w') return ok({ track: TRACK, cards: connected ? [NATIVE_CARD] : [], overlays: [], can_close: true, can_reopen: false });
    if (request.path === '/api/tracks/w/conversations') return ok(connected ? [ROW] : []);
    if (request.path === '/api/opencode/connections') return ok({ connections: [{ id: 'ops', label: BOUND.label, directory: BOUND.directory }] });
    if (request.path === CONNECT) {
      if (options.connect) return options.connect(request);
      connected = true; return { ...ok(ROW), status: 201 };
    }
    if (request.path.endsWith('/planner/run') && options.runRead) return options.runRead();
    if (request.path.endsWith('/planner/run')) return ok({ card_id: ROW.id, worker_session_id: 'runtime',
      phase: options.bound?.status === 'running' ? 'turn_running' : 'idle', model: 'deepseek/flash', reasoning_effort: null,
      blocked_reason: null, running_turn: null, supports_steer: false,
      ...(options.bound === null ? {} : { attached_session: { ...BOUND, ...options.bound } }),
      pending: options.bound?.status === 'running' ? [{ entry_id: 'entry', text: 'Queued elsewhere', rev: 1, queued_at_ms: 2 }] : [],
    });
    if (request.path.endsWith('/harness/live')) return ok({ turn_id: null, items: [] });
    if (request.path.includes('/harness/items') && options.history) return ok(options.history);
    if (request.path.includes('/harness/items')) return ok([{
      id: 1, worker_session_id: 'runtime', card_id: ROW.id, track_id: 'w', thread_id: 'ses_existing',
      turn_id: null, turn_error_text: null, item_uuid: 'original', item_type: 'agentMessage', method: 'item/completed',
      params: JSON.stringify({ item: { id: 'original', type: 'agentMessage', text: 'Original ETL output' } }), created_at_ms: 1,
    }]);
    if (request.path.endsWith('/planner/input')) return options.input ? options.input(request)
      : ok({ card_id: ROW.id, worker_session_id: 'runtime', entry_id: 'queued-input' });
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
  fireEvent.click((await screen.findAllByRole('button', { name: /^Conversation iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  expect(screen.getByRole('note', { name: 'Connected OpenCode session' }).textContent).toContain('ses_existing');
  expect(requests.filter(request => request.method === 'POST')).toHaveLength(0);
  expect(document.querySelector('[data-nc-card-inventory] [data-nc-row="attached"]')).toBeNull();
  fireEvent.click(screen.getByRole('button', { name: 'Close conversation' }));
  expect(requests.filter(request => request.method === 'DELETE' || request.path.endsWith('/planner/interrupt'))).toHaveLength(0);
});

it('observes a foreign running turn without sending, stopping, steering or changing model', async () => {
  const { requests } = mount({ existing: true, bound: { status: 'running', can_submit: false } });
  fireEvent.click((await screen.findAllByRole('button', { name: /^Conversation iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  expect(screen.getByRole('note', { name: 'Connected OpenCode session' }).textContent).toContain('Observing');
  expect(screen.getByRole('combobox', { name: 'Message' }).getAttribute('contenteditable')).toBe('false');
  expect(screen.queryByRole('button', { name: 'Stop' })).toBeNull();
  expect(screen.queryByRole('button', { name: 'Say it now' })).toBeNull();
  expect(screen.queryByRole('button', { name: /^Model:/ })).toBeNull();
  expect(requests.filter(request => request.path.startsWith('/api/models'))).toHaveLength(0);
  fireEvent.keyDown(screen.getByRole('combobox', { name: 'Message' }), { key: 'Escape' });
  await act(async () => { await Promise.resolve(); });
  expect(requests.filter(request => request.method !== 'GET')).toHaveLength(0);
});


it('continues an idle bound conversation through the existing input endpoint', async () => {
  const { requests } = mount({ existing: true });
  fireEvent.click((await screen.findAllByRole('button', { name: /^Conversation iFood progress/ }))[0]);
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
  fireEvent.click((await screen.findAllByRole('button', { name: /^Conversation iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  expect(screen.getByRole('combobox', { name: 'Message' }).getAttribute('contenteditable')).toBe('false');
  expect(screen.queryByRole('button', { name: 'Stop' })).toBeNull();
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


it.each(['unavailable', 'running', 'initial failure'] as const)(
  'refreshes %s native status with unchanged history and stops polling after close', async initial => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let status: 'unavailable' | 'running' | 'initial failure' | 'idle' = initial;
    const { requests, client } = mount({ existing: true, runRead: () => {
      if (status === 'initial failure') return { status: 503, statusText: 'Unavailable', body: { error: 'OpenCode connection unavailable' } };
      return ok({ card_id: ROW.id, worker_session_id: 'runtime', phase: status === 'running' ? 'turn_running' : 'idle',
        model: 'deepseek/flash', reasoning_effort: null, blocked_reason: null, running_turn: null, supports_steer: false, pending: [],
        attached_session: { connection_id: 'ops', label: BOUND.label, session_id: BOUND.session_id, directory: BOUND.directory,
          model: BOUND.model, status, can_submit: status === 'idle', can_stop: false } });
    } });
    fireEvent.click((await screen.findAllByRole('button', { name: /^Conversation iFood progress/ }))[0]);
    await screen.findByText('Original ETL output');
    await waitFor(() => expect(requests.filter(request => request.path.endsWith('/planner/run'))).toHaveLength(1));
    if (initial === 'initial failure') {
      await waitFor(() => expect(client.getQueryState(queryKeys.plannerRun(ROW.id))?.status).toBe('error'));
    } else {
      await waitFor(() => expect(screen.getByRole('note', { name: 'Connected OpenCode session' }).textContent)
        .toContain(initial === 'running' ? 'Observing' : 'unavailable'));
    }
    expect(screen.getByRole('combobox', { name: 'Message' }).getAttribute('contenteditable')).toBe('false');
    const historyReads = requests.filter(request => request.path.includes('/harness/items')).length;
    status = 'idle';
    await act(async () => { await vi.advanceTimersByTimeAsync(3100); });
    await waitFor(() => expect(screen.getByRole('combobox', { name: 'Message' }).getAttribute('contenteditable')).toBe('true'));
    expect(screen.getByRole('note', { name: 'Connected OpenCode session' }).textContent).toContain('Ready to continue');
    expect(requests.filter(request => request.path.includes('/harness/items'))).toHaveLength(historyReads);
    expect(requests.filter(request => request.method === 'POST')).toHaveLength(0);
    fireEvent.click(screen.getByRole('button', { name: 'Close conversation' }));
    const runReads = requests.filter(request => request.path.endsWith('/planner/run')).length;
    await act(async () => { await vi.advanceTimersByTimeAsync(6200); });
    expect(requests.filter(request => request.path.endsWith('/planner/run'))).toHaveLength(runReads);
  },
);


it('retains a lost input acknowledgement key while the native session becomes busy', async () => {
  const bound = { status: 'idle', can_submit: true };
  let attempts = 0;
  const mounted = mount({ existing: true, bound, input: async () => {
    attempts += 1;
    if (attempts === 1) {
      bound.status = 'running'; bound.can_submit = false;
      await mounted.client.invalidateQueries({ queryKey: queryKeys.plannerRun(ROW.id) });
      return { status: 504, statusText: 'Timeout', body: { error: 'Input acknowledgement lost' } };
    }
    return ok({ card_id: ROW.id, worker_session_id: 'runtime', entry_id: 'same-native-input' });
  } });
  fireEvent.click((await screen.findAllByRole('button', { name: /^Conversation iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  const field = screen.getByRole('combobox', { name: 'Message' });
  field.textContent = 'Check ETL progress'; fireEvent.input(field); fireEvent.keyDown(field, { key: 'Enter' });
  await waitFor(() => expect(mounted.requests.filter(request => request.path.endsWith('/planner/input'))).toHaveLength(2));
  const [first, replay] = mounted.requests.filter(request => request.path.endsWith('/planner/input'));
  expect(first?.headers?.['Idempotency-Key']).toBeTruthy();
  expect(replay?.headers).toEqual(first?.headers);
  expect(replay?.body).toEqual(first?.body);
  expect(replay?.body).toEqual({ text: 'Check ETL progress' });
  expect(field.getAttribute('contenteditable')).toBe('false');
  expect(mounted.requests.filter(request => request.path.endsWith('/planner/interrupt'))).toHaveLength(0);
});

it('keeps native completed history readable without offering a local replacement Edit', async () => {
  const base = { worker_session_id: 'runtime', card_id: ROW.id, track_id: 'w', thread_id: 'ses_existing',
    turn_id: 'native-turn', turn_error_text: null, created_at_ms: 1 };
  const { requests } = mount({ existing: true, history: [
    { ...base, id: 1, item_uuid: 'native-user', item_type: 'userMessage', method: 'item/completed',
      input_segments: [{ presentation: 'user', text: 'User says:\nCheck ETL progress', attachments: [] }],
      params: JSON.stringify({ item: { id: 'native-user', type: 'userMessage', content: [{ text: 'Check ETL progress' }] } }) },
    { ...base, id: 2, item_uuid: 'native-answer', item_type: 'agentMessage', method: 'item/completed',
      params: JSON.stringify({ item: { id: 'native-answer', type: 'agentMessage', text: 'Original ETL output' } }) },
    { ...base, id: 3, item_uuid: null, item_type: null, method: 'turn/completed',
      params: JSON.stringify({ id: 'native-turn', status: 'completed', error: null }) },
  ] });
  fireEvent.click((await screen.findAllByRole('button', { name: /^Conversation iFood progress/ }))[0]);
  await screen.findByText('Original ETL output');
  expect(screen.getByRole('combobox', { name: 'Message' }).getAttribute('contenteditable')).toBe('true');
  expect(screen.queryByRole('button', { name: 'Edit message' })).toBeNull();
  expect(screen.getByRole('button', { name: 'Edit message (not available now)' }).getAttribute('aria-disabled')).toBe('true');
  expect(screen.getByRole('button', { name: 'Regenerate response' })).toBeTruthy();
  fireEvent.click(screen.getByRole('button', { name: 'Regenerate response' }));
  await waitFor(() => expect(requests.filter(request => request.path.endsWith('/planner/input'))).toHaveLength(1));
  expect(requests.find(request => request.path.endsWith('/planner/input'))?.body).toEqual({ text: 'Check ETL progress' });
});
