// @vitest-environment jsdom
import { useSyncExternalStore } from 'react';
import { useState } from '../../ui/state/public.ts';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, expect, it } from 'vitest';
import type { ApiRequest, ApiTransportPort, ApiTransportResponse } from '../../../../core/api/types.ts';
import { createUnauthorizedChannel } from '../../../../core/api/unauthorized.ts';
import { ConversationProvider, useConversationRegistry } from '../conversations/public.tsx';
import { ConnectOpenCodeDialog } from '../../features/track/opencode-session/connect.tsx';
import { ThemeProvider } from '../theme/public.tsx';
import { useOpenCodeConnection } from './opencode-connections.tsx';
import { queryKeys } from './queries.ts';

afterEach(cleanup);

it('retains a late successful connection for Track A without replacing Track B open intent', async () => {
  let acknowledge!: (response: ApiTransportResponse) => void;
  const requests: ApiRequest[] = [];
  const row = { id: 'attached-a', trackId: 'a', title: 'ETL', kind: 'track-opencode', state: 'idle', updatedAt: 1, lastTurnCompletedAt: null };
  const transport: ApiTransportPort = { send: request => {
    requests.push(request);
    if (request.method === 'POST') return new Promise(resolve => { acknowledge = resolve; });
    return Promise.resolve({ status: 200, statusText: 'OK', body: { connections: [{ id: 'ops', label: 'Operations', directory: '/srv/etl' }] } });
  } };
  const unauthorized = createUnauthorizedChannel({ enqueue: task => task() });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  function Controls({ trackId }: { trackId: string }) {
    const connection = useOpenCodeConnection(transport, unauthorized, trackId);
    return <><button onClick={connection.show}>Connect</button><ConnectOpenCodeDialog {...connection.dialog} /></>;
  }
  function TrackScopes() {
    const [trackId, setTrackId] = useState('a');
    const registry = useConversationRegistry();
    const draft = useSyncExternalStore(registry.connectionDrafts.subscribe, () => registry.connectionDrafts.get('a'));
    return <><Controls key={trackId} trackId={trackId} />
      <button onClick={() => { setTrackId('b'); registry.requestOpen('existing-b'); }}>Go to Track B</button>
      <output aria-label="Open intent">{registry.requestedOpenId}</output>
      <output aria-label="Retained connection">{draft?.connected?.id ?? ''}</output></>;
  }
  render(<QueryClientProvider client={client}><ThemeProvider><ConversationProvider><TrackScopes /></ConversationProvider></ThemeProvider></QueryClientProvider>);
  fireEvent.click(screen.getByRole('button', { name: 'Connect' }));
  const dialog = await screen.findByRole('dialog', { name: 'Connect OpenCode session' });
  fireEvent.click(await within(dialog).findByRole('button', { name: 'Connection: Choose connection' }));
  fireEvent.click(await screen.findByRole('menuitem', { name: 'Operations' }));
  fireEvent.change(within(dialog).getByRole('textbox', { name: /^OpenCode session ID/ }), { target: { value: 'ses_a' } });
  fireEvent.click(within(dialog).getByRole('button', { name: 'Connect session' }));
  await waitFor(() => expect(requests.filter(request => request.method === 'POST')).toHaveLength(1));
  fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }));
  fireEvent.click(screen.getByRole('button', { name: 'Go to Track B' }));
  act(() => { acknowledge({ status: 201, statusText: 'Created', body: row }); });
  await waitFor(() => expect(screen.getByLabelText('Retained connection').textContent).toBe(row.id));
  expect(screen.getByLabelText('Open intent').textContent).toBe('existing-b');
  expect(client.getQueryData(queryKeys.trackConversations('a'))).toEqual([expect.objectContaining({ id: row.id })]);
  expect(requests.filter(request => request.method === 'POST')).toHaveLength(1);
});
