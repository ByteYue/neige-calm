import { useSyncExternalStore } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import type { ApiTransportPort } from '../../../../core/api/types.ts';
import type { UnauthorizedChannel } from '../../../../core/api/unauthorized.ts';
import { connectOpenCodeSessionOperation, openCodeConnectionsOperation } from '../../../../core/domain/opencode-connections.ts';
import type { Conversation } from '../../../../core/domain/conversation.ts';
import { useState } from '../../ui/state/public.ts';
import { useConversationRegistry } from '../conversations/public.tsx';
import { admitTransport } from './recovery-mutation.ts';
import { ApiError, OfflineSubmissionError, queryKeys, runOperation } from './queries.ts';

/** App composition: typed operations, request lease, query reconciliation and conversation selection. */
export function useOpenCodeConnection(transport: ApiTransportPort, unauthorized: UnauthorizedChannel, trackId: string) {
  const registry = useConversationRegistry();
  const store = registry.connectionDrafts;
  const draft = useSyncExternalStore(store.subscribe, () => store.get(trackId));
  const [open, setOpen] = useState(false);
  const query = useQuery({ queryKey: ['opencode-connections'],
    queryFn: () => runOperation(transport, openCodeConnectionsOperation(), unauthorized), enabled: open });
  const client = useQueryClient();
  const accept = (row: Conversation) => {
    client.setQueryData<Conversation[]>(queryKeys.trackConversations(trackId), current =>
      [...(current ?? []).filter(candidate => candidate.id !== row.id), row]);
    void client.invalidateQueries({ queryKey: queryKeys.trackConversations(trackId) }).catch(() => undefined);
    registry.requestOpen(row.id);
  };
  const submit = () => {
    const current = store.get(trackId);
    if (current === null || current.busy) return;
    if (current.connected !== null) { accept(current.connected); store.forget(trackId); setOpen(false); return; }
    const request = current.request ?? { connection_id: current.connectionId, session_id: current.sessionId.trim() };
    if (request.connection_id === '' || request.session_id === '') return;
    let admitted: ApiTransportPort;
    try { admitted = admitTransport(transport); }
    catch (error) {
      store.update(trackId, { error: error instanceof Error ? error.message : 'Reconnect before connecting this session.' }); return;
    }
    store.update(trackId, { busy: true, request, error: null });
    void runOperation(admitted, connectOpenCodeSessionOperation(trackId, request, current.key), unauthorized)
      .then(row => {
        store.update(trackId, { connected: row });
        accept(row); setOpen(false);
      }).catch((error: unknown) => {
        const refused = current.request === null && (error instanceof OfflineSubmissionError
          || (error instanceof ApiError && error.failure.kind === 'http'
            && error.failure.status >= 400 && error.failure.status < 500
            && error.failure.status !== 408 && error.failure.status !== 409));
        store.update(trackId, { error: error instanceof Error ? error.message : 'Could not confirm the connection.',
          ...(refused ? { request: null } : {}) });
      }).finally(() => { store.update(trackId, { busy: false }); });
  };
  return {
    show: () => {
      if (draft?.connected !== null && draft?.connected !== undefined) store.forget(trackId);
      else if (draft === null) store.update(trackId, {});
      setOpen(true);
    },
    dialog: {
      open, connections: query.data?.connections, connectionId: draft?.connectionId ?? '',
      sessionId: draft?.sessionId ?? '', locked: draft?.request !== null && draft?.request !== undefined,
      busy: draft?.busy ?? false, error: draft?.error ?? null,
      loadError: query.error instanceof Error ? query.error.message : null,
      onConnectionChange: (connectionId: string) => { if (draft?.request == null) store.update(trackId, { connectionId }); },
      onSessionChange: (sessionId: string) => { if (draft?.request == null) store.update(trackId, { sessionId }); },
      onClose: () => setOpen(false), onSubmit: submit,
      onReload: () => { void query.refetch().catch(() => undefined); },
    },
  };
}
