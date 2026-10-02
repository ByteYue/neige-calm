import { z } from 'zod';
import type { ApiOperation } from '../api/types.js';
import { toTrackConversation, trackConversationSummarySchema, type Conversation } from './conversation.js';

const connectionSchema = z.object({ id: z.string(), label: z.string(), directory: z.string() });
export type OpenCodeConnection = z.infer<typeof connectionSchema>;
export type OpenCodeConnectRequest = Readonly<{ connection_id: string; session_id: string }>;

/** Public configuration metadata only: addresses, credentials and profiles stay on the server. */
export function openCodeConnectionsOperation(): ApiOperation<{ connections: OpenCodeConnection[] }> {
  return { method: 'GET', path: '/api/opencode/connections',
    responseSchema: z.object({ connections: z.array(connectionSchema) }) };
}

/** A connection request has no prompt; repeated acknowledgements open the same conversation. */
export function connectOpenCodeSessionOperation(
  trackId: string, body: OpenCodeConnectRequest, idempotencyKey: string,
): ApiOperation<Conversation> {
  return { method: 'POST', path: `/api/tracks/${encodeURIComponent(trackId)}/opencode-conversations`,
    headers: { 'Idempotency-Key': idempotencyKey }, body,
    responseSchema: trackConversationSummarySchema.transform(toTrackConversation) };
}
