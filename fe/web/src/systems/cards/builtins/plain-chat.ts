import type { CardEntry, KernelCardInput } from '../registry.js';

declare module '../registry.js' {
  interface CardDataMap {
    plainChat: PlainChatCard;
  }
}

export type PlainChatCard = Readonly<{ type: 'plain-chat'; id: string }>;

/** Shared by area chats and attached native sessions; neither owns a terminal surface. */
export function isPlainChatPayload(payload: unknown): boolean {
  return typeof payload === 'object' && payload !== null
    && (payload as { harness_profile?: unknown }).harness_profile === 'plain_chat';
}

/** Conversation presentation belongs to the conversation drawer, rather than the card board. */
export const PLAIN_CHAT_CARD_ENTRY = Object.freeze({
  type: 'plain-chat',
  component: () => null,
  headless: true,
  defaultSize: Object.freeze({ w: 1, h: 1, minW: 1, minH: 1 }),
  title: () => 'Conversation',
  accessibleName: () => 'Plain chat conversation',
  create: Object.freeze({ mode: 'kernel-minted-only' } as const),
  fromKernel: (card: KernelCardInput): PlainChatCard | null => (
    card.kind === 'codex' && isPlainChatPayload(card.payload)
      ? Object.freeze({ type: 'plain-chat', id: card.id } as const)
      : null
  ),
}) satisfies CardEntry<PlainChatCard>;
