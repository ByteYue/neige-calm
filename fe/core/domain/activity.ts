// Activity: the one vocabulary every indicator speaks. The kernel's `kernel/track/activity`
// overlay is the only source of "in motion / waiting on a person / broken" for a track; nothing else gets folded in.

/** What an indicator can show. `quiet` renders nothing. */
export type ActivityState = 'failed' | 'attention' | 'working' | 'unread' | 'quiet';

/** The kernel's verdict on whether a person has to act: nothing, give input, or repair. */
export type AttentionKind = 'none' | 'input' | 'failed';

/** The per-card verdict the overlay's `cards[]` carries; a card without one has no indicator. */
export type CardActivity = 'working' | 'input' | 'failed';

/** What a notification is: the Planner asks the user something, or the Planner stopped. */
export type NotificationSource = 'ask' | 'planner_down';

/** One thing addressed to the user and not yet handled, as the kernel listed it (`items[]`). */
export type ActivityItem = Readonly<{
  source: NotificationSource;
  /** The kernel's identity for it; the same source happening again is a new key. */
  key: string;
  /** The kernel's words: the Planner's question, or the reason it stopped. */
  text: string;
  atMs: number;
}>;

/** The precedence, stated once: `failed > attention > working > unread > quiet`. */
export function activityStateOf(
  s: Readonly<{ working: boolean; attention: AttentionKind; unread: boolean }>,
): ActivityState {
  if (s.attention === 'failed') return 'failed';
  if (s.attention === 'input') return 'attention';
  if (s.working) return 'working';
  if (s.unread) return 'unread';
  return 'quiet';
}

/** The spoken counterpart of an indicator state — the ONE vocabulary every accessible label of an indicator draws from. `quiet` has nothing to say. */
export function activityLabelOf(state: ActivityState): string | null {
  switch (state) {
    case 'working': return 'Working';
    case 'attention': return 'Needs input';
    case 'failed': return 'Needs attention';
    case 'unread': return 'Unread updates';
    case 'quiet': return null;
  }
}

/** The activity bit a track row's accessible *name* carries; `unread` is never part of a name. Empty string, not `null`: the value is concatenated, never rendered alone. */
export function activityNameBit(state: ActivityState): string {
  switch (state) {
    case 'working': return 'working';
    case 'attention': return 'waiting on you';
    case 'failed': return 'needs attention';
    case 'unread':
    case 'quiet': return '';
  }
}

/** The one read of a track's per-card verdicts; `null` is "the kernel said nothing about this card". */
export function cardActivityOf(
  activity: Readonly<{ cards: Readonly<Record<string, CardActivity>> }>,
  cardId: string,
): CardActivity | null {
  return activity.cards[cardId] ?? null;
}

/** A card verdict as the attention axis reads it; `null` (no verdict) and `working` are `none`. */
export function attentionOfCard(card: CardActivity | null): AttentionKind {
  return card === 'input' ? 'input' : card === 'failed' ? 'failed' : 'none';
}

/** A card verdict as an indicator shows it. Cards have no read receipt, so `unread` is never part of a card-level state. */
export function cardActivityState(card: CardActivity): ActivityState {
  return activityStateOf({ working: card === 'working', attention: attentionOfCard(card), unread: false });
}
