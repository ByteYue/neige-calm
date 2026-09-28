import { describe, expect, it } from 'vitest';

import {
  activityLabelOf, activityNameBit, activityStateOf, attentionOfCard, cardActivityOf, cardActivityState,
  notificationPlainText, type ActivityState, type AttentionKind, type CardActivity,
} from './activity.js';

const ATTENTION: readonly AttentionKind[] = ['none', 'input', 'failed'];

describe('activityStateOf', () => {
  // The whole input domain, 2 × 3 × 2 = 12 rows, written out by hand.
  const table: readonly [boolean, AttentionKind, boolean, ActivityState][] = [
    [false, 'none', false, 'quiet'],
    [false, 'none', true, 'unread'],
    [true, 'none', false, 'working'],
    [true, 'none', true, 'working'],
    [false, 'input', false, 'attention'],
    [false, 'input', true, 'attention'],
    [true, 'input', false, 'attention'],
    [true, 'input', true, 'attention'],
    [false, 'failed', false, 'failed'],
    [false, 'failed', true, 'failed'],
    [true, 'failed', false, 'failed'],
    [true, 'failed', true, 'failed'],
  ];

  it('covers every combination of the input domain', () => {
    const seen = new Set(table.map(([working, attention, unread]) => `${working}/${attention}/${unread}`));
    expect(seen.size).toBe(2 * ATTENTION.length * 2);
  });

  it.each(table)('working=%s attention=%s unread=%s → %s', (working, attention, unread, expected) => {
    expect(activityStateOf({ working, attention, unread })).toBe(expected);
  });

  it('ranks a person-needed state above motion when both hold at once', () => {
    expect(activityStateOf({ working: true, attention: 'failed', unread: true })).toBe('failed');
    expect(activityStateOf({ working: true, attention: 'input', unread: true })).toBe('attention');
  });
});

describe('activityLabelOf', () => {
  it.each<[ActivityState, string | null]>([
    ['working', 'Working'],
    ['attention', 'Needs input'],
    ['failed', 'Needs attention'],
    ['unread', 'Unread updates'],
    ['quiet', null],
  ])('%s → %s', (state, expected) => {
    expect(activityLabelOf(state)).toBe(expected);
  });
});

describe('activityNameBit', () => {
  // `unread` is deliberately silent here — it is the rail row's description, not part of a name.
  it.each<[ActivityState, string]>([
    ['working', 'working'],
    ['attention', 'waiting on you'],
    ['failed', 'needs attention'],
    ['unread', ''],
    ['quiet', ''],
  ])('%s → %s', (state, expected) => {
    expect(activityNameBit(state)).toBe(expected);
  });
});

describe('cardActivityOf', () => {
  it('returns the kernel verdict for a listed card and null for an unlisted one', () => {
    const activity = { cards: { a: 'working' as const, b: 'failed' as const } };
    expect(cardActivityOf(activity, 'a')).toBe('working');
    expect(cardActivityOf(activity, 'b')).toBe('failed');
    expect(cardActivityOf(activity, 'c')).toBeNull();
    expect(cardActivityOf({ cards: {} }, 'a')).toBeNull();
  });
});

describe('card verdicts as indicator states', () => {
  it('maps each per-card verdict onto the attention axis, and nothing else', () => {
    expect(attentionOfCard(null)).toBe('none');
    expect(attentionOfCard('working')).toBe('none');
    expect(attentionOfCard('input')).toBe('input');
    expect(attentionOfCard('failed')).toBe('failed');
  });

  it.each<[CardActivity, ActivityState]>([
    ['working', 'working'], ['input', 'attention'], ['failed', 'failed'],
  ])('shows a %s card as %s, never as unread', (card, expected) => {
    expect(cardActivityState(card)).toBe(expected);
  });
});

describe('notificationPlainText', () => {
  it('keeps the visible words of markdown and drops its syntax', () => {
    expect(notificationPlainText('Before I dispatch I need **one decision**:\n\n- keep `legacy_orders`\n'
      + '- drop it\n\nContext: [PR #1811](https://example.com/pr/1811).'))
      .toBe('Before I dispatch I need one decision: keep legacy_orders drop it Context: PR #1811.');
  });

  it('leaves plain text as it is', () => {
    expect(notificationPlainText("400: The 'gpt-6-astra' model requires a newer version of Codex."))
      .toBe("400: The 'gpt-6-astra' model requires a newer version of Codex.");
  });
});
