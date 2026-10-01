import { describe, expect, it } from 'vitest';

import { agentProviderSchema } from '../api/schemas.js';
import {
  agentProvidersSchema, availabilityOf, CREATE_REFUSED_WHEN_UNAVAILABLE,
  PLANNER_PROVIDERS, PLANNER_PROVIDER_LABELS,
} from './agent-providers.js';
import { modelCatalogSchema } from './conversation.js';

describe('OpenCode Planner contracts', () => {
  it('decodes the third provider and keeps the creation roster and labels total', () => {
    expect(agentProviderSchema.parse('opencode')).toBe('opencode');
    expect(PLANNER_PROVIDERS).toEqual(['codex', 'claude', 'opencode']);
    expect(PLANNER_PROVIDERS).toEqual(agentProviderSchema.options);
    expect(PLANNER_PROVIDERS.map((provider) => PLANNER_PROVIDER_LABELS[provider]))
      .toEqual(['Codex', 'Claude', 'OpenCode']);
    expect(Object.isFrozen(PLANNER_PROVIDERS)).toBe(true);
    expect(CREATE_REFUSED_WHEN_UNAVAILABLE.opencode).toBe(true);
    expect(agentProviderSchema.safeParse('unknown').success).toBe(false);
  });

  it('retains the OpenCode availability reason without treating it as Codex', () => {
    const providers = agentProvidersSchema.parse([
      { provider: 'opencode', status: 'unavailable', reason: 'OpenCode credentials are missing', checked_at_ms: 1 },
    ]);
    expect(availabilityOf(providers, 'opencode')?.reason).toBe('OpenCode credentials are missing');
    expect(availabilityOf(providers, 'codex')).toBeNull();
  });

  it('decodes the configured model slug and native variant keys unchanged', () => {
    const result = modelCatalogSchema.parse({
      models: [], default: { model: 'openai/gpt-5', reasoning_effort: 'deep',
        supported_reasoning_efforts: [{ reasoning_effort: 'deep', description: null }] },
      default_source: 'opencode_config', source: 'live', fetched_at_ms: 1,
    });
    expect(result.default).toEqual({ model: 'openai/gpt-5', reasoning_effort: 'deep',
      supported_reasoning_efforts: [{ reasoning_effort: 'deep', description: null }] });
  });
});
