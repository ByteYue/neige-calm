import { z } from 'zod';
import type { ApiOperation } from '../api/types.js';

/** Author-supplied display metadata is optional; the actual template body is required. */
export const templateDetailSchema = z.object({
  id: z.string(),
  title: z.string(),
  description: z.string().nullable(),
  instructions: z.string().nullable(),
  body: z.string(),
});
export type TemplateDetail = z.infer<typeof templateDetailSchema>;
export type LoadTemplate = (id: string) => Promise<TemplateDetail>;

export function templateDetailOperation(id: string): ApiOperation<TemplateDetail> {
  return { method: 'GET', path: `/api/track-templates/${encodeURIComponent(id)}`, responseSchema: templateDetailSchema };
}

export const templatePluginGuideSchema = z.object({ id: z.string(), name: z.string() });
export type TemplatePluginGuide = z.infer<typeof templatePluginGuideSchema>;
export function templatePluginGuidesOperation(templateId: string): ApiOperation<TemplatePluginGuide[]> {
  return { method: 'GET', path: `/api/track-templates/${encodeURIComponent(templateId)}/plugin-guides`,
    responseSchema: z.array(templatePluginGuideSchema) };
}
