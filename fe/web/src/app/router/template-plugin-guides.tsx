import { useQuery } from '@tanstack/react-query';
import type { ApiTransportPort } from '../../../../core/api/types.ts';
import type { UnauthorizedChannel } from '../../../../core/api/unauthorized.ts';
import { templatePluginGuidesOperation } from '../../../../core/domain/template.ts';
import { TemplatePluginGuides as GuidePills } from '../../features/area/new-track/template-plugin-guides.tsx';
import { runOperation } from '../providers/queries.ts';

export function TemplatePluginGuides({ templateId, transport, unauthorized }: Readonly<{
  templateId: string; transport: ApiTransportPort; unauthorized: UnauthorizedChannel;
}>) {
  const guides = useQuery({ queryKey: ['template-plugin-guides', templateId],
    queryFn: ({ signal }) => runOperation(transport, { ...templatePluginGuidesOperation(templateId), signal }, unauthorized) });
  return <GuidePills guides={guides.data} error={guides.isError} onRetry={() => { void guides.refetch(); }} />;
}
