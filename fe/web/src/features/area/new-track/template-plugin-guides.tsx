import { Badge } from '@astryxdesign/core/Badge';
import { Button } from '@astryxdesign/core/Button';
import type { TemplatePluginGuide } from '../../../../../core/domain/template.ts';
import styles from './template-plugin-guides.module.css';

export function TemplatePluginGuides({ guides, error, onRetry }: Readonly<{
  guides: readonly TemplatePluginGuide[] | undefined;
  error: boolean;
  onRetry: () => void;
}>) {
  if (error) return <span role="alert">Could not load template guides. <Button label="Retry" variant="ghost" size="sm" onClick={onRetry} /></span>;
  if (guides === undefined || guides.length === 0) return null;
  return <div className={styles.guides} role="group" aria-label="Template plugin guides">
    {guides.map(guide => <Badge key={guide.id} variant="neutral" label={guide.name}
      aria-label={`${guide.name}, included by template`}
      icon={<svg className={styles.lock} viewBox="0 0 16 16" aria-hidden="true">
        <rect x="3" y="7" width="10" height="7" rx="2" />
        <path d="M5 7V5a3 3 0 0 1 6 0v2" />
      </svg>} />)}
  </div>;
}
