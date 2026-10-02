import { useId } from 'react';
import { Banner } from '@astryxdesign/core/Banner';
import { Button } from '@astryxdesign/core/Button';
import { DropdownMenu, DropdownMenuItem } from '@astryxdesign/core/DropdownMenu';
import { HStack } from '@astryxdesign/core/HStack';
import { TextInput } from '@astryxdesign/core/TextInput';
import { VStack } from '@astryxdesign/core/VStack';
import type { OpenCodeConnection } from '../../../../../core/domain/opencode-connections.ts';
import { Dialog } from '../../../ui/dialog/public.tsx';
import styles from './connect.module.css';

export type ConnectOpenCodeDialogProps = Readonly<{
  open: boolean; connections: readonly OpenCodeConnection[] | undefined;
  connectionId: string; sessionId: string; locked: boolean; busy: boolean;
  error: string | null; loadError: string | null;
  onConnectionChange: (id: string) => void; onSessionChange: (id: string) => void;
  onClose: () => void; onSubmit: () => void; onReload: () => void;
}>;

export function ConnectOpenCodeDialog(props: ConnectOpenCodeDialogProps) {
  const helpId = `${useId()}-help`;
  const selected = props.connections?.find(connection => connection.id === props.connectionId);
  return <Dialog open={props.open} onClose={props.onClose} title="Connect OpenCode session">
    <VStack as="form" gap={2} className={styles.form} onSubmit={event => {
      event.preventDefault(); if (!props.busy) props.onSubmit();
    }}>
      <p id={helpId}>Open the original history and observe progress. Connecting sends no message.</p>
      {props.loadError !== null && <><Banner status="error" title={props.loadError} />
        <Button label="Reload connections" variant="ghost" onClick={props.onReload} /></>}
      {props.connections === undefined && props.loadError === null && <p>Loading connections…</p>}
      {props.connections?.length === 0 && <p>No OpenCode connections are registered. Ask the server operator to configure one.</p>}
      <DropdownMenu button={{ label: `Connection: ${selected?.label ?? 'Choose connection'}`,
        variant: 'secondary', isDisabled: props.locked || props.connections === undefined || props.connections.length === 0 }}>
        {(props.connections ?? []).map(connection => <DropdownMenuItem key={connection.id}
          label={connection.label} onClick={() => props.onConnectionChange(connection.id)} />)}
      </DropdownMenu>
      {selected !== undefined && <p className={styles.directory}>Working directory: {selected.directory}</p>}
      <TextInput label="OpenCode session ID" value={props.sessionId} onChange={props.onSessionChange}
        isDisabled={props.locked} width="100%" placeholder="ses_…" isRequired aria-describedby={helpId} />
      {props.error !== null && <Banner status="error" title={props.error} />}
      {props.locked && !props.busy && <p>The original connection request is retained. Retry checks the same request.</p>}
      <HStack gap={1} justify="end">
        <Button label="Cancel" type="button" variant="ghost" onClick={props.onClose} />
        <Button label={props.locked ? 'Retry connection' : 'Connect session'} type="submit" variant="primary"
          isLoading={props.busy} isDisabled={props.busy || (!props.locked && (selected === undefined || props.sessionId.trim() === ''))} />
      </HStack>
    </VStack>
  </Dialog>;
}
