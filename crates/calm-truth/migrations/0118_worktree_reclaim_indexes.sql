-- #1815 — the released-worktree reclaim (scheduler reconcile sweep, boot and every tick) asks,
-- per released lease: the card's latest `worktree.removed` / `worktree.provisioned` event ids,
-- the lease's latest delivery row, and the tasks its card worked. Without these, each lookup
-- scanned every event of a kind, every delivery or every task: quadratic in the lease count.
CREATE INDEX idx_events_kind_scope_card ON events(kind, scope_card, id);
CREATE INDEX idx_task_git_deliveries_lease_ordinal ON task_git_deliveries(lease_id, ordinal);
CREATE INDEX idx_tasks_worker_card_id ON tasks(worker_card_id) WHERE worker_card_id IS NOT NULL;
