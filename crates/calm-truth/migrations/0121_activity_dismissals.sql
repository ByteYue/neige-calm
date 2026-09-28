-- 0121_activity_dismissals.sql
--
-- #1829 S3 — Dismiss: the user set one notification item of the `kernel/track/activity` overlay
-- aside. `item_key` is the item's key (`ask:lifecycle:<id>`, `ask:notify:<id>`,
-- `planner_down:<id>`); it carries the evidence row's id, so the same source happening again is a
-- new key and lights up again. The projector drops the listed keys of its track. Rows are only
-- inserted (the first dismissal's time is kept) and leave with their track.
CREATE TABLE activity_dismissals (
    track_id        TEXT    NOT NULL REFERENCES tracks(id) ON DELETE CASCADE,
    item_key        TEXT    NOT NULL,
    dismissed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (track_id, item_key)
) WITHOUT ROWID;
