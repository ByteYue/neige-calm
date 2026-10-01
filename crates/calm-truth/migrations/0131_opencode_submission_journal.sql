-- Immutable native send identity survives worker/card retirement and kernel boots.
-- These identifiers deliberately have no cascading foreign keys: deleting a card
-- must not erase evidence of a request whose acceptance or outcome is unknown.
CREATE TABLE opencode_submissions (
    id TEXT PRIMARY KEY NOT NULL CHECK (length(id) > 0),
    worker_session_id TEXT NOT NULL CHECK (length(worker_session_id) > 0),
    card_id TEXT NOT NULL CHECK (length(card_id) > 0),
    scope_id TEXT NOT NULL CHECK (length(scope_id) > 0),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    thread_id TEXT NOT NULL CHECK (length(thread_id) > 0),
    native_session_id TEXT NOT NULL CHECK (length(native_session_id) > 0),
    client_id TEXT NOT NULL CHECK (length(client_id) > 0),
    native_message_id TEXT NOT NULL CHECK (length(native_message_id) > 0),
    input_json TEXT NOT NULL CHECK (json_valid(input_json)),
    input_fingerprint TEXT NOT NULL CHECK (length(input_fingerprint) = 64),
    state TEXT NOT NULL CHECK (state IN ('prepared','sending','unknown','completed','failed','interrupted')),
    outcome_json TEXT CHECK (outcome_json IS NULL OR json_valid(outcome_json)),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    CHECK ((state IN ('prepared','sending','unknown') AND outcome_json IS NULL)
        OR (state IN ('completed','failed','interrupted') AND outcome_json IS NOT NULL)),
    UNIQUE (scope_id, native_session_id, client_id),
    UNIQUE (scope_id, native_session_id, native_message_id)
);
CREATE UNIQUE INDEX opencode_unresolved_card_idx ON opencode_submissions(card_id)
    WHERE state IN ('prepared','sending','unknown');
CREATE UNIQUE INDEX opencode_unresolved_native_idx ON opencode_submissions(scope_id, native_session_id)
    WHERE state IN ('prepared','sending','unknown');
CREATE INDEX opencode_worker_submission_idx ON opencode_submissions(worker_session_id, created_at_ms, id);

CREATE TRIGGER opencode_submission_identity_immutable BEFORE UPDATE ON opencode_submissions
WHEN NEW.id != OLD.id OR NEW.worker_session_id != OLD.worker_session_id
    OR NEW.card_id != OLD.card_id OR NEW.scope_id != OLD.scope_id
    OR NEW.generation != OLD.generation OR NEW.thread_id != OLD.thread_id
    OR NEW.native_session_id != OLD.native_session_id OR NEW.client_id != OLD.client_id
    OR NEW.native_message_id != OLD.native_message_id OR NEW.input_json != OLD.input_json
    OR NEW.input_fingerprint != OLD.input_fingerprint OR NEW.created_at_ms != OLD.created_at_ms
BEGIN SELECT RAISE(ABORT, 'OpenCode submission identity is immutable'); END;
CREATE TRIGGER opencode_submission_transition_guard BEFORE UPDATE OF state ON opencode_submissions
WHEN NOT (NEW.state = OLD.state
    OR (OLD.state = 'prepared' AND NEW.state IN ('sending','failed','interrupted'))
    OR (OLD.state = 'sending' AND NEW.state IN ('unknown','completed','failed','interrupted'))
    OR (OLD.state = 'unknown' AND NEW.state IN ('completed','failed','interrupted')))
BEGIN SELECT RAISE(ABORT, 'OpenCode submission transition denied'); END;
CREATE TRIGGER opencode_submission_terminal_immutable BEFORE UPDATE ON opencode_submissions
WHEN OLD.state IN ('completed','failed','interrupted')
BEGIN SELECT RAISE(ABORT, 'OpenCode submission outcome is immutable'); END;
CREATE TRIGGER opencode_submission_no_delete BEFORE DELETE ON opencode_submissions
BEGIN SELECT RAISE(ABORT, 'OpenCode submission evidence is permanent'); END;
