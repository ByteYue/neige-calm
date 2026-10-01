-- Private test database only: preserve production-prepared data as a crash image.
CREATE TRIGGER fixture_stop_claim
BEFORE UPDATE OF state ON opencode_submissions
WHEN NEW.state = 'sending'
BEGIN
    SELECT RAISE(ABORT, 'fixture claim failure');
END;

CREATE TRIGGER fixture_keep_projection
BEFORE DELETE ON harness_items
WHEN OLD.turn_id IS NULL AND OLD.method = 'item/completed'
BEGIN
    SELECT RAISE(ABORT, 'fixture retain crash projection');
END;
