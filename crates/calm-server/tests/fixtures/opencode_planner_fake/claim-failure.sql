-- Private test database only: fail the send claim before any native POST.
CREATE TRIGGER fixture_stop_claim
BEFORE UPDATE OF state ON opencode_submissions
WHEN NEW.state = 'sending'
BEGIN
    SELECT RAISE(ABORT, 'fixture claim failure');
END;
