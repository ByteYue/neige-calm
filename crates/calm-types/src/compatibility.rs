//! Shared REST contract/capability revision for the kernel and upgrade checks.

// Revision 14 (#1829): `POST /api/tracks/{id}/activity/dismissals` is new, and the transcript wire
// gains a required `turn_error_text`; a bundle of this revision would get 404 for Dismiss and reject
// an older kernel's transcript rows, so preflight refuses the pairing.
pub const REST_API_VERSION: &str = "14";
