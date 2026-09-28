//! Shared REST contract/capability revision for the kernel and upgrade checks.

// Revision 14 (#1829): `POST /api/tracks/{id}/activity/dismissals` is new; a bundle that offers
// Dismiss would get 404 from an older kernel, so preflight refuses the pairing.
pub const REST_API_VERSION: &str = "14";
