pub mod admin;
pub mod middleware;
pub mod mock_roles;

// Re-export shared auth types and client from authotron crates.
// These replace the local types.rs, jwt.rs, and client.rs modules.
pub use admin::is_admin_bypass_user;
pub use authotron_client::{AuthClient, convert_email_key};
pub use authotron_types::{AuthError, User as AuthUser};
pub use mock_roles::MockRolesAudit;
