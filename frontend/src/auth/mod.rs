pub mod middleware;

// Re-export shared auth types and client from authotron crates.
// These replace the local types.rs, jwt.rs, and client.rs modules.
pub use authotron_client::{AuthClient, convert_email_key};
pub use authotron_types::{AuthError, User as AuthUser};
