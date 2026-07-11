// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

pub mod admin;
pub mod middleware;
pub mod mock_roles;
pub mod mock_time;

// Re-export shared auth types and client from authotron crates.
// These replace the local types.rs, jwt.rs, and client.rs modules.
pub use admin::is_admin_bypass_user;
pub use authotron_client::{AuthClient, JwtPublicKey, convert_email_key};
pub use authotron_types::{AuthError, User as AuthUser};
pub use mock_roles::MockRolesAudit;
pub use mock_time::{MockTime, MockTimeAudit};
