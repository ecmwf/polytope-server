use std::collections::HashMap;

use crate::auth::AuthClient;
use crate::config::SupportConfig;

pub struct AppState {
    pub bits: bits::Bits,
    pub auth_client: Option<AuthClient>,
    pub collections: HashMap<String, bits::RouteHandle>,
    pub allow_anonymous: bool,
    pub admin_bypass_roles: Option<HashMap<String, Vec<String>>>,
    pub support: SupportConfig,
}
