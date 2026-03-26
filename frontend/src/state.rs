use crate::auth::AuthClient;

pub struct AppState {
    pub bits: bits::Bits,
    pub auth_client: Option<AuthClient>,
    pub allow_anonymous: bool,
}
