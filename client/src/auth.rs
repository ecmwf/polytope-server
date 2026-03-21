use base64::{prelude::BASE64_STANDARD, Engine};
use serde_json::Value;
use std::{error::Error, fs};
use tracing::info;

#[derive(Debug)]
pub enum AuthHeader {
    Bearer(String),
    Basic(String, String),
    EmailKey(String, String),
}

impl AuthHeader {
    pub fn from_file(file_path: &str) -> Result<Self, Box<dyn Error>> {
        let content = fs::read_to_string(file_path)
            .map_err(|e| format!("Failed to read authentication file '{}': {}", file_path, e))?;
        let config: Value = serde_json::from_str(&content)
            .map_err(|e| format!("Failed to parse authentication file '{}': {}", file_path, e))?;

        if let (Some(email), Some(key)) = (
            config.get("email").and_then(|v| v.as_str()),
            config.get("key").and_then(|v| v.as_str()),
        ) {
            return Ok(AuthHeader::EmailKey(email.to_string(), key.to_string()));
        }

        if let (Some(user_email), Some(user_key)) = (
            config.get("user_email").and_then(|v| v.as_str()),
            config.get("user_key").and_then(|v| v.as_str()),
        ) {
            return Ok(AuthHeader::EmailKey(
                user_email.to_string(),
                user_key.to_string(),
            ));
        }

        if let Some(key) = config
            .get("key")
            .or_else(|| config.get("user_key"))
            .and_then(|v| v.as_str())
        {
            return Ok(AuthHeader::Bearer(key.to_string()));
        }

        Err("No valid authentication configuration found in the file".into())
    }

    pub fn from_default_files() -> Result<Self, Box<dyn Error>> {
        let home_dir = dirs::home_dir().ok_or("Unable to determine home directory")?;
        let polytope_file = home_dir.join(".polytopeapirc");
        let ecmwf_file = home_dir.join(".ecmwfapirc");

        if polytope_file.exists() {
            info!("Using authentication from: {}", polytope_file.display());
            Self::from_file(polytope_file.to_str().unwrap())
        } else if ecmwf_file.exists() {
            info!("Using authentication from: {}", ecmwf_file.display());
            Self::from_file(ecmwf_file.to_str().unwrap())
        } else {
            Err("No valid authentication configuration found in default files.".into())
        }
    }
}

impl Into<String> for AuthHeader {
    fn into(self) -> String {
        match self {
            AuthHeader::Bearer(api_key) => format!("Bearer {}", api_key),
            AuthHeader::Basic(user_name, user_password) => {
                let credentials = format!("{}:{}", user_name, user_password);
                format!("Basic {}", BASE64_STANDARD.encode(credentials))
            }
            AuthHeader::EmailKey(user_email, user_key) => {
                format!("EmailKey {}:{}", user_email, user_key)
            }
        }
    }
}
