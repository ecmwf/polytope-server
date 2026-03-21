use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum PolytopeError {
    ServerError(String),
    BadRequest(String),
    Unauthorized(String),
    Forbidden(String),
    NotFound(String),
    Unexpected(String),
}

impl fmt::Display for PolytopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PolytopeError::ServerError(msg) => write!(f, "ServerError: {}", msg),
            PolytopeError::BadRequest(msg) => write!(f, "BadRequest: {}", msg),
            PolytopeError::Unauthorized(msg) => write!(f, "Unauthorized: {}", msg),
            PolytopeError::Forbidden(msg) => write!(f, "Forbidden: {}", msg),
            PolytopeError::NotFound(msg) => write!(f, "NotFound: {}", msg),
            PolytopeError::Unexpected(msg) => write!(f, "Unexpected: {}", msg),
        }
    }
}

impl Error for PolytopeError {}
