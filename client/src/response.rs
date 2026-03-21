use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

#[derive(Debug, Deserialize, Serialize)]
pub enum RequestDetailsResponse {
    Post(PostResponse),
    Redirect(RedirectResponse),
}
#[derive(Debug, Deserialize, Serialize)]
pub enum PolytopeApiResponse {
    Collections(CollectionsResponse),
    Error(MessageResponse),
    Deletion(MessageResponse),
    Requests(RequestsResponse),
    Post(PostResponse),
    Redirect(RedirectResponse),
}

// For: GET /api/v1/collections -> { "message": [ "collection1" ] }
#[derive(Debug, Deserialize, Serialize)]
pub struct CollectionsResponse {
    #[serde(alias = "message")]
    pub collections: Vec<String>,
}

// For: DELETE /api/v1/requests/{request_id}
// For: GET /api/v1/test
//  and error messages -> { "message": "No user information found for token" }
#[derive(Debug, Deserialize, Serialize)]
pub struct MessageResponse {
    pub message: String,
}

// For: GET /api/v1/requests
// For: GET /api/v1/requests/{colleciton_id} -> { "message": { ...request fields... } }
// -> { "message": [ { ...request fields... } ] }
#[derive(Debug, Deserialize, Serialize)]
pub struct RequestsResponse {
    #[serde(alias = "message")]
    pub requests: Vec<RequestInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct User {
    pub id: String,
    pub username: String,
    pub realm: String,
    pub roles: Vec<String>,
    pub attributes: UserAttributes,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UserAttributes {
    #[serde(rename = "ecmwf-email")]
    pub ecmwf_email: String,
    #[serde(rename = "ecmwf-apikey")]
    pub ecmwf_apikey: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RequestInfo {
    pub id: String,
    pub timestamp: f64,
    pub last_modified: f64,
    pub user: User,
    pub verb: String,
    pub url: String,
    pub md5: Option<String>,
    pub collection: String,
    pub status: String,
    pub user_message: Option<String>,
    pub user_request: String,
    pub content_length: Option<f64>,
    pub content_type: Option<String>,
}

// For: POST /api/v1/requests -> { "message": "Request queued", "status": "queued" }
#[derive(Debug, Deserialize, Serialize)]
pub struct PostResponse {
    pub message: String,
    pub status: String,
    pub request_id: Option<String>,
}

// For: GET /api/v1/requests/{request_id} -> { "contentLength": 0, "contenttype": "string", ... }
#[derive(Debug, Deserialize, Serialize)]
pub struct RedirectResponse {
    #[serde(rename = "contentLength")]
    pub content_length: u64,
    #[serde(rename = "contentType")]
    pub content_type: String,
    pub location: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub struct LiveRequests {
    #[serde(alias = "live requests")]
    #[serde_as(as = "DisplayFromStr")]
    pub live_requests: u64,
}
