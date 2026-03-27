use crate::{
    auth::AuthHeader,
    error::PolytopeError,
    response::{LiveRequests, PolytopeApiResponse, RequestDetailsResponse},
};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use reqwest::{header, Client, StatusCode, Url};
use serde_json::Value;
use std::{error::Error, pin::Pin, time::Duration};
use tokio::{io::AsyncWriteExt, time::sleep};
use tracing::{debug, info};

use crate::response::{
    CollectionsResponse, MessageResponse, PostResponse, RedirectResponse, RequestsResponse,
};

#[derive(Debug)]
pub enum MimeType {
    Grib,
    CovJson,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiVersion {
    V1,
    V2,
}

impl MimeType {
    fn from_content_type(content_type: &str) -> Self {
        debug!("Determining MimeType from content type: {}", content_type);
        match content_type {
            "application/x-grib" => MimeType::Grib,
            "application/prs.coverage+json" => MimeType::CovJson,
            _ => MimeType::Unknown,
        }
    }

    pub fn file_extension(&self) -> &'static str {
        match self {
            MimeType::Grib => ".grib",
            MimeType::CovJson => ".json",
            MimeType::Unknown => ".bin",
        }
    }
}

#[derive(Debug)]
pub struct PolytopeClient {
    client: Client,
    auth_header: String,
    base_url: Url,
    api_version: ApiVersion,
    backoff: Option<f32>,
    backoff_rate: Option<f32>,
    max_backoff: Option<f32>,
}

impl PolytopeClient {
    /// Create a new PolytopeClient instance. Uses ~/.polytopeapirc or ~/.ecmwfapirc for authentication if no auth_header is provided.
    /// # Arguments:
    /// * `base_url` - The base URL of the Polytope API.
    /// * `auth_header` - Optional authentication header to use for requests.
    /// * `auth_config_file` - Optional path to a file containing authentication configuration. Ignored if auth_header is provided.
    /// * `backoff` - Optional initial backoff time in seconds for retrying requests.
    /// * `backoff_rate` - Optional rate at which the backoff time increases after each retry.
    /// * `max_backoff` - Optional maximum backoff time in seconds.
    pub fn new(
        base_url: String,
        auth_header: Option<AuthHeader>,
        auth_config_file: Option<&str>,
        backoff: Option<f32>,
        backoff_rate: Option<f32>,
        max_backoff: Option<f32>,
    ) -> Result<Self, Box<dyn Error>> {
        // Check for ambiguity
        if auth_header.is_some() && auth_config_file.is_some() {
            return Err("Cannot provide both auth_header and auth_config_file; it's ambiguous which to use.".into());
        }

        // Determine the auth header
        let auth_header = if let Some(auth) = auth_header {
            auth
        } else if let Some(file) = auth_config_file {
            AuthHeader::from_file(file)?
        } else {
            AuthHeader::from_default_files()?
        };

        // Convert AuthHeader into a formatted string
        let auth_header: String = auth_header.into();
        debug!("Using auth header: {}", auth_header);
        Ok(Self {
            client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            auth_header,
            base_url: Url::parse(&base_url)?,
            api_version: ApiVersion::V2,
            backoff,
            backoff_rate,
            max_backoff,
        })
    }

    pub fn api_version(mut self, api_version: ApiVersion) -> Self {
        self.api_version = api_version;
        self
    }

    /// List all collections the user has access to.
    pub async fn list_collections(&self) -> Result<CollectionsResponse, Box<dyn Error>> {
        let url = self
            .base_url
            .join("api/v1/collections")
            .expect("Invalid URL");
        let response: reqwest::Response = self
            .client
            .get(url)
            .headers(self.auth_headers())
            .send()
            .await?;
        debug!("Response: {:?}", response);
        let status = response.status();
        let body = response.text().await?;
        debug!("Response body: {}", body);
        if status.is_success() {
            let parsed_response: CollectionsResponse = serde_json::from_str(&body)?;
            Ok(parsed_response)
        } else {
            Err(handle_error_from_body(status, &body))
        }
    }

    /// Retrieve details for all requests for a specific collection.
    pub async fn get_requests(
        &self,
        collection: Option<&str>,
    ) -> Result<RequestsResponse, Box<dyn Error>> {
        // Only add the collection to the URL if it's provided
        let collection = collection.unwrap_or("");
        let url = self
            .base_url
            .join(&format!("api/v1/requests/{}", collection))?;
        let response = self
            .client
            .get(url)
            .headers(self.auth_headers())
            .send()
            .await?;
        debug!("Response: {:?}", response);
        let status = response.status();
        let body = response.text().await?;
        debug!("Response body: {}", body);
        if status.is_success() {
            let parsed_response: RequestsResponse = serde_json::from_str(&body)?;
            Ok(parsed_response)
        } else {
            Err(handle_error_from_body(status, &body))
        }
    }

    /// Query request status.
    pub async fn get_request_details(
        &self,
        request_id: &str,
    ) -> Result<RequestDetailsResponse, Box<dyn Error>> {
        let url = self.base_url.join(&self.request_status_path(request_id))?;
        let response = self
            .client
            .get(url.clone())
            .headers(self.auth_headers())
            .send()
            .await?;
        debug!("Response: {:?}", response);
        let status = response.status();
        let body = response.text().await?;
        debug!("Response body: {}", body);
        match status {
            StatusCode::ACCEPTED => {
                let parsed: PostResponse = serde_json::from_str(&body)?;
                Ok(RequestDetailsResponse::Post(parsed))
            }
            StatusCode::SEE_OTHER => {
                if body.trim().is_empty() {
                    Ok(RequestDetailsResponse::Post(PostResponse {
                        message: "Request is being processed".to_string(),
                        status: "queued".to_string(),
                        request_id: Some(request_id.to_string()),
                    }))
                } else {
                    let parsed: RedirectResponse = serde_json::from_str(&body)?;
                    Ok(RequestDetailsResponse::Redirect(parsed))
                }
            }
            _ => Err(handle_error_from_body(status, &body)),
        }
    }

    /// Delete a specific request.
    pub async fn delete_request(
        &self,
        request_id: &str,
    ) -> Result<PolytopeApiResponse, Box<dyn Error>> {
        let url = self.base_url.join(&self.request_status_path(request_id))?;
        let response = self
            .client
            .delete(url)
            .headers(self.auth_headers())
            .send()
            .await?;
        debug!("Response: {:?}", response);
        let status = response.status();
        let body = response.text().await?;
        debug!("Response body: {}", body);
        if status.is_success() {
            let parsed: MessageResponse = serde_json::from_str(&body)?;
            Ok(PolytopeApiResponse::Deletion(parsed))
        } else {
            Err(handle_error_from_body(status, &body))
        }
    }

    /// Download a file from a given URL.
    pub async fn download(
        &self,
        url: Url,
    ) -> Result<
        (
            Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
            MimeType,
            u64,
        ),
        Box<dyn Error>,
    > {
        info!("Downloading from URL: {}", url);
        let response = self
            .client
            .get(url)
            // .headers(self.auth_headers()) // currently seaweedfs denies access if you send auth
            .send()
            .await?;
        debug!("Response: {:?}", response);
        if response.status().is_success() {
            let mime_type = MimeType::from_content_type(
                response
                    .headers()
                    .get("Content-Type")
                    .unwrap_or(&header::HeaderValue::from_static(
                        "application/octet-stream",
                    ))
                    .to_str()?,
            );
            let content_length = response.content_length().unwrap_or(0);
            Ok((Box::pin(response.bytes_stream()), mime_type, content_length))
        } else {
            Err(handle_error_response(response).await)
        }
    }

    /// Submit a request to a collection and return the initial response.
    pub async fn submit_request(
        &self,
        collection: &str,
        request_body: Value,
    ) -> Result<PostResponse, Box<dyn Error>> {
        debug!("Creating payload for collection: {}", collection);
        let (url, payload) = match self.api_version {
            ApiVersion::V1 => {
                let mut payload = Value::Object(serde_json::Map::new());
                payload["request"] = request_body.clone();
                payload["collection"] = Value::String(collection.to_string());
                payload["verb"] = Value::String("retrieve".to_string());
                (
                    self.base_url
                        .join(&format!("api/v1/requests/{}", collection))?,
                    payload,
                )
            }
            ApiVersion::V2 => (
                self.base_url
                    .join(&format!("api/v2/{}/requests", collection))?,
                request_body,
            ),
        };
        info!("Sending request: {}\nto {}", payload, url);

        let response = self
            .client
            .post(url)
            .json(&payload)
            .headers(self.auth_headers())
            .send()
            .await?;
        debug!("Response: {:?}", response);
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.text().await?;
        debug!("Response body: {}", body);
        match status {
            StatusCode::ACCEPTED => {
                // Extract Location header for request_id - to be fixed on api side
                let request_id = headers
                    .get("Location")
                    .and_then(|loc| loc.to_str().ok())
                    .and_then(|loc| loc.split('/').next_back().map(String::from));
                if let Some(ref req_id) = request_id {
                    info!("Request with ID: {} accepted!", req_id);
                }
                let mut post_response: PostResponse = serde_json::from_str(&body)?;
                post_response.request_id = request_id;
                debug!("Parsed PostResponse: {:?}", post_response);
                Ok(post_response)
            }
            StatusCode::SEE_OTHER if self.api_version == ApiVersion::V2 => {
                let request_id = headers
                    .get("Location")
                    .and_then(|loc| loc.to_str().ok())
                    .and_then(|loc| loc.split('/').next_back().map(String::from));
                Ok(PostResponse {
                    message: "Request queued".to_string(),
                    status: "queued".to_string(),
                    request_id,
                })
            }
            _ => Err(handle_error_from_body(status, &body)),
        }
    }

    /// Get the number of live requests for the user.
    pub async fn get_live_requests(&self) -> Result<u64, Box<dyn Error>> {
        let url = self.base_url.join("api/v1/user")?;
        let response = self
            .client
            .get(url)
            .headers(self.auth_headers())
            .send()
            .await?;
        debug!("Response: {:?}", response);
        let status = response.status();
        let body = response.text().await?;
        debug!("Response body: {}", body);
        if status.is_success() {
            let live_requests: LiveRequests = serde_json::from_str(&body)?;
            Ok(live_requests.live_requests)
        } else {
            Err(handle_error_from_body(status, &body))
        }
    }

    /// Submit a request to a collection, follow the redirects, and return the download stream.
    pub async fn retrieve(
        &self,
        collection: &str,
        request_body: Value,
    ) -> Result<
        (
            Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
            MimeType,
            u64,
        ),
        Box<dyn Error>,
    > {
        match self.api_version {
            ApiVersion::V1 => self.retrieve_v1(collection, request_body).await,
            ApiVersion::V2 => self.retrieve_v2(collection, request_body).await,
        }
    }

    /// Submit a request to a collection, follow the redirects, and download the result to a file.
    ///
    /// # Arguments:
    /// * `collection` - The name of the collection to submit the request to.
    /// * `request_body` - The request body as a JSON value.
    /// * `file_path` - The path where the downloaded file should be saved. If empty, the file name will be derived from the URL.
    pub async fn retrieve_to_file(
        &self,
        collection: &str,
        request_body: Value,
        file_path: &str,
    ) -> Result<(String, MimeType), Box<dyn Error>> {
        let result_url = self.retrieve_to_url(collection, request_body).await?;
        info!(
            "Request processed successfully! Now downloading from URL: {}",
            result_url
        );
        let (stream, mime_type, _) = self.download(Url::parse(&result_url)?).await?;
        let file_path = file_path.to_string();
        // Use the last part of the URL as the file name if file_path is empty, otherwise filepath.mime_type_ext
        let full_path = if file_path.is_empty() {
            result_url
                .split('/')
                .last()
                .map(String::from)
                .unwrap_or_else(|| "downloaded_file".to_string())
        } else {
            format!("{}{}", file_path, mime_type.file_extension())
        };
        let full_path_for_return = full_path.clone();

        let mut file = tokio::fs::File::create(full_path).await?;
        tokio::pin!(stream);

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(data) => {
                    file.write_all(&data).await?;
                }
                Err(e) => {
                    return Err(Box::new(e));
                }
            }
        }
        Ok((full_path_for_return, mime_type))
    }

    /// Submit a request to a collection, follow the redirects, and return the URL of the result.
    ///
    /// the result
    pub async fn retrieve_to_url(
        &self,
        collection: &str,
        request_body: Value,
    ) -> Result<String, Box<dyn Error>> {
        if self.api_version == ApiVersion::V2 {
            return Err(Box::new(PolytopeError::Unexpected(
                "retrieve_to_url is only available for ApiVersion::V1".to_string(),
            )));
        }

        let post_response = self.submit_request(collection, request_body).await?;
        let request_id = post_response.request_id.as_ref().ok_or_else(|| {
            PolytopeError::Unexpected("Request ID is missing in PostResponse".to_string())
        })?;

        let mut backoff = self.backoff.unwrap_or(0.1);
        let backoff_rate = self.backoff_rate.unwrap_or(1.03);
        let max_backoff = self.max_backoff.unwrap_or(20.0);

        info!("Polling for request status...");
        loop {
            sleep(Duration::from_secs_f32(backoff)).await;
            let poly_response = self.get_request_details(&request_id).await?;
            match poly_response {
                RequestDetailsResponse::Post(post_response) => {
                    debug!("Request status: {}", post_response.status);
                    backoff = (backoff * backoff_rate).min(max_backoff);
                }
                RequestDetailsResponse::Redirect(redirect_response) => {
                    info!("Request processed successfully!");
                    return Ok(redirect_response.location);
                }
            }
        }
    }

    fn auth_headers(&self) -> header::HeaderMap {
        let mut headers = header::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            header::HeaderValue::from_str(&self.auth_header).unwrap(),
        );
        headers
    }

    fn request_status_path(&self, request_id: &str) -> String {
        match self.api_version {
            ApiVersion::V1 => format!("api/v1/requests/{}", request_id),
            ApiVersion::V2 => format!("api/v2/requests/{}", request_id),
        }
    }

    async fn retrieve_v2(
        &self,
        collection: &str,
        request_body: Value,
    ) -> Result<
        (
            Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
            MimeType,
            u64,
        ),
        Box<dyn Error>,
    > {
        let mut next_url = self
            .base_url
            .join(&format!("api/v2/{}/requests", collection))?;
        let mut is_first = true;

        loop {
            let response = if is_first {
                is_first = false;
                self.client
                    .post(next_url.clone())
                    .json(&request_body)
                    .headers(self.auth_headers())
                    .send()
                    .await?
            } else {
                self.client
                    .get(next_url.clone())
                    .headers(self.auth_headers())
                    .send()
                    .await?
            };

            let status = response.status();

            match status {
                StatusCode::OK => {
                    let mime_type = MimeType::from_content_type(
                        response
                            .headers()
                            .get("Content-Type")
                            .unwrap_or(&header::HeaderValue::from_static(
                                "application/octet-stream",
                            ))
                            .to_str()?,
                    );
                    let content_length = response.content_length().unwrap_or(0);
                    return Ok((Box::pin(response.bytes_stream()), mime_type, content_length));
                }
                StatusCode::SEE_OTHER => {
                    let location = response
                        .headers()
                        .get(header::LOCATION)
                        .and_then(|loc| loc.to_str().ok())
                        .ok_or_else(|| {
                            PolytopeError::Unexpected(
                                "v2 redirect response missing Location header".to_string(),
                            )
                        })?;
                    next_url = self.base_url.join(location)?;
                }
                _ => {
                    let body = response.text().await?;
                    return Err(handle_error_from_body(status, &body));
                }
            }
        }
    }

    async fn retrieve_v1(
        &self,
        collection: &str,
        request_body: Value,
    ) -> Result<
        (
            Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>,
            MimeType,
            u64,
        ),
        Box<dyn Error>,
    > {
        let post_response = self.submit_request(collection, request_body).await?;
        let request_id = post_response.request_id.ok_or_else(|| {
            PolytopeError::Unexpected("Request ID is missing in PostResponse".to_string())
        })?;

        let mut backoff = self.backoff.unwrap_or(0.1);
        let backoff_rate = self.backoff_rate.unwrap_or(1.03);
        let max_backoff = self.max_backoff.unwrap_or(20.0);

        loop {
            sleep(Duration::from_secs_f32(backoff)).await;
            let poll_url = self.base_url.join(&self.request_status_path(&request_id))?;
            let response = self
                .client
                .get(poll_url)
                .headers(self.auth_headers())
                .send()
                .await?;

            let status = response.status();
            match status {
                StatusCode::ACCEPTED => {
                    backoff = (backoff * backoff_rate).min(max_backoff);
                }
                StatusCode::SEE_OTHER => {
                    let location = response
                        .headers()
                        .get(header::LOCATION)
                        .and_then(|loc| loc.to_str().ok())
                        .ok_or_else(|| {
                            PolytopeError::Unexpected(
                                "v1 redirect response missing Location header".to_string(),
                            )
                        })?;
                    let download_url = self.base_url.join(location)?;
                    return self.download(download_url).await;
                }
                StatusCode::OK => {
                    let mime_type = MimeType::from_content_type(
                        response
                            .headers()
                            .get("Content-Type")
                            .unwrap_or(&header::HeaderValue::from_static(
                                "application/octet-stream",
                            ))
                            .to_str()?,
                    );
                    let content_length = response.content_length().unwrap_or(0);
                    return Ok((Box::pin(response.bytes_stream()), mime_type, content_length));
                }
                _ => {
                    let body = response.text().await?;
                    return Err(handle_error_from_body(status, &body));
                }
            }
        }
    }
}

async fn handle_error_response(response: reqwest::Response) -> Box<dyn Error> {
    return handle_error_from_body(response.status(), &response.text().await.unwrap());
}

fn handle_error_from_body(status: StatusCode, body: &str) -> Box<dyn Error> {
    let msg: String = serde_json::from_str(body)
        .map(|error_body: MessageResponse| error_body.message)
        .unwrap_or_else(|_| body.to_string());

    let error = match status {
        StatusCode::BAD_REQUEST => PolytopeError::BadRequest(format!("400 Bad Request: {}", msg)),
        StatusCode::UNAUTHORIZED => {
            PolytopeError::Unauthorized(format!("401 Unauthorized: {}", msg))
        }
        StatusCode::FORBIDDEN => PolytopeError::Forbidden(format!("403 Forbidden: {}", msg)),
        StatusCode::INTERNAL_SERVER_ERROR => {
            PolytopeError::ServerError(format!("500 Internal Server Error: {}", msg))
        }
        StatusCode::NOT_FOUND => PolytopeError::NotFound(format!("404 Not Found: {}", msg)),
        _ => PolytopeError::Unexpected(format!("{}: {}", status, msg)),
    };

    Box::new(error)
}
