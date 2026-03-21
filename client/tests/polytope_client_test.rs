use polytope_client::polytope_client::PolytopeClient;
use regex::Regex;
use serde_json::json;
use std::error::Error;
use tracing::{debug, error, info};
use tracing_test::traced_test;

#[tokio::test]
#[traced_test]
async fn test_retrieve_to_file() -> Result<(), Box<dyn Error>> {
    // Define test parameters
    let base_url = "https://polytope-test.lumi.apps.dte.destination-earth.eu".to_string();
    let collection = "destination-earth";
    let request_body = json!({
            "activity": "ScenarioMIP",
            "class": "d1",
            "dataset": "climate-dt",
            "date": "20200101/to/20200102",
            "experiment": "SSP3-7.0",
            "expver": "0001",
            "generation": "1",
            "levtype": "sfc",
            "model": "IFS-NEMO",
            "param": "134/165/166",
            "realization": "1",
            "resolution": "standard",
            "stream": "clte",
            "time": "0600",
            "type": "fc",
    });
    debug!("Request body: {:?}", request_body);

    // Create the PolytopeClient
    let client = PolytopeClient::new(base_url, None, None, None, None, None)?;

    // Submit the request to a file
    let result = client.retrieve_to_file(collection, request_body, "").await;

    // Assert the result
    assert!(result.is_ok(), "Request submission failed");
    debug!("Result: {:?}", result);
    match result {
        Ok((full_path, mime_type)) => {
            // Check if the file exists
            assert!(
                std::path::Path::new(&full_path).exists(),
                "File does not exist at path: {}",
                full_path
            );
            // Check if the file has the expected format uuid.extension
            let re = Regex::new(r"^[0-9a-z\-]{36}\.[a-z]+$").unwrap();
            assert!(
                re.is_match(&full_path),
                "Filename does not match expected format: {{uuid}}.{{ext}}: {}",
                full_path
            );
            info!(
                "Request processed successfully and saved to file {full_path} of type {:?}",
                mime_type
            );
            // delete the file after test
            std::fs::remove_file(full_path).expect("Failed to delete test file");
            info!("Test file deleted successfully.");
            assert!(true);
        }
        Err(err) => {
            error!("Failed to submit request: {}", err);
            assert!(false, "Request submission failed");
        }
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_list_collections() -> Result<(), Box<dyn Error>> {
    // Define test parameters
    let base_url = "https://polytope-test.lumi.apps.dte.destination-earth.eu".to_string();

    // Create the PolytopeClient
    let client = PolytopeClient::new(base_url, None, None, None, None, None)?;

    // Retrieve collections
    let result = client.list_collections().await;

    // Assert the result
    match result {
        Ok(collections) => {
            info!("Collections retrieved successfully: {:?}", collections);
            assert!(!collections.collections.is_empty());
        }
        Err(err) => {
            error!("Failed to retrieve collections: {}", err);
            assert!(false, "Collection retrieval failed");
        }
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_get_requests() -> Result<(), Box<dyn Error>> {
    let base_url = "https://polytope-test.lumi.apps.dte.destination-earth.eu".to_string();
    let client = PolytopeClient::new(base_url, None, None, None, None, None)?;
    let result = client.get_requests(Some("destination-earth")).await;
    match result {
        Ok(requests) => {
            info!("Requests retrieved successfully: {:?}", requests);
            assert!(requests.requests.len() > 0);
        }
        Err(err) => {
            error!("Failed to retrieve requests: {}", err);
            assert!(false, "Request retrieval failed");
        }
    }
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_get_request_details_404() -> Result<(), Box<dyn Error>> {
    let base_url = "https://polytope-test.lumi.apps.dte.destination-earth.eu".to_string();
    let client = PolytopeClient::new(base_url, None, None, None, None, None)?;
    // You need a valid request_id for this test to succeed
    let request_id = "your_request_id_here";
    let result = client.get_request_details(request_id).await;
    match result {
        Ok(details) => {
            assert!(false, "Expected 404 error, but got details: {:?}", details);
        }
        Err(err) => {
            if let Some(polytope_err) = err.downcast_ref::<polytope_client::error::PolytopeError>()
            {
                match polytope_err {
                    polytope_client::error::PolytopeError::NotFound(_) => {
                        info!("Expected error: {}", polytope_err);
                        assert!(true);
                    }
                    _ => {
                        assert!(false, "Expected NotFound error, but got: {}", polytope_err);
                    }
                }
            } else {
                assert!(false, "Error is not a PolytopeError: {}", err);
            }
        }
    }
    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_delete_request_lifecycle() -> Result<(), Box<dyn Error>> {
    let base_url = "https://polytope-test.lumi.apps.dte.destination-earth.eu".to_string();
    let collection = "destination-earth";
    let request_body = json!({
        "activity": "ScenarioMIP",
        "class": "d1",
        "dataset": "climate-dt",
        "date": "20200101/to/20200102",
        "experiment": "SSP3-7.0",
        "expver": "0001",
        "generation": "1",
        "levtype": "sfc",
        "model": "IFS-NEMO",
        "param": "134/165/166",
        "realization": "1",
        "resolution": "standard",
        "stream": "clte",
        "time": "0600",
        "type": "fc",
    });

    let client = PolytopeClient::new(base_url, None, None, None, None, None)?;

    // Submit the request
    let submit_result = client.submit_request(collection, request_body).await?;
    let request_id = submit_result
        .request_id
        .ok_or("No request ID returned from submission")?;
    info!("Request submitted successfully with ID: {}", request_id);
    // Check that it exists
    let details_result = client.get_request_details(&request_id).await;
    match details_result {
        Ok(details) => {
            info!("Request exists: {:?}", details);
            assert!(true);
        }
        Err(err) => {
            error!("Request should exist but was not found: {}", err);
            assert!(false, "Request should exist after submission");
            return Ok(());
        }
    }

    // Delete the request
    let delete_result = client.delete_request(&request_id).await;
    match delete_result {
        Ok(response) => {
            info!("Delete response: {:?}", response);
            assert!(true);
        }
        Err(err) => {
            error!("Failed to delete request: {}", err);
            assert!(false, "Delete request failed");
            return Ok(());
        }
    }

    // Check that it no longer exists
    let details_after_delete = client.get_request_details(&request_id).await;
    match details_after_delete {
        Ok(details) => {
            error!(
                "Request should not exist after deletion, but got: {:?}",
                details
            );
            assert!(false, "Request still exists after deletion");
        }
        Err(_err) => {
            // Expected: not found
            assert!(true);
        }
    }

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn test_get_live_requests() -> () {
    let base_url = "https://polytope-test.lumi.apps.dte.destination-earth.eu".to_string();
    let client = PolytopeClient::new(base_url, None, None, None, None, None).unwrap();

    // Retrieve live requests
    let result = client.get_live_requests().await.unwrap();
    info!("Live requests retrieved successfully: {:?}", result);
    // Assert the result
    assert!(true);
}
