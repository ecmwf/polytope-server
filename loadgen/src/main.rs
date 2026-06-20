use futures_util::StreamExt;
use loadgen::{
    Config, IterationResult, Outcome, ProgressTracker, loadgen_progress_interval_from_env,
    summarize, to_bobs_internal, warmup_failed_summary,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, LOCATION};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[tokio::main]
async fn main() {
    let config = match Config::from_env() {
        Ok(config) => Arc::new(config),
        Err(err) => {
            eprintln!("loadgen config error: {err}");
            std::process::exit(2);
        }
    };

    let client = match Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .build()
    {
        Ok(client) => Arc::new(client),
        Err(err) => {
            eprintln!("failed to build HTTP client: {err}");
            std::process::exit(2);
        }
    };

    for idx in 0..config.warmup_iters {
        match run_iteration(client.clone(), config.clone(), idx, None).await {
            Ok(result) if result.outcome == Outcome::Downloaded => {}
            Ok(result) => {
                eprintln!(
                    "warm-up iteration {idx} failed: {:?}, status={:?}",
                    result.outcome, result.status
                );
                let summary = warmup_failed_summary(config.summary_config());
                println!("LOADGEN_SUMMARY:{summary}");
                std::process::exit(1);
            }
            Err(err) => {
                eprintln!("warm-up iteration {idx} failed: {err}");
                let summary = warmup_failed_summary(config.summary_config());
                println!("LOADGEN_SUMMARY:{summary}");
                std::process::exit(1);
            }
        }
    }
    eprintln!("warm-up passed: {} full cycles", config.warmup_iters);

    let progress_interval = match loadgen_progress_interval_from_env() {
        Ok(interval) => interval,
        Err(err) => {
            eprintln!("loadgen config error: {err}");
            std::process::exit(2);
        }
    };

    let measured_start = Instant::now();
    let progress = progress_interval.map(|_| Arc::new(ProgressTracker::new(measured_start)));
    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_task = progress_interval
        .zip(progress.clone())
        .map(|(interval, progress)| {
            let done = progress_done.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(interval).await;
                    if done.load(Ordering::Relaxed) {
                        break;
                    }
                    let snapshot = progress.snapshot(Instant::now());
                    println!(
                        "LOADGEN_PROGRESS:{snapshot}",
                        snapshot = serde_json::to_string(&snapshot).expect("progress serializes")
                    );
                }
            })
        });
    let results = run_measured(client, config.clone(), progress).await;
    progress_done.store(true, Ordering::Relaxed);
    if let Some(task) = progress_task {
        task.abort();
    }
    let duration_s = measured_start.elapsed().as_secs_f64();
    let summary = summarize("measured", config.summary_config(), &results, duration_s);
    let error_rate = summary.error_rate;
    println!(
        "LOADGEN_SUMMARY:{summary}",
        summary = serde_json::to_string(&summary).expect("summary serializes")
    );
    if error_rate > config.max_error_rate {
        std::process::exit(1);
    }
}

async fn run_measured(
    client: Arc<Client>,
    config: Arc<Config>,
    progress: Option<Arc<ProgressTracker>>,
) -> Vec<IterationResult> {
    let semaphore = Arc::new(Semaphore::new(0));
    let permits = semaphore.clone();
    let concurrency = config.concurrency;
    let ramp_seconds = config.ramp_seconds;
    tokio::spawn(async move {
        if ramp_seconds == 0 {
            permits.add_permits(concurrency);
            return;
        }
        let interval = Duration::from_secs_f64(ramp_seconds as f64 / concurrency as f64);
        for _ in 0..concurrency {
            permits.add_permits(1);
            tokio::time::sleep(interval).await;
        }
    });

    let mut join_set = JoinSet::new();
    for idx in 0..config.total_iters {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is not closed");
        let client = client.clone();
        let config = config.clone();
        let progress = progress.clone();
        join_set.spawn(async move {
            if let Some(progress) = &progress {
                progress.record_started();
            }
            let result = run_iteration(client, config, idx, progress.clone())
                .await
                .unwrap_or(IterationResult {
                    outcome: Outcome::DownloadFailed,
                    submit_ms: None,
                    ready_ms: None,
                    read_ms: None,
                    bytes: 0,
                    status: None,
                    read_start: None,
                    read_end: None,
                });
            if let Some(progress) = &progress {
                progress.record_finished(result.outcome, result.bytes);
            }
            drop(permit);
            result
        });
    }

    let mut results = Vec::with_capacity(config.total_iters);
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(result) => results.push(result),
            Err(err) => {
                eprintln!("iteration task failed: {err}");
                if let Some(progress) = &progress {
                    progress.record_finished(Outcome::DownloadFailed, 0);
                }
                results.push(IterationResult {
                    outcome: Outcome::DownloadFailed,
                    submit_ms: None,
                    ready_ms: None,
                    read_ms: None,
                    bytes: 0,
                    status: None,
                    read_start: None,
                    read_end: None,
                });
            }
        }
    }
    results
}

async fn run_iteration(
    client: Arc<Client>,
    config: Arc<Config>,
    idx: usize,
    progress: Option<Arc<ProgressTracker>>,
) -> Result<IterationResult, String> {
    let iteration_start = Instant::now();
    let submit_url = format!(
        "{}/api/v1/requests/{}",
        config.frontend_url, config.collection
    );
    let mut submit_headers = base_headers(&config, true)?;
    add_mock_headers(&config, idx, &mut submit_headers)?;

    let submit_response = match client
        .post(submit_url)
        .headers(submit_headers)
        .json(&config.request_body())
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => return Err(format!("submit request failed: {err}")),
    };
    let submit_ms = iteration_start.elapsed().as_secs_f64() * 1000.0;
    if let Some(progress) = &progress {
        progress.record_submitted();
    }
    let submit_status = submit_response.status();
    if submit_status != StatusCode::ACCEPTED {
        drain_response(submit_response).await.ok();
        return Ok(IterationResult {
            outcome: Outcome::SubmitFailed,
            submit_ms: Some(submit_ms),
            ready_ms: None,
            read_ms: None,
            bytes: 0,
            status: Some(submit_status.as_u16()),
            read_start: None,
            read_end: None,
        });
    }
    let submit_json: serde_json::Value = submit_response
        .json()
        .await
        .map_err(|err| format!("submit response JSON decode failed: {err}"))?;
    let Some(request_id) = submit_json
        .get("id")
        .or_else(|| submit_json.get("request_id"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
    else {
        return Ok(IterationResult {
            outcome: Outcome::SubmitFailed,
            submit_ms: Some(submit_ms),
            ready_ms: None,
            read_ms: None,
            bytes: 0,
            status: Some(submit_status.as_u16()),
            read_start: None,
            read_end: None,
        });
    };

    let poll_url = format!("{}/api/v1/requests/{request_id}", config.frontend_url);
    let deadline = Instant::now() + config.poll_timeout;
    let location = loop {
        if Instant::now() >= deadline {
            return Ok(IterationResult {
                outcome: Outcome::TimedOut,
                submit_ms: Some(submit_ms),
                ready_ms: None,
                read_ms: None,
                bytes: 0,
                status: None,
                read_start: None,
                read_end: None,
            });
        }
        let mut poll_headers = base_headers(&config, false)?;
        add_mock_headers(&config, idx, &mut poll_headers)?;
        let response = match client.get(&poll_url).headers(poll_headers).send().await {
            Ok(response) => response,
            Err(_) => {
                tokio::time::sleep(config.poll_interval).await;
                continue;
            }
        };
        let status = response.status();
        let location = response
            .headers()
            .get(LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string);
        drain_response(response).await.ok();
        if status == StatusCode::SEE_OTHER {
            if let Some(location) = location {
                break location;
            }
        } else if status == StatusCode::ACCEPTED
            || status == StatusCode::NO_CONTENT
            || status.is_redirection()
            || status.is_success()
        {
            tokio::time::sleep(config.poll_interval).await;
            continue;
        } else if status.is_client_error() || status.is_server_error() {
            return Ok(IterationResult {
                outcome: Outcome::PollFailed,
                submit_ms: Some(submit_ms),
                ready_ms: None,
                read_ms: None,
                bytes: 0,
                status: Some(status.as_u16()),
                read_start: None,
                read_end: None,
            });
        }
        tokio::time::sleep(config.poll_interval).await;
    };

    let ready_ms = iteration_start.elapsed().as_secs_f64() * 1000.0;
    if let Some(progress) = &progress {
        progress.record_ready(ready_ms);
    }
    let (internal_url, _) = match to_bobs_internal(&location, &config.bobs_svc_template) {
        Ok(value) => value,
        Err(err) => return Err(err),
    };
    let read_start = Instant::now();
    let download_result = download_body(&client, &internal_url).await;
    let read_end = Instant::now();
    let read_ms = (read_end - read_start).as_secs_f64() * 1000.0;

    match download_result {
        Ok(bytes) => Ok(IterationResult {
            outcome: Outcome::Downloaded,
            submit_ms: Some(submit_ms),
            ready_ms: Some(ready_ms),
            read_ms: Some(read_ms),
            bytes,
            status: None,
            read_start: Some(read_start),
            read_end: Some(read_end),
        }),
        Err(DownloadError { status }) => Ok(IterationResult {
            outcome: Outcome::DownloadFailed,
            submit_ms: Some(submit_ms),
            ready_ms: Some(ready_ms),
            read_ms: Some(read_ms),
            bytes: 0,
            status,
            read_start: Some(read_start),
            read_end: Some(read_end),
        }),
    }
}

fn base_headers(config: &Config, include_content_type: bool) -> Result<HeaderMap, String> {
    let mut headers = HeaderMap::new();
    headers.insert(
        AUTHORIZATION,
        HeaderValue::from_str(&config.auth)
            .map_err(|err| format!("LOADGEN_AUTH is not a valid header value: {err}"))?,
    );
    if include_content_type {
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    }
    Ok(headers)
}

fn add_mock_headers(config: &Config, idx: usize, headers: &mut HeaderMap) -> Result<(), String> {
    let Some(realm) = &config.mock_realm else {
        return Ok(());
    };
    headers.insert(
        "polytope-mock-user",
        HeaderValue::from_str(&format!("{}{idx}", config.mock_user_prefix))
            .map_err(|err| format!("mock user header is invalid: {err}"))?,
    );
    headers.insert(
        "polytope-mock-roles",
        HeaderValue::from_str(&format!("{}:{}", realm, config.mock_role))
            .map_err(|err| format!("mock roles header is invalid: {err}"))?,
    );
    Ok(())
}

struct DownloadError {
    status: Option<u16>,
}

async fn download_body(client: &Client, internal_url: &str) -> Result<u64, DownloadError> {
    let mut last_status = None;
    for attempt in 1..=5 {
        let response = client
            .get(internal_url)
            .send()
            .await
            .map_err(|_| DownloadError { status: None })?;
        let status = response.status();
        if status == StatusCode::TEMPORARY_REDIRECT {
            last_status = Some(status.as_u16());
            drain_response(response).await.ok();
            if attempt < 5 {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            return Err(DownloadError {
                status: last_status,
            });
        }
        if status != StatusCode::OK && status != StatusCode::PARTIAL_CONTENT {
            let status = status.as_u16();
            drain_response(response).await.ok();
            return Err(DownloadError {
                status: Some(status),
            });
        }
        return drain_response(response).await.map_err(|_| DownloadError {
            status: last_status,
        });
    }
    Err(DownloadError {
        status: last_status,
    })
}

async fn drain_response(response: reqwest::Response) -> Result<u64, reqwest::Error> {
    let mut stream = response.bytes_stream();
    let mut total = 0u64;
    while let Some(chunk) = stream.next().await {
        total += chunk?.len() as u64;
    }
    Ok(total)
}
