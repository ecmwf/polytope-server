// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use futures_util::StreamExt;
use loadgen::{
    Config, IterationResult, Outcome, ProgressTracker, RunLimit, SummaryMetadata,
    loadgen_progress_interval_from_env, summarize_with_metadata, to_bobs_internal,
    warmup_failed_summary,
};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, LOCATION};
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
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
    let measured = run_measured(client, config.clone(), progress).await;
    progress_done.store(true, Ordering::Relaxed);
    if let Some(task) = progress_task {
        task.abort();
    }
    let duration_s = measured_start.elapsed().as_secs_f64();
    let summary = summarize_with_metadata(
        "measured",
        config.summary_config(),
        &measured.results,
        duration_s,
        SummaryMetadata {
            run_limit: config.run_limit.name().to_string(),
            target_duration_s: config.run_limit.target_duration_s(),
            submission_duration_s: measured.submission_duration_s,
            drain_duration_s: measured.drain_duration_s,
            scheduled: measured.scheduled,
            missed_starts: measured.missed_starts,
            measured_start: Some(measured_start),
            bucket_width_s: 60.0,
        },
    );
    let error_rate = summary.error_rate;
    println!(
        "LOADGEN_SUMMARY:{summary}",
        summary = serde_json::to_string(&summary).expect("summary serializes")
    );
    if error_rate > config.max_error_rate {
        std::process::exit(1);
    }
}

struct MeasuredRun {
    results: Vec<IterationResult>,
    submission_duration_s: f64,
    drain_duration_s: f64,
    scheduled: usize,
    missed_starts: usize,
}

async fn run_measured(
    client: Arc<Client>,
    config: Arc<Config>,
    progress: Option<Arc<ProgressTracker>>,
) -> MeasuredRun {
    match config.run_limit.clone() {
        RunLimit::Iterations => run_measured_iterations(client, config, progress).await,
        RunLimit::Duration { duration, rps } => match rps {
            Some(rps) => run_measured_duration_open(client, config, progress, duration, rps).await,
            None => run_measured_duration_closed_loop(client, config, progress, duration).await,
        },
    }
}

async fn run_measured_iterations(
    client: Arc<Client>,
    config: Arc<Config>,
    progress: Option<Arc<ProgressTracker>>,
) -> MeasuredRun {
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
    let submission_start = Instant::now();
    for idx in 0..config.total_iters {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is not closed");
        spawn_iteration(
            &mut join_set,
            client.clone(),
            config.clone(),
            idx,
            progress.clone(),
            permit,
        );
    }

    let submission_duration_s = submission_start.elapsed().as_secs_f64();
    let drain_start = Instant::now();
    let mut results = Vec::with_capacity(config.total_iters);
    drain_join_set(&mut join_set, &mut results, &progress).await;
    MeasuredRun {
        results,
        submission_duration_s,
        drain_duration_s: drain_start.elapsed().as_secs_f64(),
        scheduled: config.total_iters,
        missed_starts: 0,
    }
}

async fn run_measured_duration_closed_loop(
    client: Arc<Client>,
    config: Arc<Config>,
    progress: Option<Arc<ProgressTracker>>,
    duration: Duration,
) -> MeasuredRun {
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

    let submission_start = Instant::now();
    let deadline = submission_start + duration;
    let mut join_set = JoinSet::new();
    let mut scheduled = 0usize;
    loop {
        if Instant::now() >= deadline {
            break;
        }
        let permit_fut = semaphore.clone().acquire_owned();
        tokio::pin!(permit_fut);
        tokio::select! {
            permit = &mut permit_fut => {
                let permit = permit.expect("semaphore is not closed");
                if Instant::now() >= deadline {
                    drop(permit);
                    break;
                }
                spawn_iteration(
                    &mut join_set,
                    client.clone(),
                    config.clone(),
                    scheduled,
                    progress.clone(),
                    permit,
                );
                scheduled += 1;
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => break,
        }
    }

    let submission_duration_s = submission_start.elapsed().as_secs_f64();
    let drain_start = Instant::now();
    let mut results = Vec::with_capacity(scheduled);
    drain_join_set(&mut join_set, &mut results, &progress).await;
    MeasuredRun {
        results,
        submission_duration_s,
        drain_duration_s: drain_start.elapsed().as_secs_f64(),
        scheduled,
        missed_starts: 0,
    }
}

async fn run_measured_duration_open(
    client: Arc<Client>,
    config: Arc<Config>,
    progress: Option<Arc<ProgressTracker>>,
    duration: Duration,
    rps: f64,
) -> MeasuredRun {
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let submission_start = Instant::now();
    let deadline = submission_start + duration;
    let mut join_set = JoinSet::new();
    let mut results = Vec::new();
    let mut scheduled = 0usize;
    let mut missed_starts = 0usize;
    let mut start_number = 1usize;

    loop {
        let planned_offset = planned_open_model_offset(start_number, rps, config.ramp_seconds);
        let planned_at = submission_start + planned_offset;
        if planned_at >= deadline {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
            break;
        }
        while let Some(join_result) = join_set.try_join_next() {
            handle_join_result(join_result, &mut results, &progress);
        }
        tokio::time::sleep_until(tokio::time::Instant::from_std(planned_at)).await;
        match semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                spawn_iteration(
                    &mut join_set,
                    client.clone(),
                    config.clone(),
                    scheduled,
                    progress.clone(),
                    permit,
                );
                scheduled += 1;
            }
            Err(_) => {
                missed_starts += 1;
                if let Some(progress) = &progress {
                    progress.record_missed_start();
                }
            }
        }
        start_number += 1;
    }

    let submission_duration_s = submission_start.elapsed().as_secs_f64();
    let drain_start = Instant::now();
    drain_join_set(&mut join_set, &mut results, &progress).await;
    MeasuredRun {
        results,
        submission_duration_s,
        drain_duration_s: drain_start.elapsed().as_secs_f64(),
        scheduled,
        missed_starts,
    }
}

fn planned_open_model_offset(start_number: usize, rps: f64, ramp_seconds: u64) -> Duration {
    let n = start_number as f64;
    let ramp_s = ramp_seconds as f64;
    let offset_s = if ramp_s <= 0.0 {
        n / rps
    } else {
        let starts_during_ramp = rps * ramp_s / 2.0;
        if n <= starts_during_ramp {
            (2.0 * ramp_s * n / rps).sqrt()
        } else {
            (n / rps) + (ramp_s / 2.0)
        }
    };
    Duration::from_secs_f64(offset_s)
}

fn spawn_iteration(
    join_set: &mut JoinSet<IterationResult>,
    client: Arc<Client>,
    config: Arc<Config>,
    idx: usize,
    progress: Option<Arc<ProgressTracker>>,
    permit: OwnedSemaphorePermit,
) {
    join_set.spawn(async move {
        if let Some(progress) = &progress {
            progress.record_started();
        }
        let result = run_iteration(client, config, idx, progress.clone())
            .await
            .unwrap_or_else(|_| failed_iteration_result());
        if let Some(progress) = &progress {
            progress.record_finished(result.outcome, result.bytes);
        }
        drop(permit);
        result
    });
}

async fn drain_join_set(
    join_set: &mut JoinSet<IterationResult>,
    results: &mut Vec<IterationResult>,
    progress: &Option<Arc<ProgressTracker>>,
) {
    while let Some(join_result) = join_set.join_next().await {
        handle_join_result(join_result, results, progress);
    }
}

fn handle_join_result(
    join_result: Result<IterationResult, tokio::task::JoinError>,
    results: &mut Vec<IterationResult>,
    progress: &Option<Arc<ProgressTracker>>,
) {
    match join_result {
        Ok(result) => results.push(result),
        Err(err) => {
            eprintln!("iteration task failed: {err}");
            if let Some(progress) = progress {
                progress.record_finished(Outcome::DownloadFailed, 0);
            }
            results.push(failed_iteration_result());
        }
    }
}

fn failed_iteration_result() -> IterationResult {
    IterationResult {
        outcome: Outcome::DownloadFailed,
        submit_ms: None,
        ready_ms: None,
        read_ms: None,
        bytes: 0,
        status: None,
        read_start: None,
        read_end: None,
        completed_at: Instant::now(),
    }
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
            completed_at: Instant::now(),
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
            completed_at: Instant::now(),
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
                completed_at: Instant::now(),
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
                completed_at: Instant::now(),
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
            completed_at: read_end,
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
            completed_at: read_end,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_config(base_url: String, duration: Duration, rps: Option<f64>) -> Arc<Config> {
        Arc::new(Config {
            frontend_url: base_url.clone(),
            collection: "c".to_string(),
            auth: "Bearer test".to_string(),
            payload_json: json!({"request": "test"}),
            mock_realm: None,
            mock_role: "default".to_string(),
            mock_user_prefix: "mock-".to_string(),
            warmup_iters: 0,
            concurrency: 1,
            total_iters: 0,
            run_limit: RunLimit::Duration { duration, rps },
            ramp_seconds: 0,
            poll_interval: Duration::from_millis(1),
            poll_timeout: Duration::from_secs(5),
            bobs_svc_template: base_url,
            max_error_rate: 1.0,
        })
    }

    async fn start_test_server(download_delay: Duration) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let location_base = base_url.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let location_base = location_base.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let Ok(n) = stream.read(&mut buf).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&buf[..n]);
                    let Some(first_line) = request.lines().next() else {
                        return;
                    };
                    let mut parts = first_line.split_whitespace();
                    let method = parts.next().unwrap_or_default();
                    let path = parts.next().unwrap_or_default();
                    let response = if method == "POST" && path == "/api/v1/requests/c" {
                        "HTTP/1.1 202 Accepted\r\nContent-Type: application/json\r\nContent-Length: 14\r\n\r\n{\"id\":\"req-1\"}".to_string()
                    } else if method == "GET" && path == "/api/v1/requests/req-1" {
                        format!(
                            "HTTP/1.1 303 See Other\r\nLocation: {location_base}/download-0/0123abcd\r\nContent-Length: 0\r\n\r\n"
                        )
                    } else if method == "GET"
                        && matches!(
                            path,
                            "/api/v1/read/api/v1/read/0123abcd" | "/api/v1/read/0123abcd"
                        )
                    {
                        tokio::time::sleep(download_delay).await;
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".to_string()
                    } else {
                        format!(
                            "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\n\r\n{path}",
                            path.len()
                        )
                    };
                    let _ = stream.write_all(response.as_bytes()).await;
                });
            }
        });
        base_url
    }

    #[tokio::test]
    async fn duration_closed_loop_stops_scheduling_at_deadline_and_drains_in_flight() {
        let base_url = start_test_server(Duration::from_millis(500)).await;
        let client = Arc::new(
            Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        );
        let config = test_config(base_url, Duration::from_millis(20), None);
        let run =
            run_measured_duration_closed_loop(client, config, None, Duration::from_millis(20))
                .await;
        assert_eq!(run.scheduled, 1);
        assert_eq!(run.missed_starts, 0);
        assert_eq!(run.results.len(), 1);
        assert!(
            run.submission_duration_s >= 0.015,
            "{}",
            run.submission_duration_s
        );
        assert!(run.drain_duration_s >= 0.05, "{}", run.drain_duration_s);
    }

    #[tokio::test]
    async fn duration_open_model_counts_missed_starts_instead_of_queueing() {
        let base_url = start_test_server(Duration::from_millis(200)).await;
        let client = Arc::new(
            Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
        );
        let config = test_config(base_url, Duration::from_millis(250), Some(20.0));
        let progress = Arc::new(ProgressTracker::new(Instant::now()));
        let run = run_measured_duration_open(
            client,
            config,
            Some(progress.clone()),
            Duration::from_millis(250),
            20.0,
        )
        .await;
        assert!(run.scheduled >= 1, "scheduled={}", run.scheduled);
        assert!(run.missed_starts >= 1, "missed={}", run.missed_starts);
        assert!(
            run.submission_duration_s >= 0.24,
            "{}",
            run.submission_duration_s
        );
        let snapshot = progress.snapshot(Instant::now());
        assert_eq!(snapshot.counts.scheduled, run.scheduled);
        assert_eq!(snapshot.counts.missed_starts, run.missed_starts);
    }
}
