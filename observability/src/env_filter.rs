use tracing_subscriber::EnvFilter;

const DEFAULT_FILTER: &str =
    "info,hyper=warn,hyper_util=warn,h2=warn,reqwest=warn,tikv_client=warn,tikv-client=warn";

pub fn env_filter_from_env() -> EnvFilter {
    match std::env::var("RUST_LOG") {
        Ok(value) if !value.trim().is_empty() => value
            .trim()
            .parse()
            .unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER)),
        _ => EnvFilter::new(DEFAULT_FILTER),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tracing::Level;
    use tracing_subscriber::filter::LevelFilter;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn max_level(filter: EnvFilter) -> LevelFilter {
        filter.max_level_hint().unwrap()
    }

    #[test]
    fn unset_empty_and_invalid_fall_back_to_info() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::remove_var("RUST_LOG") };
        assert!(max_level(env_filter_from_env()) >= Level::INFO);
        unsafe { std::env::set_var("RUST_LOG", "   ") };
        assert!(max_level(env_filter_from_env()) >= Level::INFO);
        unsafe { std::env::set_var("RUST_LOG", "not a valid filter!!!") };
        assert!(max_level(env_filter_from_env()) >= Level::INFO);
    }

    #[test]
    fn explicit_directives_are_honoured() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("RUST_LOG", "error") };
        assert_eq!(max_level(env_filter_from_env()), LevelFilter::ERROR);
    }
}
