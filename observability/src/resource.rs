// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use serde_json::{Map, Value};

#[derive(Debug, Clone)]
pub struct Resource {
    service_name: &'static str,
    service_version: &'static str,
    deployment_environment: Option<String>,
    k8s_namespace_name: Option<String>,
    k8s_pod_name: Option<String>,
}

impl Resource {
    pub fn from_env(service_name: &'static str) -> Self {
        Self {
            service_name,
            service_version: env!("CARGO_PKG_VERSION"),
            deployment_environment: non_empty_env("POLYTOPE_ENV"),
            k8s_namespace_name: non_empty_env("K8S_NAMESPACE_NAME"),
            k8s_pod_name: non_empty_env("K8S_POD_NAME"),
        }
    }

    pub fn as_json(&self) -> Value {
        let mut map = Map::new();
        map.insert("service.name".into(), self.service_name.into());
        map.insert("service.version".into(), self.service_version.into());
        if let Some(value) = &self.deployment_environment {
            map.insert("deployment.environment".into(), value.clone().into());
        }
        if let Some(value) = &self.k8s_namespace_name {
            map.insert("k8s.namespace.name".into(), value.clone().into());
        }
        if let Some(value) = &self.k8s_pod_name {
            map.insert("k8s.pod.name".into(), value.clone().into());
        }
        Value::Object(map)
    }
}

fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn resource_reads_env_fields() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("POLYTOPE_ENV", "dev");
            std::env::set_var("K8S_NAMESPACE_NAME", "ns");
            std::env::set_var("K8S_POD_NAME", "pod");
        }
        let json = Resource::from_env("svc").as_json();
        assert_eq!(json["service.name"], "svc");
        assert_eq!(json["deployment.environment"], "dev");
        assert_eq!(json["k8s.namespace.name"], "ns");
        assert_eq!(json["k8s.pod.name"], "pod");
    }
}
