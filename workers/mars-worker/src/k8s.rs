// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Pod, Service, ServicePort, ServiceSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::{Api, Client};
use tracing::{info, warn};

/// Manages the NodePort that targets one Mars worker callback relay.
pub struct NodePortManager {
    node_port: u16,
    node_name: String,
    namespace: String,
    service_name: String,
    relay_port: u16,
}

fn callback_node_port(service: &Service) -> Option<i32> {
    service
        .spec
        .as_ref()?
        .ports
        .as_ref()?
        .iter()
        .find(|port| port.name.as_deref() == Some("mars-dhs-callback"))
        .and_then(|port| port.node_port)
}

impl NodePortManager {
    async fn retry_api_call<T, E, F, Fut>(
        mut operation: F,
        operation_name: &str,
    ) -> Result<T, Box<dyn Error>>
    where
        E: Error + Send + Sync + 'static,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut last_error: Option<E> = None;

        for attempt in 1..=3 {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    warn!(
                        operation = operation_name,
                        attempt,
                        error = %err,
                        "Kubernetes API call failed"
                    );
                    last_error = Some(err);

                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        if let Some(err) = last_error {
            Err(Box::new(err))
        } else {
            Err(Box::new(std::io::Error::other(format!(
                "{operation_name} failed without any attempts"
            ))))
        }
    }

    /// Create a NodePort Service targeting the already-bound relay port.
    pub async fn new(relay_port: u16) -> Result<Self, Box<dyn Error>> {
        let pod_name = std::env::var("POD_NAME")?;
        let pod_uid = std::env::var("POD_UID")?;
        let namespace = std::env::var("POD_NAMESPACE")?;
        let node_name = std::env::var("K8S_NODE_NAME")?;
        let service_name = format!("mars-callback-{pod_name}");

        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        let services: Api<Service> = Api::namespaced(client, &namespace);

        let label_patch = serde_json::json!({
            "metadata": {
                "labels": {
                    "polytope-server/mars-callback": pod_name.as_str()
                }
            }
        });

        Self::retry_api_call(
            || async {
                pods.patch(
                    &pod_name,
                    &PatchParams::default(),
                    &Patch::Merge(&label_patch),
                )
                .await
            },
            "patch pod labels",
        )
        .await?;

        let service = Service {
            metadata: ObjectMeta {
                name: Some(service_name.clone()),
                namespace: Some(namespace.clone()),
                owner_references: Some(vec![OwnerReference {
                    api_version: "v1".to_string(),
                    kind: "Pod".to_string(),
                    name: pod_name.clone(),
                    uid: pod_uid.clone(),
                    controller: Some(true),
                    block_owner_deletion: Some(true),
                }]),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("NodePort".to_string()),
                selector: Some(BTreeMap::from([(
                    "polytope-server/mars-callback".to_string(),
                    pod_name.clone(),
                )])),
                ports: Some(vec![ServicePort {
                    name: Some("mars-dhs-callback".to_string()),
                    port: relay_port as i32,
                    target_port: Some(IntOrString::Int(relay_port as i32)),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let mut created_service = match services.create(&PostParams::default(), &service).await {
            Ok(svc) => svc,
            Err(kube::Error::Api(ref resp)) if resp.code == 409 => {
                info!(
                    service_name = %service_name,
                    "NodePort service already exists, reconciling"
                );
                services.get(&service_name).await?
            }
            Err(e) => return Err(Box::new(e)),
        };

        let existing_node_port = callback_node_port(&created_service);

        // A Service owned by a restarted Pod may still carry the former fixed
        // targetPort or stale ownership/selector metadata. Reconcile the full
        // routing contract while preserving Kubernetes' allocated NodePort.
        let mut callback_port = serde_json::json!({
            "name": "mars-dhs-callback",
            "port": relay_port,
            "targetPort": relay_port,
            "protocol": "TCP"
        });
        if let Some(node_port) = existing_node_port {
            callback_port["nodePort"] = serde_json::json!(node_port);
        }
        let service_patch = serde_json::json!({
            "metadata": {
                "ownerReferences": [{
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": pod_name,
                    "uid": pod_uid,
                    "controller": true,
                    "blockOwnerDeletion": true
                }]
            },
            "spec": {
                "type": "NodePort",
                "selector": {
                    "polytope-server/mars-callback": pod_name
                },
                "ports": [callback_port]
            }
        });
        created_service = Self::retry_api_call(
            || async {
                services
                    .patch(
                        &service_name,
                        &PatchParams::default(),
                        &Patch::Merge(&service_patch),
                    )
                    .await
            },
            "reconcile NodePort relay target",
        )
        .await?;

        let node_port_i32 = callback_node_port(&created_service)
            .ok_or_else(|| std::io::Error::other("NodePort not assigned after reconciliation"))?;
        let node_port = u16::try_from(node_port_i32).map_err(|_| {
            std::io::Error::other(format!(
                "Assigned NodePort {node_port_i32} is outside u16 range"
            ))
        })?;

        info!(
            pod_name = %pod_name,
            service_name = %service_name,
            namespace = %namespace,
            node_port,
            relay_port,
            "Allocated NodePort service for Mars callback relay"
        );

        Ok(Self {
            node_port,
            node_name,
            namespace,
            service_name,
            relay_port,
        })
    }

    /// Get the allocated NodePort
    pub fn node_port(&self) -> u16 {
        self.node_port
    }

    /// Get the Pod relay port targeted by the Service.
    pub fn relay_port(&self) -> u16 {
        self.relay_port
    }

    /// Get the node name
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Cleanup the NodePort allocation
    pub async fn cleanup(&self) -> Result<(), Box<dyn Error>> {
        let client = Client::try_default().await?;
        let services: Api<Service> = Api::namespaced(client, &self.namespace);

        Self::retry_api_call(
            || async {
                services
                    .delete(&self.service_name, &DeleteParams::default())
                    .await
                    .map(|_| ())
            },
            "delete NodePort service",
        )
        .await?;

        info!(
            service_name = %self.service_name,
            namespace = %self.namespace,
            "Deleted NodePort service during cleanup"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_node_port_uses_named_port_when_ports_are_reordered() {
        let service = Service {
            spec: Some(ServiceSpec {
                ports: Some(vec![
                    ServicePort {
                        name: Some("unrelated".to_string()),
                        node_port: Some(31_001),
                        ..Default::default()
                    },
                    ServicePort {
                        name: Some("mars-dhs-callback".to_string()),
                        node_port: Some(31_002),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(callback_node_port(&service), Some(31_002));
    }

    #[test]
    fn callback_node_port_handles_missing_assignment() {
        let service = Service {
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    name: Some("mars-dhs-callback".to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(callback_node_port(&service), None);
    }
}
