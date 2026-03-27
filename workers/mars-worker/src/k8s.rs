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

/// Manages NodePort allocation and cleanup for Mars worker instances
pub struct NodePortManager {
    node_port: u16,
    node_name: String,
    namespace: String,
    service_name: String,
    local_port: u16,
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
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("{operation_name} failed without any attempts"),
            )))
        }
    }

    /// Create a new NodePortManager for the given local port
    pub async fn new(local_port: u16) -> Result<Self, Box<dyn Error>> {
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
                    uid: pod_uid,
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
                    port: local_port as i32,
                    target_port: Some(IntOrString::Int(local_port as i32)),
                    protocol: Some("TCP".to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let created_service = match services.create(&PostParams::default(), &service).await {
            Ok(svc) => svc,
            Err(kube::Error::Api(ref resp)) if resp.code == 409 => {
                info!(
                    service_name = %service_name,
                    "NodePort service already exists, reusing"
                );
                services.get(&service_name).await?
            }
            Err(e) => return Err(Box::new(e)),
        };

        let node_port_i32 = created_service
            .spec
            .as_ref()
            .and_then(|spec| spec.ports.as_ref())
            .and_then(|ports| ports.first())
            .and_then(|port| port.node_port)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "NodePort not assigned by Kubernetes",
                )
            })?;
        let node_port = u16::try_from(node_port_i32).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Assigned NodePort {node_port_i32} is outside u16 range"),
            )
        })?;

        info!(
            pod_name = %pod_name,
            service_name = %service_name,
            namespace = %namespace,
            node_port,
            local_port,
            "Allocated NodePort service for Mars callback"
        );

        Ok(Self {
            node_port,
            node_name,
            namespace,
            service_name,
            local_port,
        })
    }

    /// Get the allocated NodePort
    pub fn node_port(&self) -> u16 {
        self.node_port
    }

    /// Get the local port
    pub fn local_port(&self) -> u16 {
        self.local_port
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
