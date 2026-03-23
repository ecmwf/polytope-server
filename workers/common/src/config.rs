use crate::delivery_config::DeliveryConfig;

pub const DEFAULT_CONFIG_PATH: &str = "/etc/worker/config.yaml";

pub struct WorkerConfigFile {
    pub raw: serde_yml::Value,
    pub delivery: DeliveryConfig,
    pub management_port: u16,
}

impl WorkerConfigFile {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = std::fs::read_to_string(path)?;
        let raw: serde_yml::Value = serde_yml::from_str(&contents)?;

        let delivery_value = raw
            .get("delivery")
            .cloned()
            .unwrap_or_else(|| serde_yml::Value::Mapping(Default::default()));

        let delivery = DeliveryConfig::from_value(delivery_value)?;

        let management_port = raw
            .get("management")
            .and_then(|m| m.get("port"))
            .and_then(|p| p.as_u64())
            .unwrap_or(9090) as u16;

        Ok(Self {
            raw,
            delivery,
            management_port,
        })
    }

    pub fn section(&self, key: &str) -> Option<&serde_yml::Value> {
        self.raw.get(key)
    }
}
