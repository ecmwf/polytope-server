use crate::delivery_config::DeliveryConfig;

pub struct WorkerConfigFile {
    pub raw: serde_yml::Value,
    pub delivery: DeliveryConfig,
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

        Ok(Self { raw, delivery })
    }

    pub fn section(&self, key: &str) -> Option<&serde_yml::Value> {
        self.raw.get(key)
    }
}
