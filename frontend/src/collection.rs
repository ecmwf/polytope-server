/// Routes requests to the correct pipeline branch based on the `collection`
/// metadata label set by the frontend.
///
/// # How it works
///
/// The v1 API path `/api/v1/requests/:collection` extracts the collection name
/// from the URL and stores it in `job.metadata["collection"]`.  Each route in
/// the bits config can include a `Collection` check with a specific name:
///
/// ```yaml
/// routes:
///   climate-dt:
///     - type: collection
///       name: climate-dt
///     - type: ...
/// ```
///
/// **Evaluation rules:**
///
/// | `job.metadata["collection"]` | Action config `name` | Result |
/// |------------------------------|----------------------|--------|
/// | absent                       | _any_                | Pass — allows unconstrained requests through |
/// | `"climate-dt"`               | `"climate-dt"`       | Pass — collection matches |
/// | `"reanalysis"`               | `"climate-dt"`       | Reject (silent) — tries next route |
///
/// Silent rejection means this check participates in route _selection_, not
/// validation.  When no route matches the collection, the switch's own
/// "no route matched" error is surfaced instead of individual rejection reasons.
///
/// # Introspection
///
/// `describe()` returns `{"collection": "<name>"}`, which the frontend uses to
/// populate the `list_collections` endpoint by walking all instantiated actions
/// via `Bits::describe_actions()`.
use async_trait::async_trait;
use bits::actions::{ActionError, CheckAction, CheckResult};
use bits::Job;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct Collection {
    pub name: String,
}

#[async_trait]
impl CheckAction for Collection {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        match job.metadata.get("collection").and_then(|v| v.as_str()) {
            None => Ok(CheckResult::Pass),
            Some(c) if c == self.name => Ok(CheckResult::Pass),
            Some(c) => Ok(CheckResult::Reject {
                reason: format!(
                    "collection '{}' does not match route collection '{}'",
                    c, self.name
                ),
                silent: true,
            }),
        }
    }

    fn describe(&self) -> Value {
        serde_json::json!({"collection": self.name})
    }
}

bits::register_action!(check, "collection", Collection);
