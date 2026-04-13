use anyhow::Result;
use std::collections::HashSet;
use std::time::Duration;

/// Forwards unsupported RPC methods (sendTransaction, simulateTransaction)
/// to an upstream Solana validator RPC.
#[derive(Clone)]
pub struct UpstreamForwarder {
    client: reqwest::Client,
    endpoint: String,
    forwarded_methods: HashSet<String>,
}

impl UpstreamForwarder {
    pub fn new(endpoint: String, forwarded_methods: Vec<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("building http client");

        Self {
            client,
            endpoint,
            forwarded_methods: forwarded_methods.into_iter().collect(),
        }
    }

    pub fn should_forward(&self, method: &str) -> bool {
        self.forwarded_methods.contains(method)
    }

    pub async fn forward(&self, body: &str) -> Result<String> {
        let response = self
            .client
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await?
            .text()
            .await?;

        Ok(response)
    }
}
