use anyhow::Result;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::BehaviorVersion;
use futures::future::join_all;
use rusqlite::{Connection, params};
use serde_json::Value;

struct LayerInfo {
    repo: String,
    tag: String,
    layer_no: i32,
    layer_hash: String,
    layer_size: i64,
}

#[derive(Clone, Debug)]
pub struct Bootstrapper {
    bucket: String,
    db_path: String,
    batch_size: usize,
    s3_client: S3Client,
}

impl Bootstrapper {
    async fn new(bucket: &str, db_path: &str, batch_size: usize) -> Result<Self> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let s3_client = S3Client::new(&config);

        let scraper = Self {
            bucket: bucket.to_string(),
            db_path: db_path.to_string(),
            batch_size,
            s3_client,
        };

        scraper.setup_db()?;

        tracing::debug!("Database initialized at {db_path}");
        Ok(scraper)
    }

    fn setup_db(&self) -> Result<()> {
        let conn = Connection::open(&self.db_path)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS layers (
                repo TEXT, 
                tag TEXT, 
                layer_no INTEGER, 
                layer_hash TEXT, 
                layer_size INTEGER, 
                PRIMARY KEY (repo, tag, layer_no)
            )",
            [],
        )?;
        Ok(())
    }

    async fn get_sha(&self, repo: &str, tag: &str) -> Option<String> {
        let meta_key = format!(
            "docker/registry/v2/repositories/forge/v1/{repo}/_manifests/tags/{tag}/current/link"
        );

        match self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(meta_key)
            .send()
            .await
        {
            Ok(response) => {
                let body = response.body.collect().await.ok()?;
                let content = String::from_utf8(body.into_bytes().to_vec()).ok()?;
                Some(content.trim().split(':').nth(1)?.to_string())
            }
            Err(e) => {
                tracing::debug!("Error getting sha: {e:?}");
                None
            }
        }
    }

    async fn get_manifest(&self, sha: &str) -> Option<Value> {
        let blob_key = format!("docker/registry/v2/blobs/sha256/{}/{}/data", &sha[..2], sha);

        match self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(blob_key)
            .send()
            .await
        {
            Ok(response) => {
                let body = response.body.collect().await.ok()?;
                let blob_data = String::from_utf8(body.into_bytes().to_vec()).ok()?;
                serde_json::from_str(&blob_data).ok()
            }
            Err(_) => None,
        }
    }

    async fn process_repo_tag(&self, repo: &str, tag: &str) -> Vec<LayerInfo> {
        let mut layer_info = Vec::new();

        match self.get_sha(repo, tag).await {
            Some(sha) => {
                if let Some(manifest) = self.get_manifest(&sha).await {
                    if let Some(layers) = manifest.get("layers").and_then(|l| l.as_array()) {
                        for (i, layer) in layers.iter().enumerate() {
                            if let (Some(digest), Some(size)) = (
                                layer.get("digest").and_then(|d| d.as_str()),
                                layer.get("size").and_then(|s| s.as_i64()),
                            ) {
                                let layer_hash = digest.replace("sha256:", "");
                                layer_info.push(LayerInfo {
                                    repo: repo.to_string(),
                                    tag: tag.to_string(),
                                    layer_no: i as i32,
                                    layer_hash,
                                    layer_size: size,
                                });
                            }
                        }
                    }
                }
            }
            None => {
                tracing::debug!("No sha found! {}", tag.to_string())
            }
        }
        layer_info
    }

    fn save_to_db(&self, layer_data: &[LayerInfo]) -> Result<()> {
        if layer_data.is_empty() {
            return Ok(());
        }
        let mut conn = Connection::open(&self.db_path)?;
        let tx = conn.transaction()?;
        for layer in layer_data {
            tx.execute(
                "INSERT OR REPLACE INTO layers (repo, tag, layer_no, layer_hash, layer_size) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![layer.repo, layer.tag, layer.layer_no, layer.layer_hash, layer.layer_size],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    async fn process_batch(&self, batch: Vec<(String, String)>) -> Result<()> {
        let mut tasks = Vec::new();
        for (repo, tag) in batch {
            let repo_clone = repo.clone();
            let tag_clone = tag.clone();
            let scraper = self.clone();

            let task =
                tokio::spawn(
                    async move { scraper.process_repo_tag(&repo_clone, &tag_clone).await },
                );

            tasks.push(task);
        }
        let mut all_layer_data = Vec::new();
        for task in join_all(tasks).await {
            match task {
                Ok(layer_data) => {
                    all_layer_data.extend(layer_data);
                }
                Err(e) => {
                    tracing::debug!("Task error: {}", e);
                }
            }
        }
        if !all_layer_data.is_empty() {
            if let Err(e) = self.save_to_db(&all_layer_data) {
                tracing::debug!("Database error: {}", e);
            }
        }
        Ok(())
    }
}
