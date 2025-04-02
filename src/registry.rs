use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::BehaviorVersion;
use aws_sdk_s3::presigning::PresigningConfig;
use rusqlite::Connection;
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct Registry {
    bucket: String,
    db_path: String,
    s3_client: S3Client,
}

impl Registry {
    pub async fn new(bucket: &str, db_path: &str) -> Result<Self> {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let s3_client = S3Client::new(&config);

        let registry = Self {
            bucket: bucket.to_string(),
            db_path: db_path.to_string(),
            s3_client,
        };

        registry.setup_db()?;

        tracing::info!("Database initialized at {db_path}");
        Ok(registry)
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

    async fn get_sha(&self, repo: &str, tag: &str) -> Result<String> {
        let meta_key =
            format!("docker/registry/v2/repositories/{repo}/_manifests/tags/{tag}/current/link");

        match self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(meta_key)
            .send()
            .await
        {
            Ok(response) => {
                let body = response.body.collect().await?;
                let content = String::from_utf8(body.into_bytes().to_vec())?;
                Ok(content
                    .trim()
                    .split(':')
                    .nth(1)
                    .context("incorrect sha format")?
                    .to_string())
            }
            Err(e) => {
                tracing::error!("Error getting sha: {e:?}");
                Err(e.into())
            }
        }
    }

    pub async fn get_manifest(&self, repo: &str, tag: &str) -> Result<Value> {
        let sha = self.get_sha(repo, tag).await?;
        self.get_manifest_from_sha(&sha).await
    }

    pub async fn get_blob_redirect(&self, sha: &str) -> Result<String> {
        let sha = sha.strip_prefix("sha256:").unwrap_or(sha);
        let blob_key = format!("docker/registry/v2/blobs/sha256/{}/{}/data", &sha[..2], sha);
        let presigned_url = self
            .s3_client
            .get_object()
            .bucket(&self.bucket)
            .key(blob_key)
            .presigned(
                PresigningConfig::builder()
                    .expires_in(std::time::Duration::from_secs(60 * 5))
                    .build()
                    .expect("less than one week"),
            )
            .await?
            .uri()
            .to_string();
        Ok(presigned_url)
    }

    async fn get_manifest_from_sha(&self, sha: &str) -> Result<Value> {
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
                let body = response.body.collect().await?;
                let blob_data = String::from_utf8(body.into_bytes().to_vec())?;
                Ok(serde_json::from_str(&blob_data)?)
            }
            Err(e) => {
                tracing::error!("Error getting manifest: {e:?}");
                Err(e.into())
            }
        }
    }

    /*
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
    */
}
