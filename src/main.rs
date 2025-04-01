use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    http::header::{HeaderMap, HeaderValue},
    routing::{delete, get, head, patch, post, put},
};
use std::{collections::HashMap, net::SocketAddr};
use tokio::net::TcpListener;

mod bootstrap;
use crate::bootstrap::Bootstrapper;

#[derive(Clone, Default)]
struct AppState {}

async fn root_handler() -> StatusCode {
    tracing::info!("Root endpoint accessed");
    StatusCode::OK
}

async fn get_blob_handler(
    State(_state): State<AppState>,
    Path((name, digest)): Path<(String, String)>,
) -> StatusCode {
    tracing::info!("Get blob: {name} with digest: {digest}");
    StatusCode::OK
}

async fn head_manifest_handler(
    State(_state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
) -> StatusCode {
    tracing::info!("Head manifest: {name} with reference: {reference}");
    StatusCode::OK
}

async fn get_manifest_handler(
    State(_state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
) -> Result<(HeaderMap, Json<serde_json::Value>), StatusCode> {
    tracing::info!("Get manifest: {name} with reference: {reference}");
    let json_manifest = serde_json::json!({
       "schemaVersion": 2,
       "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
       "config": {
          "mediaType": "application/vnd.docker.container.image.v1+json",
          "size": 1462,
          "digest": "sha256:9b47f4deebb8cd63abd99322ccc30fa4ff89cd1ed7c07a7c06a847114342140c"
       },
       "layers": [
          {
             "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
             "size": 49552605,
             "digest": "sha256:7bb465c2914923b08ae03b7fc67b92a1ef9b09c4c1eb9d6711b22ee6bbb46a00"
          },
          {
             "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
             "size": 19843689,
             "digest": "sha256:680c204119455e95f2c3e8dc517c6186de81657f6fadf711ae8a02fe13254aa2"
          },
          {
             "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
             "size": 122745,
             "digest": "sha256:471d5b6e12ac12d19bfaaebde15c10a2b5b5cef56f58262a1f76189b436543b1"
          },
          {
             "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
             "size": 84869603,
             "digest": "sha256:59578d707313ffdc49a62ed54492c049c87f57211ca9adc212afe5c713bb57e5"
          }
       ]
    });

    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/vnd.docker.distribution.manifest.v2+json"),
    );

    Result::Ok((headers, Json(json_manifest)))
}

async fn initiate_upload_handler(
    State(_state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> StatusCode {
    tracing::info!("Initiating upload for: {name} {params:?}");
    StatusCode::ACCEPTED
}

async fn upload_chunk_handler(
    State(_state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
) -> StatusCode {
    tracing::info!("Uploading chunk for: {name} with reference: {reference}");
    StatusCode::ACCEPTED
}

async fn complete_upload_handler(
    State(_state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
    Query(_params): Query<HashMap<String, String>>,
) -> StatusCode {
    tracing::info!("Completing upload for: {name} with reference: {reference}");
    StatusCode::CREATED
}

async fn put_manifest_handler(
    State(_state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
) -> StatusCode {
    tracing::info!("Putting manifest for: {name} with reference: {reference}");
    StatusCode::CREATED
}

async fn list_tags_handler(
    State(_state): State<AppState>,
    Path(name): Path<String>,
    Query(_params): Query<HashMap<String, String>>,
) -> StatusCode {
    tracing::info!("Listing tags for: {name}");
    StatusCode::OK
}

async fn delete_manifest_handler(
    State(_state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
) -> StatusCode {
    tracing::info!("Deleting manifest for: {name} with reference: {reference}");
    StatusCode::ACCEPTED
}

async fn delete_blob_handler(
    State(_state): State<AppState>,
    Path((name, digest)): Path<(String, String)>,
) -> StatusCode {
    tracing::info!("Deleting blob: {name} with digest: {digest}");
    StatusCode::ACCEPTED
}

async fn get_referrers_handler(
    State(_state): State<AppState>,
    Path((name, digest)): Path<(String, String)>,
    Query(_params): Query<HashMap<String, String>>,
) -> StatusCode {
    tracing::info!("Getting referrers for: {name} with digest: {digest}");
    StatusCode::OK
}

async fn get_upload_status_handler(
    State(_state): State<AppState>,
    Path((name, reference)): Path<(String, String)>,
) -> StatusCode {
    tracing::info!("Getting upload status for: {name} with reference: {reference}");
    StatusCode::NO_CONTENT
}

// Grand idea:
// 1. reading manifests and blobs is compatible with docker distribution does
// 2. on top of that, if you run with --bootstrap-db, it will first scan
//    the whole bucket and put all manifest info into SQLite.
// 3. then, it will treat SQLite as the first source of truth.
// 4. then, additional features include:
//    - listing all tags for a repo
//    - listing all blobs for a repo
//    - listing all manifests for a repo
//    - listing all repos
//    - listing all layers for a repo
//    - etc

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();
    let state = AppState::default();

    let app = Router::new()
        .route("/v2/", get(root_handler))
        .route("/v2/{name}/blobs/{digest}", get(get_blob_handler))
        .route("/v2/{name}/blobs/{digest}", head(get_blob_handler))
        .route(
            "/v2/{name}/manifests/{reference}",
            get(get_manifest_handler),
        )
        .route(
            "/v2/{name}/manifests/{reference}",
            head(head_manifest_handler),
        )
        .route(
            "/v2/{name}/manifests/{reference}",
            put(put_manifest_handler),
        )
        .route(
            "/v2/{name}/manifests/{reference}",
            delete(delete_manifest_handler),
        )
        .route("/v2/{name}/blobs/uploads/", post(initiate_upload_handler))
        .route(
            "/v2/{name}/blobs/uploads/{reference}",
            patch(upload_chunk_handler),
        )
        .route(
            "/v2/{name}/blobs/uploads/{reference}",
            put(complete_upload_handler),
        )
        .route(
            "/v2/{name}/blobs/uploads/{reference}",
            get(get_upload_status_handler),
        )
        .route("/v2/{name}/blobs/{digest}", delete(delete_blob_handler))
        .route("/v2/{name}/tags/list", get(list_tags_handler))
        .route("/v2/{name}/referrers/{digest}", get(get_referrers_handler))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
