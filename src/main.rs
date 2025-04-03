use axum::http::{HeaderMap, HeaderValue};
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    response::Response,
    routing::get,
};
use serde_json::Value;

use std::net::SocketAddr;
use tokio::net::TcpListener;

mod database;
mod registry;
use crate::registry::Registry;

#[derive(Clone)]
struct AppState {
    registry: Registry,
}

async fn root_handler() -> StatusCode {
    StatusCode::OK
}

fn parse_repo_ref(path: &[&str], delimiter: &str) -> Option<(String, String)> {
    if let Some(delimiter) = path.iter().position(|&s| s == delimiter) {
        if delimiter > 0 && delimiter < path.len() - 1 {
            let name = path[0..delimiter].join("/");
            let reference = path[delimiter + 1].to_string();
            return Some((name, reference));
        }
    }
    None
}

async fn path_handler(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    let segments = path.split('/').collect::<Vec<&str>>();

    if let Some((name, reference)) = parse_repo_ref(&segments, "manifests") {
        tracing::info!("Parsed repo: {}, ref: {}", name, reference);
        match state.registry.get_manifest(&name, &reference).await {
            Ok(manifest_json) => {
                let mut headers = HeaderMap::new();
                match HeaderValue::from_str(manifest_json["mediaType"].as_str().unwrap_or("")) {
                    Ok(header_value) => {
                        headers.insert("Content-Type", header_value);
                    }
                    Err(e) => {
                        tracing::error!("Error setting header: {:?}", e);
                        return Err::<Json<Value>, StatusCode>(StatusCode::INTERNAL_SERVER_ERROR)
                            .into_response();
                    }
                }
                return (headers, Json(manifest_json)).into_response();
            }
            Err(e) => {
                tracing::error!("Error getting manifest: {:?}", e);
                return Err::<Json<Value>, StatusCode>(StatusCode::NOT_FOUND).into_response();
            }
        }
    } else if let Some((name, sha)) = parse_repo_ref(&segments, "blobs") {
        tracing::info!("Parsed repo: {}, sha: {}", name, sha);
        match state.registry.get_blob_redirect(&sha).await {
            Ok(blob_redirect) => {
                let mut headers = HeaderMap::new();
                match HeaderValue::from_str(&blob_redirect) {
                    Ok(header_value) => {
                        headers.insert("Location", header_value);
                    }
                    Err(e) => {
                        tracing::error!("Error setting header: {:?}", e);
                        return Err::<Json<Value>, StatusCode>(StatusCode::INTERNAL_SERVER_ERROR)
                            .into_response();
                    }
                }
                tracing::info!("Blob redirect: {:?}", blob_redirect);
                return (headers, StatusCode::TEMPORARY_REDIRECT).into_response();
            }
            Err(e) => {
                tracing::error!("Error getting blob redirect: {:?}", e);
                return Err::<Json<Value>, StatusCode>(StatusCode::NOT_FOUND).into_response();
            }
        }
    } else if let Some((name, cmd)) = parse_repo_ref(&segments, "tags") {
        tracing::info!("Parsed repo: {}, cmd: {}", name, cmd);
        if cmd != "list" {
            tracing::info!("Unsupported command: {}", cmd);
            return Err::<Json<Value>, StatusCode>(StatusCode::NOT_FOUND).into_response();
        }
        tracing::info!("Listing tags for repo: {}", name);
        match state.registry.list_tags(&name).await {
            Ok(tags) => {
                return Json(tags).into_response();
            }
            Err(e) => {
                tracing::error!("Error listing tags: {:?}", e);
                return Err::<Json<Value>, StatusCode>(StatusCode::NOT_FOUND).into_response();
            }
        }
    }

    tracing::info!("Unsupported path {path}");
    Err::<Json<Value>, StatusCode>(StatusCode::NOT_FOUND).into_response()
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

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <bucket-name> <db-path>", args[0]);
        std::process::exit(1);
    }
    let bucket_name = &args[1];
    let db_path = &args[2];

    let state = AppState {
        registry: Registry::new(bucket_name, db_path).await?,
    };

    let app = Router::new()
        .route("/v2/", get(root_handler))
        .route("/v2/{*path}", get(path_handler))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 2137));
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
