use leptos::config::get_configuration;
use leptos_wasi::executor::init_wasip3_spawner;
use leptos_wasi::prelude::Handler;
use wasip3::http::types::{Request, Response, ErrorCode};

use crate::app::{shell, App, GetCounterView, IncrementCount, DecrementCount, ResetCount};

struct LeptosServer;

impl wasip3::exports::http::handler::Guest for LeptosServer {
    async fn handle(request: Request) -> Result<Response, ErrorCode> {
        // 1. Initialize host async task scheduling
        let _ = init_wasip3_spawner();

        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();

        // Convert the WASI request to http::Request before storage work so
        // static assets do not trigger remote schema checks.
        let req = wasip3::http_compat::http_from_wasi_request(request)?;
        let request_path = req.uri().path().to_string();

        // Store-level initialization is guarded by an async lock, so concurrent
        // first requests do not run migrations more than once.
        if !request_path.starts_with("/pkg/") {
            if let Err(e) = crate::store::initialize_schema_async().await {
                eprintln!("Error executing boot schema migrations: {:?}", e);
                return Err(ErrorCode::InternalError(None));
            }
        }

        if request_path == "/api/counter/stream" {
            let response = crate::store::counter_stream_response(&req)
                .await
                .map_err(|e| {
                    eprintln!("Error building counter stream response: {:?}", e);
                    ErrorCode::InternalError(None)
                })?;
            return wasip3::http_compat::http_into_wasi_response(response);
        }

        let conf = get_configuration(None).unwrap();
        let leptos_options = conf.leptos_options;

        // 2. Build and handle request natively
        let wasi_res = Handler::build(req).await
            .map_err(|e| {
                eprintln!("Error building handler: {:?}", e);
                ErrorCode::InternalError(None)
            })?
            .static_files_handler("/pkg", serve_static_files)
            .with_server_fn::<GetCounterView>()
            .with_server_fn::<IncrementCount>()
            .with_server_fn::<DecrementCount>()
            .with_server_fn::<ResetCount>()
            .generate_routes(App)
            .handle_with_context(move || shell(leptos_options.clone()), || {})
            .await
            .map_err(|e| {
                eprintln!("Error handling request: {:?}", e);
                ErrorCode::InternalError(None)
            })?;

        Ok(wasi_res)
    }
}

fn serve_static_files(path: String) -> Option<leptos_wasi::response::Body> {
    use std::fs;
    let path = path.strip_prefix("/").unwrap_or(&path);
    // Wasmtime mounts site directory at root, so look at /path directly
    let file_path = format!("/{}", path);
    println!("serving static file: {}", file_path);

    if let Ok(bytes) = fs::read(&file_path) {
        Some(leptos_wasi::response::Body::Sync(bytes.into()))
    } else {
        println!("Could not read file at {}", file_path);
        None
    }
}

// Export the server for standard WASIp3 http trigger
wasip3::http::service::export!(LeptosServer);
