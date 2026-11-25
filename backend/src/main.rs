mod signal;
mod turn;

use std::{env, error::Error, path::PathBuf};

use axum::{Router, routing::get};
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

fn router(state: signal::AppState, static_dir: Option<PathBuf>) -> Router {
    let app = Router::new()
        .route("/api/signal", get(signal::upgrade))
        .with_state(state);
    match static_dir {
        Some(dir) => app.fallback_service(ServeDir::new(dir)),
        None => app,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind_addr = match env::var("BIND_ADDR") {
        Ok(addr) => addr,
        Err(env::VarError::NotPresent) => "127.0.0.1:3001".into(),
        Err(err) => return Err(err.into()),
    };
    let static_dir = env::var_os("STATIC_DIR")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from);
    let share_password = match env::var("SHARE_PASSWORD") {
        Ok(password) => Some(password),
        Err(env::VarError::NotPresent) => None,
        Err(env::VarError::NotUnicode(_)) => {
            return Err("SHARE_PASSWORD must be valid UTF-8".into());
        }
    };
    let state = signal::AppState::new(turn::Turn::from_env()?, share_password.as_deref());
    drop(share_password);
    let app = router(state, static_dir);
    let listener = TcpListener::bind(bind_addr).await?;
    eprintln!("Listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests;
