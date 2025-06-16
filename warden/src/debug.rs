use crate::assignment_computation::{
    AssignmentComputationImpl, LoadedRangeStatus, PrimaryRangeStatus, RangeStatus,
    SecondaryRangeStatus,
};
use axum::{extract::State, routing::get, Json, Router};
use serde::Serialize;
use std::sync::Arc;

pub struct DebugServer {
    assignment_computation: Arc<AssignmentComputationImpl>,
}

#[derive(Debug, Serialize)]
struct DebugResponse {
    primary_range_statuses: Vec<PrimaryRangeStatus>,
    secondary_range_statuses: Vec<SecondaryRangeStatus>,
}

impl DebugServer {
    pub fn new(assignment_computation: Arc<AssignmentComputationImpl>) -> Arc<Self> {
        Arc::new(Self {
            assignment_computation,
        })
    }

    async fn debug_handler(State(state): State<Arc<DebugServer>>) -> Json<DebugResponse> {
        let range_statuses = state.assignment_computation.range_statuses.read().unwrap();
        let mut primary_range_statuses = Vec::new();
        let mut secondary_range_statuses = Vec::new();
        for (_, range_status) in range_statuses.iter() {
            match range_status {
                RangeStatus::Loaded(LoadedRangeStatus::Primary(primary_range_status)) => {
                    primary_range_statuses.push(primary_range_status.clone());
                }
                RangeStatus::Loaded(LoadedRangeStatus::Secondary(secondary_range_status)) => {
                    secondary_range_statuses.push(secondary_range_status.clone());
                }
                _ => {}
            }
        }
        Json(DebugResponse {
            primary_range_statuses,
            secondary_range_statuses,
        })
    }

    pub fn router(self: Arc<Self>) -> Router {
        let shared_state = self;
        Router::new()
            .route("/debug", get(Self::debug_handler))
            .with_state(shared_state)
    }
}

pub async fn run_debug_server(
    addr: String,
    assignment_computation: Arc<AssignmentComputationImpl>,
) -> Result<(), Box<dyn std::error::Error>> {
    let debug_server = DebugServer::new(assignment_computation);
    let app = debug_server.router();
    axum::Server::bind(&addr.parse()?)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}
