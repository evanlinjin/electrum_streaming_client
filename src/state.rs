//! State management for Electrum client.

use crate::{request::Request, response::Response, *};
use bitcoin::block::Header;
use futures::channel::oneshot;
use notification::Notification;
use std::collections::HashMap;

/// Event emitted by the client.
#[derive(Debug, Clone)]
pub enum Event {
    /// Successful response to a request.
    Response {
        id: u32,
        request: Request,
        response: Response,
    },
    /// Error response to a request.
    ResponseError {
        id: u32,
        request: Request,
        error: ResponseError,
    },
    /// Server-initiated notification.
    Notification(Notification),
}

impl Event {
    pub fn try_to_headers(&self) -> Option<Vec<(u32, Header)>> {
        match self {
            Event::Response {
                response: Response::Header(resp),
                ..
            } => {
                Some(vec![(0, resp.header)]) // Note: height info lost in simplified version
            }
            Event::Response {
                response: Response::HeaderWithProof(resp),
                ..
            } => {
                Some(vec![(0, resp.header)]) // Note: height info lost in simplified version
            }
            Event::Response {
                response: Response::Headers(resp),
                ..
            } => {
                Some((0..).zip(resp.headers.clone()).collect()) // Note: height info lost
            }
            Event::Response {
                response: Response::HeadersWithCheckpoint(resp),
                ..
            } => {
                Some((0..).zip(resp.headers.clone()).collect()) // Note: height info lost
            }
            Event::Notification(Notification::Header(n)) => Some(vec![(n.height(), *n.header())]),
            _ => None,
        }
    }
}

pub struct PendingRequest {
    pub request: Request,
    pub response_tx: Option<oneshot::Sender<Result<Response, ResponseError>>>,
}

pub struct State {
    pending_requests: HashMap<u32, PendingRequest>,
}

impl State {
    pub fn new() -> Self {
        Self {
            pending_requests: HashMap::new(),
        }
    }

    pub fn track_request(
        &mut self,
        id: u32,
        request: Request,
        response_tx: Option<oneshot::Sender<Result<Response, ResponseError>>>,
    ) -> RawRequest {
        self.pending_requests.insert(
            id,
            PendingRequest {
                request: request.clone(),
                response_tx,
            },
        );

        let method = request.method_name().to_string();
        let params = request.params();

        RawRequest {
            jsonrpc: JSONRPC_VERSION_2_0.into(),
            id,
            method: method.into(),
            params,
        }
    }

    pub fn handle_response(
        &mut self,
        id: u32,
        result: Result<serde_json::Value, serde_json::Value>,
    ) -> Option<Event> {
        let pending = self.pending_requests.remove(&id)?;

        match result {
            Ok(value) => {
                // Use the JSON-RPC method name directly
                let method_name = pending.request.method_name();

                match Response::from_json(method_name, value) {
                    Ok(response) => {
                        if let Some(tx) = pending.response_tx {
                            let _ = tx.send(Ok(response.clone()));
                            None
                        } else {
                            Some(Event::Response {
                                id,
                                request: pending.request,
                                response,
                            })
                        }
                    }
                    Err(e) => {
                        let error = ResponseError(serde_json::json!({
                            "message": format!("Failed to deserialize response: {}", e)
                        }));
                        if let Some(tx) = pending.response_tx {
                            let _ = tx.send(Err(error));
                            None
                        } else {
                            Some(Event::ResponseError {
                                id,
                                request: pending.request,
                                error,
                            })
                        }
                    }
                }
            }
            Err(error_value) => {
                let error = ResponseError(error_value);
                if let Some(tx) = pending.response_tx {
                    let _ = tx.send(Err(error.clone()));
                    None
                } else {
                    Some(Event::ResponseError {
                        id,
                        request: pending.request,
                        error,
                    })
                }
            }
        }
    }

    pub fn cancel_request(&mut self, id: u32) -> bool {
        self.pending_requests.remove(&id).is_some()
    }
}
