//! Async and blocking Electrum client implementations.

use crate::notification::Notification;
use crate::*;
use futures::channel::{mpsc, oneshot};
use futures::StreamExt;

/// Error type for client operations.
#[derive(Debug)]
pub enum ClientError {
    /// Failed to send request to the client task.
    SendError,
    /// Server returned an error response.
    ServerError(ResponseError),
    /// Request was cancelled or receiver dropped.
    Cancelled,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::SendError => write!(f, "Failed to send request"),
            ClientError::ServerError(e) => write!(f, "Server error: {:?}", e),
            ClientError::Cancelled => write!(f, "Request cancelled"),
        }
    }
}

impl std::error::Error for ClientError {}

/// Async Electrum client for non-blocking operations.
#[derive(Debug, Clone)]
pub struct AsyncClient {
    tx: mpsc::UnboundedSender<(
        Request,
        Option<oneshot::Sender<Result<Response, ResponseError>>>,
    )>,
}

impl AsyncClient {
    /// Create a new async client from reader and writer streams.
    ///
    /// Returns a tuple of (client, event_receiver, runner_future).
    /// The runner future must be spawned to process I/O.
    pub fn new<R, W>(
        reader: R,
        mut writer: W,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<Event>,
        impl std::future::Future<Output = std::io::Result<()>> + Send,
    )
    where
        R: futures::AsyncRead + Send + Unpin,
        W: futures::AsyncWrite + Send + Unpin,
    {
        let (event_tx, event_recv) = mpsc::unbounded::<Event>();
        let (req_tx, mut req_recv) = mpsc::unbounded::<(
            Request,
            Option<oneshot::Sender<Result<Response, ResponseError>>>,
        )>();

        let mut incoming_stream =
            crate::io::ReadStreamer::new(futures::io::BufReader::new(reader)).fuse();
        let mut state = State::new();
        let mut next_id = 0_u32;

        let fut = async move {
            loop {
                futures::select! {
                    req_opt = req_recv.next() => match req_opt {
                        Some((req, resp_tx)) => {
                            let raw_req = state.track_request(next_id, req, resp_tx);
                            next_id = next_id.wrapping_add(1);
                            crate::io::async_write(&mut writer, MaybeBatch::Single(raw_req)).await?;
                        },
                        None => break,
                    },
                    msg_opt = incoming_stream.next() => match msg_opt {
                        Some(Ok(msg)) => match msg {
                            RawNotificationOrResponse::Response(raw_resp) => {
                                if let Some(event) = state.handle_response(raw_resp.id, raw_resp.result) {
                                    let _ = event_tx.unbounded_send(event);
                                }
                            }
                            RawNotificationOrResponse::Notification(raw_notif) => {
                                if let Ok(notif) = Notification::new(&raw_notif) {
                                    let _ = event_tx.unbounded_send(Event::Notification(notif));
                                }
                            }
                        },
                        Some(Err(e)) => return Err(e),
                        None => break,
                    }
                }
            }
            Ok(())
        };

        (AsyncClient { tx: req_tx }, event_recv, fut)
    }

    /// Create a new async client from Tokio streams.
    #[cfg(feature = "tokio")]
    pub fn new_tokio<R, W>(
        reader: R,
        writer: W,
    ) -> (
        Self,
        mpsc::UnboundedReceiver<Event>,
        impl std::future::Future<Output = std::io::Result<()>> + Send,
    )
    where
        R: tokio::io::AsyncRead + Send + Unpin,
        W: tokio::io::AsyncWrite + Send + Unpin,
    {
        use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
        Self::new(reader.compat(), writer.compat_write())
    }

    /// Get a block header by height.
    pub async fn header(&self, height: u32) -> Result<response::HeaderResp, ClientError> {
        let req = Request::Header {
            height,
            cp_height: None,
        };
        self.send_request(req).await.map(|resp| match resp {
            Response::Header(h) => h,
            _ => unreachable!(),
        })
    }

    /// Get a block header with Merkle proof.
    pub async fn header_with_proof(
        &self,
        height: u32,
        cp_height: u32,
    ) -> Result<response::HeaderWithProofResp, ClientError> {
        let req = Request::Header {
            height,
            cp_height: Some(cp_height),
        };
        self.send_request(req).await.map(|resp| match resp {
            Response::HeaderWithProof(h) => h,
            _ => unreachable!(),
        })
    }

    /// Get multiple consecutive block headers.
    pub async fn headers(
        &self,
        start_height: u32,
        count: usize,
    ) -> Result<response::HeadersResp, ClientError> {
        let req = Request::Headers {
            start_height,
            count,
            cp_height: None,
        };
        self.send_request(req).await.map(|resp| match resp {
            Response::Headers(h) => h,
            _ => unreachable!(),
        })
    }

    /// Get headers with checkpoint proof.
    pub async fn headers_with_checkpoint(
        &self,
        start_height: u32,
        count: usize,
        cp_height: u32,
    ) -> Result<response::HeadersWithCheckpointResp, ClientError> {
        let req = Request::Headers {
            start_height,
            count,
            cp_height: Some(cp_height),
        };
        self.send_request(req).await.map(|resp| match resp {
            Response::HeadersWithCheckpoint(h) => h,
            _ => unreachable!(),
        })
    }

    /// Estimate fee for confirmation within target blocks.
    pub async fn estimate_fee(
        &self,
        number: usize,
    ) -> Result<response::EstimateFeeResp, ClientError> {
        let req = Request::EstimateFee { number };
        self.send_request(req).await.map(|resp| match resp {
            Response::EstimateFee(e) => e,
            _ => unreachable!(),
        })
    }

    /// Subscribe to new block headers.
    pub async fn headers_subscribe(&self) -> Result<response::HeadersSubscribeResp, ClientError> {
        let req = Request::HeadersSubscribe;
        self.send_request(req).await.map(|resp| match resp {
            Response::HeadersSubscribe(h) => h,
            _ => unreachable!(),
        })
    }

    /// Get minimum relay fee.
    pub async fn relay_fee(&self) -> Result<response::RelayFeeResp, ClientError> {
        let req = Request::RelayFee;
        self.send_request(req).await.map(|resp| match resp {
            Response::RelayFee(r) => r,
            _ => unreachable!(),
        })
    }

    /// Get confirmed and unconfirmed balance.
    pub async fn get_balance(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<response::GetBalanceResp, ClientError> {
        let req = Request::GetBalance { script_hash };
        self.send_request(req).await.map(|resp| match resp {
            Response::GetBalance(b) => b,
            _ => unreachable!(),
        })
    }

    /// Get transaction history for a script.
    pub async fn get_history(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<Vec<response::Tx>, ClientError> {
        let req = Request::GetHistory { script_hash };
        self.send_request(req).await.map(|resp| match resp {
            Response::GetHistory(h) => h,
            _ => unreachable!(),
        })
    }

    /// Get mempool transactions for a script.
    pub async fn get_mempool(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<Vec<response::MempoolTx>, ClientError> {
        let req = Request::GetMempool { script_hash };
        self.send_request(req).await.map(|resp| match resp {
            Response::GetMempool(m) => m,
            _ => unreachable!(),
        })
    }

    /// List unspent outputs for a script.
    pub async fn list_unspent(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<Vec<response::Utxo>, ClientError> {
        let req = Request::ListUnspent { script_hash };
        self.send_request(req).await.map(|resp| match resp {
            Response::ListUnspent(u) => u,
            _ => unreachable!(),
        })
    }

    /// Subscribe to script status changes.
    pub async fn script_hash_subscribe(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<Option<ElectrumScriptStatus>, ClientError> {
        let req = Request::ScriptHashSubscribe { script_hash };
        self.send_request(req).await.map(|resp| match resp {
            Response::ScriptHashSubscribe(s) => s,
            _ => unreachable!(),
        })
    }

    /// Unsubscribe from script status changes.
    pub async fn script_hash_unsubscribe(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<bool, ClientError> {
        let req = Request::ScriptHashUnsubscribe { script_hash };
        self.send_request(req).await.map(|resp| match resp {
            Response::ScriptHashUnsubscribe(s) => s,
            _ => unreachable!(),
        })
    }

    /// Broadcast a transaction.
    pub async fn broadcast_tx(
        &self,
        tx: bitcoin::Transaction,
    ) -> Result<bitcoin::Txid, ClientError> {
        let req = Request::BroadcastTx(tx);
        self.send_request(req).await.map(|resp| match resp {
            Response::BroadcastTx(txid) => txid,
            _ => unreachable!(),
        })
    }

    /// Get a transaction by ID.
    pub async fn get_tx(&self, txid: bitcoin::Txid) -> Result<response::FullTx, ClientError> {
        let req = Request::GetTx { txid };
        self.send_request(req).await.map(|resp| match resp {
            Response::GetTx(tx) => tx,
            _ => unreachable!(),
        })
    }

    /// Get Merkle proof for a transaction.
    pub async fn get_tx_merkle(
        &self,
        txid: bitcoin::Txid,
        height: u32,
    ) -> Result<response::TxMerkle, ClientError> {
        let req = Request::GetTxMerkle { txid, height };
        self.send_request(req).await.map(|resp| match resp {
            Response::GetTxMerkle(m) => m,
            _ => unreachable!(),
        })
    }

    /// Get transaction ID from block position.
    pub async fn get_txid_from_pos(
        &self,
        height: u32,
        tx_pos: usize,
    ) -> Result<response::TxidFromPos, ClientError> {
        let req = Request::GetTxidFromPos { height, tx_pos };
        self.send_request(req).await.map(|resp| match resp {
            Response::GetTxidFromPos(t) => t,
            _ => unreachable!(),
        })
    }

    /// Get mempool fee histogram.
    pub async fn get_fee_histogram(&self) -> Result<Vec<response::FeePair>, ClientError> {
        let req = Request::GetFeeHistogram;
        self.send_request(req).await.map(|resp| match resp {
            Response::GetFeeHistogram(f) => f,
            _ => unreachable!(),
        })
    }

    /// Get server banner.
    pub async fn banner(&self) -> Result<String, ClientError> {
        let req = Request::Banner;
        self.send_request(req).await.map(|resp| match resp {
            Response::Banner(b) => b,
            _ => unreachable!(),
        })
    }

    /// Get server features.
    pub async fn features(&self) -> Result<response::ServerFeatures, ClientError> {
        let req = Request::Features;
        self.send_request(req).await.map(|resp| match resp {
            Response::Features(f) => f,
            _ => unreachable!(),
        })
    }

    /// Ping the server.
    pub async fn ping(&self) -> Result<(), ClientError> {
        let req = Request::Ping;
        self.send_request(req).await.map(|resp| match resp {
            Response::Ping => (),
            _ => unreachable!(),
        })
    }

    /// Send a custom request.
    pub async fn custom(
        &self,
        method: CowStr,
        params: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, ClientError> {
        let req = Request::Custom(request::Custom { method, params });
        self.send_request(req).await.map(|resp| match resp {
            Response::Custom(v) => v,
            _ => unreachable!(),
        })
    }

    async fn send_request(&self, req: Request) -> Result<Response, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .unbounded_send((req, Some(tx)))
            .map_err(|_| ClientError::SendError)?;

        match rx.await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(ClientError::ServerError(e)),
            Err(_) => Err(ClientError::Cancelled),
        }
    }

    /// Send a request without waiting for response (for subscriptions).
    pub fn send_event_request(&self, req: Request) -> Result<(), ClientError> {
        self.tx
            .unbounded_send((req, None))
            .map_err(|_| ClientError::SendError)
    }
}

// Blocking client with proper multi-threading implementation
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};

/// Blocking Electrum client for synchronous operations.
#[derive(Clone)]
pub struct BlockingClient {
    tx: std::sync::mpsc::Sender<(
        Request,
        Option<std::sync::mpsc::SyncSender<Result<Response, ResponseError>>>,
    )>,
}

impl BlockingClient {
    /// Create a new blocking client from reader and writer streams.
    ///
    /// Returns a tuple of (client, event_receiver, join_handle).
    pub fn new<R, W>(
        reader: R,
        mut writer: W,
    ) -> (
        Self,
        std::sync::mpsc::Receiver<Event>,
        std::thread::JoinHandle<std::io::Result<()>>,
    )
    where
        R: std::io::Read + Send + 'static,
        W: std::io::Write + Send + 'static,
    {
        let (event_tx, event_rx) = std::sync::mpsc::channel();
        let (req_tx, req_rx) = std::sync::mpsc::channel::<(
            Request,
            Option<std::sync::mpsc::SyncSender<Result<Response, ResponseError>>>,
        )>();

        // Shared state protected by mutex
        let state = Arc::new(Mutex::new(BlockingState::new()));
        let next_id = Arc::new(Mutex::new(0_u32));

        // Clone for the reader thread
        let state_clone = state.clone();
        let event_tx_clone = event_tx.clone();

        // Reader thread - processes incoming messages
        let reader_handle = std::thread::spawn(move || {
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            loop {
                line.clear();
                match reader.read_line(&mut line) {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        // Try to parse the JSON message
                        if let Ok(msg) = serde_json::from_str::<RawNotificationOrResponse>(&line) {
                            match msg {
                                RawNotificationOrResponse::Response(raw_resp) => {
                                    let mut state = state_clone.lock().unwrap();
                                    if let Some(event) =
                                        state.handle_response(raw_resp.id, raw_resp.result)
                                    {
                                        let _ = event_tx_clone.send(event);
                                    }
                                }
                                RawNotificationOrResponse::Notification(raw_notif) => {
                                    if let Ok(notif) = Notification::new(&raw_notif) {
                                        let _ = event_tx_clone.send(Event::Notification(notif));
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Read error: {}", e);
                        break;
                    }
                }
            }
        });

        // Writer thread - sends requests
        let handle = std::thread::spawn(move || {
            loop {
                match req_rx.recv() {
                    Ok((req, resp_tx)) => {
                        let mut state = state.lock().unwrap();
                        let mut id = next_id.lock().unwrap();

                        let raw_req = state.track_request(*id, req, resp_tx);
                        *id = id.wrapping_add(1);
                        drop(state);
                        drop(id);

                        // Serialize and write the request
                        if let Ok(json) = serde_json::to_string(&raw_req) {
                            if let Err(e) = writeln!(&mut writer, "{}", json) {
                                eprintln!("Write error: {}", e);
                                break;
                            }
                            if let Err(e) = writer.flush() {
                                eprintln!("Flush error: {}", e);
                                break;
                            }
                        }
                    }
                    Err(_) => break, // Channel closed
                }
            }

            // Wait for reader thread to finish
            let _ = reader_handle.join();
            Ok(())
        });

        (BlockingClient { tx: req_tx }, event_rx, handle)
    }

    // Direct methods for each request type (blocking versions)
    pub fn header(&self, height: u32) -> Result<response::HeaderResp, ClientError> {
        let req = Request::Header {
            height,
            cp_height: None,
        };
        self.send_request(req).map(|resp| match resp {
            Response::Header(h) => h,
            _ => unreachable!(),
        })
    }

    pub fn header_with_proof(
        &self,
        height: u32,
        cp_height: u32,
    ) -> Result<response::HeaderWithProofResp, ClientError> {
        let req = Request::Header {
            height,
            cp_height: Some(cp_height),
        };
        self.send_request(req).map(|resp| match resp {
            Response::HeaderWithProof(h) => h,
            _ => unreachable!(),
        })
    }

    pub fn headers(
        &self,
        start_height: u32,
        count: usize,
    ) -> Result<response::HeadersResp, ClientError> {
        let req = Request::Headers {
            start_height,
            count,
            cp_height: None,
        };
        self.send_request(req).map(|resp| match resp {
            Response::Headers(h) => h,
            _ => unreachable!(),
        })
    }

    pub fn headers_with_checkpoint(
        &self,
        start_height: u32,
        count: usize,
        cp_height: u32,
    ) -> Result<response::HeadersWithCheckpointResp, ClientError> {
        let req = Request::Headers {
            start_height,
            count,
            cp_height: Some(cp_height),
        };
        self.send_request(req).map(|resp| match resp {
            Response::HeadersWithCheckpoint(h) => h,
            _ => unreachable!(),
        })
    }

    pub fn estimate_fee(&self, number: usize) -> Result<response::EstimateFeeResp, ClientError> {
        let req = Request::EstimateFee { number };
        self.send_request(req).map(|resp| match resp {
            Response::EstimateFee(e) => e,
            _ => unreachable!(),
        })
    }

    pub fn relay_fee(&self) -> Result<response::RelayFeeResp, ClientError> {
        let req = Request::RelayFee;
        self.send_request(req).map(|resp| match resp {
            Response::RelayFee(r) => r,
            _ => unreachable!(),
        })
    }

    pub fn get_balance(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<response::GetBalanceResp, ClientError> {
        let req = Request::GetBalance { script_hash };
        self.send_request(req).map(|resp| match resp {
            Response::GetBalance(b) => b,
            _ => unreachable!(),
        })
    }

    pub fn get_history(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<Vec<response::Tx>, ClientError> {
        let req = Request::GetHistory { script_hash };
        self.send_request(req).map(|resp| match resp {
            Response::GetHistory(h) => h,
            _ => unreachable!(),
        })
    }

    pub fn list_unspent(
        &self,
        script_hash: ElectrumScriptHash,
    ) -> Result<Vec<response::Utxo>, ClientError> {
        let req = Request::ListUnspent { script_hash };
        self.send_request(req).map(|resp| match resp {
            Response::ListUnspent(u) => u,
            _ => unreachable!(),
        })
    }

    pub fn broadcast_tx(&self, tx: bitcoin::Transaction) -> Result<bitcoin::Txid, ClientError> {
        let req = Request::BroadcastTx(tx);
        self.send_request(req).map(|resp| match resp {
            Response::BroadcastTx(txid) => txid,
            _ => unreachable!(),
        })
    }

    pub fn get_tx(&self, txid: bitcoin::Txid) -> Result<response::FullTx, ClientError> {
        let req = Request::GetTx { txid };
        self.send_request(req).map(|resp| match resp {
            Response::GetTx(tx) => tx,
            _ => unreachable!(),
        })
    }

    pub fn ping(&self) -> Result<(), ClientError> {
        let req = Request::Ping;
        self.send_request(req).map(|resp| match resp {
            Response::Ping => (),
            _ => unreachable!(),
        })
    }

    pub fn banner(&self) -> Result<String, ClientError> {
        let req = Request::Banner;
        self.send_request(req).map(|resp| match resp {
            Response::Banner(b) => b,
            _ => unreachable!(),
        })
    }

    pub fn custom(
        &self,
        method: CowStr,
        params: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, ClientError> {
        let req = Request::Custom(request::Custom { method, params });
        self.send_request(req).map(|resp| match resp {
            Response::Custom(v) => v,
            _ => unreachable!(),
        })
    }

    fn send_request(&self, req: Request) -> Result<Response, ClientError> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.tx
            .send((req, Some(tx)))
            .map_err(|_| ClientError::SendError)?;

        match rx.recv() {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(ClientError::ServerError(e)),
            Err(_) => Err(ClientError::Cancelled),
        }
    }

    pub fn send_event_request(&self, req: Request) -> Result<(), ClientError> {
        self.tx
            .send((req, None))
            .map_err(|_| ClientError::SendError)
    }
}

// Blocking state management
struct BlockingState {
    pending_requests: std::collections::HashMap<u32, BlockingPendingRequest>,
}

struct BlockingPendingRequest {
    request: Request,
    response_tx: Option<std::sync::mpsc::SyncSender<Result<Response, ResponseError>>>,
}

impl BlockingState {
    fn new() -> Self {
        Self {
            pending_requests: std::collections::HashMap::new(),
        }
    }

    fn track_request(
        &mut self,
        id: u32,
        request: Request,
        response_tx: Option<std::sync::mpsc::SyncSender<Result<Response, ResponseError>>>,
    ) -> RawRequest {
        let method = request.method_name().to_string();
        let params = request.params();

        self.pending_requests.insert(
            id,
            BlockingPendingRequest {
                request: request.clone(),
                response_tx,
            },
        );

        RawRequest {
            jsonrpc: JSONRPC_VERSION_2_0.into(),
            id,
            method: method.into(),
            params,
        }
    }

    fn handle_response(
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
}
