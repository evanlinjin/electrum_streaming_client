//! Request types for the Electrum protocol.
//!
//! This module provides strongly-typed request structures for all Electrum protocol methods.
//! Each request type corresponds to a specific Electrum RPC method as documented in the
//! [Electrum Protocol](https://electrum-protocol.readthedocs.io/en/latest/).

use bitcoin::{consensus::Encodable, hex::DisplayHex, Script, Txid};
use serde_json::Value;

use crate::{CowStr, ElectrumScriptHash};

/// Represents all possible Electrum protocol requests.
///
/// This enum provides a unified interface for all request types supported by the Electrum protocol.
/// Each variant contains the specific request parameters needed for that method.
#[derive(Debug, Clone)]
pub enum Request {
    /// Request a block header by height, optionally with Merkle proof.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-block-header>
    Header { height: u32, cp_height: Option<u32> },

    /// Request multiple consecutive block headers, optionally with checkpoint proof.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-block-headers>
    Headers {
        start_height: u32,
        count: usize,
        cp_height: Option<u32>,
    },

    /// Estimate the fee rate for transaction confirmation.
    /// Target number of blocks for confirmation.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-estimatefee>
    EstimateFee { number: usize },

    /// Subscribe to new block headers.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-headers-subscribe>
    HeadersSubscribe,

    /// Get the minimum relay fee.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-relayfee>
    RelayFee,

    /// Get the balance of a script hash.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-get-balance>
    GetBalance { script_hash: ElectrumScriptHash },

    /// Get the transaction history of a script hash.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-get-history>
    GetHistory { script_hash: ElectrumScriptHash },

    /// Get mempool transactions for a script hash.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-get-mempool>
    GetMempool { script_hash: ElectrumScriptHash },

    /// List unspent outputs for a script hash.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-listunspent>
    ListUnspent { script_hash: ElectrumScriptHash },

    /// Subscribe to script hash status changes.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-subscribe>
    ScriptHashSubscribe { script_hash: ElectrumScriptHash },

    /// Unsubscribe from script hash status changes.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-scripthash-unsubscribe>
    ScriptHashUnsubscribe { script_hash: ElectrumScriptHash },

    /// Broadcast a transaction to the network.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-transaction-broadcast>
    BroadcastTx(bitcoin::Transaction),

    /// Get a transaction by its ID.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-transaction-get>
    GetTx { txid: Txid },

    /// Get the Merkle proof for a transaction.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-transaction-get-merkle>
    GetTxMerkle { txid: Txid, height: u32 },

    /// Get a transaction ID from its position in a block.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#blockchain-transaction-id-from-pos>
    GetTxidFromPos { height: u32, tx_pos: usize },

    /// Get the mempool fee histogram.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#mempool-get-fee-histogram>
    GetFeeHistogram,

    /// Get the server banner.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#server-banner>
    Banner,

    /// Get server features and capabilities.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#server-features>
    Features,

    /// Ping the server to test the connection.
    /// See: <https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html#server-ping>
    Ping,

    /// Custom request for methods not explicitly supported.
    Custom(Custom),
}

impl Request {
    /// Returns the JSON-RPC method name for this request.
    pub fn method_name(&self) -> &str {
        match self {
            Request::Header { .. } => "blockchain.block.header",
            Request::Headers { .. } => "blockchain.block.headers",
            Request::EstimateFee { .. } => "blockchain.estimatefee",
            Request::HeadersSubscribe => "blockchain.headers.subscribe",
            Request::RelayFee => "blockchain.relayfee",
            Request::GetBalance { .. } => "blockchain.scripthash.get_balance",
            Request::GetHistory { .. } => "blockchain.scripthash.get_history",
            Request::GetMempool { .. } => "blockchain.scripthash.get_mempool",
            Request::ListUnspent { .. } => "blockchain.scripthash.listunspent",
            Request::ScriptHashSubscribe { .. } => "blockchain.scripthash.subscribe",
            Request::ScriptHashUnsubscribe { .. } => "blockchain.scripthash.unsubscribe",
            Request::BroadcastTx(_) => "blockchain.transaction.broadcast",
            Request::GetTx { .. } => "blockchain.transaction.get",
            Request::GetTxMerkle { .. } => "blockchain.transaction.get_merkle",
            Request::GetTxidFromPos { .. } => "blockchain.transaction.id_from_pos",
            Request::GetFeeHistogram => "mempool.get_fee_histogram",
            Request::Banner => "server.banner",
            Request::Features => "server.features",
            Request::Ping => "server.ping",
            Request::Custom(c) => &c.method,
        }
    }

    /// Returns the JSON-RPC parameters for this request.
    pub fn params(&self) -> Vec<Value> {
        match self {
            Request::Header { height, cp_height } => {
                if let Some(cp) = cp_height {
                    vec![(*height).into(), (*cp).into()]
                } else {
                    vec![(*height).into()]
                }
            }
            Request::Headers {
                start_height,
                count,
                cp_height,
            } => {
                if let Some(cp) = cp_height {
                    vec![(*start_height).into(), (*count).into(), (*cp).into()]
                } else {
                    vec![(*start_height).into(), (*count).into()]
                }
            }
            Request::EstimateFee { number } => vec![(*number).into()],
            Request::HeadersSubscribe => vec![],
            Request::RelayFee => vec![],
            Request::GetBalance { script_hash } => vec![script_hash.to_string().into()],
            Request::GetHistory { script_hash } => vec![script_hash.to_string().into()],
            Request::GetMempool { script_hash } => vec![script_hash.to_string().into()],
            Request::ListUnspent { script_hash } => vec![script_hash.to_string().into()],
            Request::ScriptHashSubscribe { script_hash } => vec![script_hash.to_string().into()],
            Request::ScriptHashUnsubscribe { script_hash } => vec![script_hash.to_string().into()],
            Request::BroadcastTx(tx) => {
                let mut tx_bytes = Vec::<u8>::new();
                tx.consensus_encode(&mut tx_bytes).expect("must encode");
                vec![tx_bytes.to_lower_hex_string().into()]
            }
            Request::GetTx { txid } => vec![txid.to_string().into()],
            Request::GetTxMerkle { txid, height } => {
                vec![txid.to_string().into(), (*height).into()]
            }
            Request::GetTxidFromPos { height, tx_pos } => vec![(*height).into(), (*tx_pos).into()],
            Request::GetFeeHistogram => vec![],
            Request::Banner => vec![],
            Request::Features => vec![],
            Request::Ping => vec![],
            Request::Custom(c) => c.params.clone(),
        }
    }

    /// Create a GetBalance request from a Bitcoin script.
    pub fn get_balance_from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Request::GetBalance { script_hash }
    }

    /// Create a GetHistory request from a Bitcoin script.
    pub fn get_history_from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Request::GetHistory { script_hash }
    }

    /// Create a GetMempool request from a Bitcoin script.
    pub fn get_mempool_from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Request::GetMempool { script_hash }
    }

    /// Create a ListUnspent request from a Bitcoin script.
    pub fn list_unspent_from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Request::ListUnspent { script_hash }
    }

    /// Create a ScriptHashSubscribe request from a Bitcoin script.
    pub fn subscribe_from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Request::ScriptHashSubscribe { script_hash }
    }

    /// Create a ScriptHashUnsubscribe request from a Bitcoin script.
    pub fn unsubscribe_from_script<S: AsRef<Script>>(script: S) -> Self {
        let script_hash = ElectrumScriptHash::new(script.as_ref());
        Request::ScriptHashUnsubscribe { script_hash }
    }
}

/// A custom request for methods not explicitly supported.
///
/// Allows sending arbitrary method calls to the server.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Custom {
    /// The method name.
    pub method: CowStr,
    /// The method parameters.
    pub params: Vec<Value>,
}

/// Error types for request operations.
#[derive(Debug)]
pub enum Error<DispatchError> {
    /// Failed to send the request.
    Dispatch(DispatchError),
    /// Request was canceled before completion.
    Canceled,
    /// Server returned an error response.
    Response(crate::ResponseError),
}

impl<SendError: std::fmt::Display> std::fmt::Display for Error<SendError> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dispatch(e) => write!(f, "Failed to dispatch request: {}", e),
            Self::Canceled => write!(f, "Request was canceled before being satisfied."),
            Self::Response(e) => write!(f, "Request satisfied with error: {}", e),
        }
    }
}

impl<SendError: std::error::Error> std::error::Error for Error<SendError> {}
