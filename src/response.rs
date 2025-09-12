//! Response types for the Electrum protocol.

use std::collections::HashMap;
use bitcoin::{
    absolute,
    hashes::{Hash, HashEngine},
    Amount, BlockHash, Txid,
};
use serde_json::Value;

use crate::{DoubleSHA, ElectrumScriptStatus};

/// All possible Electrum protocol responses.
#[derive(Debug, Clone)]
pub enum Response {
    /// Block header response (just the header).
    Header(HeaderResp),
    /// Block header response with Merkle proof.
    HeaderWithProof(HeaderWithProofResp),
    /// Multiple block headers.
    Headers(HeadersResp),
    /// Multiple block headers with checkpoint proof.
    HeadersWithCheckpoint(HeadersWithCheckpointResp),
    /// Fee estimate response.
    EstimateFee(EstimateFeeResp),
    /// Headers subscription response.
    HeadersSubscribe(HeadersSubscribeResp),
    /// Relay fee response.
    RelayFee(RelayFeeResp),
    /// Balance response.
    GetBalance(GetBalanceResp),
    /// Transaction history.
    GetHistory(Vec<Tx>),
    /// Mempool transactions.
    GetMempool(Vec<MempoolTx>),
    /// Unspent outputs.
    ListUnspent(Vec<Utxo>),
    /// Script subscription response.
    ScriptHashSubscribe(Option<ElectrumScriptStatus>),
    /// Script unsubscribe response.
    ScriptHashUnsubscribe(bool),
    /// Broadcast transaction response.
    BroadcastTx(Txid),
    /// Raw transaction.
    GetTx(FullTx),
    /// Transaction Merkle proof.
    GetTxMerkle(TxMerkle),
    /// Transaction ID from position.
    GetTxidFromPos(TxidFromPos),
    /// Fee histogram.
    GetFeeHistogram(Vec<FeePair>),
    /// Server banner.
    Banner(String),
    /// Server features.
    Features(ServerFeatures),
    /// Ping response.
    Ping,
    /// Custom response.
    Custom(Value),
}

impl Response {
    /// Deserialize from JSON based on the JSON-RPC method name.
    pub fn from_json(method_name: &str, value: Value) -> Result<Self, serde_json::Error> {
        match method_name {
            "blockchain.block.header" => {
                // Try to deserialize as HeaderWithProofResp first (has more fields)
                if let Ok(resp) = serde_json::from_value::<HeaderWithProofResp>(value.clone()) {
                    Ok(Response::HeaderWithProof(resp))
                } else {
                    Ok(Response::Header(serde_json::from_value(value)?))
                }
            }
            "blockchain.block.headers" => {
                // Try to deserialize as HeadersWithCheckpointResp first (has more fields)
                if let Ok(resp) = serde_json::from_value::<HeadersWithCheckpointResp>(value.clone()) {
                    Ok(Response::HeadersWithCheckpoint(resp))
                } else {
                    Ok(Response::Headers(serde_json::from_value(value)?))
                }
            }
            "blockchain.estimatefee" => Ok(Response::EstimateFee(serde_json::from_value(value)?)),
            "blockchain.headers.subscribe" => Ok(Response::HeadersSubscribe(serde_json::from_value(value)?)),
            "blockchain.relayfee" => Ok(Response::RelayFee(serde_json::from_value(value)?)),
            "blockchain.scripthash.get_balance" => Ok(Response::GetBalance(serde_json::from_value(value)?)),
            "blockchain.scripthash.get_history" => Ok(Response::GetHistory(serde_json::from_value(value)?)),
            "blockchain.scripthash.get_mempool" => Ok(Response::GetMempool(serde_json::from_value(value)?)),
            "blockchain.scripthash.listunspent" => Ok(Response::ListUnspent(serde_json::from_value(value)?)),
            "blockchain.scripthash.subscribe" => Ok(Response::ScriptHashSubscribe(serde_json::from_value(value)?)),
            "blockchain.scripthash.unsubscribe" => Ok(Response::ScriptHashUnsubscribe(serde_json::from_value(value)?)),
            "blockchain.transaction.broadcast" => Ok(Response::BroadcastTx(serde_json::from_value(value)?)),
            "blockchain.transaction.get" => Ok(Response::GetTx(serde_json::from_value(value)?)),
            "blockchain.transaction.get_merkle" => Ok(Response::GetTxMerkle(serde_json::from_value(value)?)),
            "blockchain.transaction.id_from_pos" => Ok(Response::GetTxidFromPos(serde_json::from_value(value)?)),
            "mempool.get_fee_histogram" => Ok(Response::GetFeeHistogram(serde_json::from_value(value)?)),
            "server.banner" => Ok(Response::Banner(serde_json::from_value(value)?)),
            "server.features" => Ok(Response::Features(serde_json::from_value(value)?)),
            "server.ping" => Ok(Response::Ping),
            _ => Ok(Response::Custom(value)),
        }
    }
}

/// Simple block header response (just the header).
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct HeaderResp {
    #[serde(deserialize_with = "crate::custom_serde::from_consensus_hex")]
    pub header: bitcoin::block::Header,
}

impl Default for HeaderResp {
    fn default() -> Self {
        use bitcoin::hashes::Hash;
        Self {
            header: bitcoin::block::Header {
                version: bitcoin::block::Version::ONE,
                prev_blockhash: bitcoin::BlockHash::all_zeros(),
                merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                time: 0,
                bits: bitcoin::CompactTarget::from_consensus(0),
                nonce: 0,
            },
        }
    }
}

/// Block header response with Merkle proof.
#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct HeaderWithProofResp {
    /// Merkle branch.
    pub branch: Vec<DoubleSHA>,
    /// The block header.
    #[serde(deserialize_with = "crate::custom_serde::from_consensus_hex")]
    pub header: bitcoin::block::Header,
    /// Merkle root.
    pub root: DoubleSHA,
}

impl Default for HeaderWithProofResp {
    fn default() -> Self {
        use bitcoin::hashes::Hash;
        Self {
            branch: vec![],
            header: bitcoin::block::Header {
                version: bitcoin::block::Version::ONE,
                prev_blockhash: bitcoin::BlockHash::all_zeros(),
                merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                time: 0,
                bits: bitcoin::CompactTarget::from_consensus(0),
                nonce: 0,
            },
            root: DoubleSHA::all_zeros(),
        }
    }
}

/// Multiple block headers response (without checkpoint proof).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct HeadersResp {
    /// Number of headers returned.
    pub count: usize,
    /// The block headers.
    #[serde(
        rename = "hex",
        deserialize_with = "crate::custom_serde::from_cancat_consensus_hex"
    )]
    pub headers: Vec<bitcoin::block::Header>,
    /// Maximum headers available.
    pub max: usize,
}

impl Default for HeadersResp {
    fn default() -> Self {
        Self {
            count: 0,
            headers: vec![],
            max: 0,
        }
    }
}

/// Multiple block headers response with checkpoint proof.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct HeadersWithCheckpointResp {
    /// Number of headers returned.
    pub count: usize,
    /// The block headers.
    #[serde(
        rename = "hex",
        deserialize_with = "crate::custom_serde::from_cancat_consensus_hex"
    )]
    pub headers: Vec<bitcoin::block::Header>,
    /// Maximum headers available.
    pub max: usize,
    /// Merkle root for checkpoint proof.
    pub root: DoubleSHA,
    /// Merkle branch for checkpoint proof.
    pub branch: Vec<DoubleSHA>,
}

impl Default for HeadersWithCheckpointResp {
    fn default() -> Self {
        use bitcoin::hashes::Hash;
        Self {
            count: 0,
            headers: vec![],
            max: 0,
            root: DoubleSHA::all_zeros(),
            branch: vec![],
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(transparent)]
pub struct EstimateFeeResp {
    #[serde(deserialize_with = "crate::custom_serde::feerate_opt_from_btc_per_kb")]
    pub fee_rate: Option<bitcoin::FeeRate>,
}

impl Default for EstimateFeeResp {
    fn default() -> Self {
        Self { fee_rate: None }
    }
}

#[derive(Debug, Clone, serde::Deserialize, PartialEq, Eq)]
pub struct HeadersSubscribeResp {
    #[serde(
        rename = "hex",
        deserialize_with = "crate::custom_serde::from_consensus_hex"
    )]
    pub header: bitcoin::block::Header,
    pub height: u32,
}

impl Default for HeadersSubscribeResp {
    fn default() -> Self {
        use bitcoin::hashes::Hash;
        Self {
            header: bitcoin::block::Header {
                version: bitcoin::block::Version::ONE,
                prev_blockhash: bitcoin::BlockHash::all_zeros(),
                merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                time: 0,
                bits: bitcoin::CompactTarget::from_consensus(0),
                nonce: 0,
            },
            height: 0,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(transparent)]
pub struct RelayFeeResp {
    #[serde(deserialize_with = "crate::custom_serde::amount_from_btc")]
    pub fee: Amount,
}

impl Default for RelayFeeResp {
    fn default() -> Self {
        Self { fee: Amount::ZERO }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct GetBalanceResp {
    #[serde(deserialize_with = "crate::custom_serde::amount_from_sats")]
    pub confirmed: Amount,
    #[serde(deserialize_with = "crate::custom_serde::amount_from_maybe_negative_sats")]
    pub unconfirmed: Amount,
}

impl Default for GetBalanceResp {
    fn default() -> Self {
        Self {
            confirmed: Amount::ZERO,
            unconfirmed: Amount::ZERO,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum Tx {
    Mempool(MempoolTx),
    Confirmed(ConfirmedTx),
}

impl Tx {
    pub fn txid(&self) -> bitcoin::Txid {
        match self {
            Tx::Mempool(MempoolTx { txid, .. }) => *txid,
            Tx::Confirmed(ConfirmedTx { txid, .. }) => *txid,
        }
    }

    pub fn confirmation_height(&self) -> Option<absolute::Height> {
        match self {
            Tx::Mempool(_) => None,
            Tx::Confirmed(ConfirmedTx { height, .. }) => Some(*height),
        }
    }

    pub fn electrum_height(&self) -> i64 {
        match self {
            Tx::Mempool(mempool_tx) if mempool_tx.confirmed_inputs => 0,
            Tx::Mempool(_) => -1,
            Tx::Confirmed(confirmed_tx) => confirmed_tx.height.to_consensus_u32() as i64,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ConfirmedTx {
    #[serde(rename = "tx_hash")]
    pub txid: bitcoin::Txid,
    pub height: absolute::Height,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct MempoolTx {
    #[serde(rename = "tx_hash")]
    pub txid: bitcoin::Txid,
    #[serde(deserialize_with = "crate::custom_serde::amount_from_sats")]
    pub fee: bitcoin::Amount,
    #[serde(
        rename = "height",
        deserialize_with = "crate::custom_serde::all_inputs_confirmed_bool_from_height"
    )]
    pub confirmed_inputs: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Utxo {
    pub height: absolute::Height,
    pub tx_pos: usize,
    #[serde(rename = "tx_hash")]
    pub txid: bitcoin::Txid,
    #[serde(deserialize_with = "crate::custom_serde::amount_from_sats")]
    pub value: bitcoin::Amount,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(transparent)]
pub struct FullTx {
    #[serde(deserialize_with = "crate::custom_serde::from_consensus_hex")]
    pub tx: bitcoin::Transaction,
}

impl Default for FullTx {
    fn default() -> Self {
        Self {
            tx: bitcoin::Transaction {
                version: bitcoin::transaction::Version::ONE,
                lock_time: bitcoin::absolute::LockTime::ZERO,
                input: vec![],
                output: vec![],
            },
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TxMerkle {
    pub block_height: absolute::Height,
    pub merkle: Vec<DoubleSHA>,
    pub pos: usize,
}

impl Default for TxMerkle {
    fn default() -> Self {
        Self {
            block_height: bitcoin::absolute::Height::ZERO,
            merkle: vec![],
            pos: 0,
        }
    }
}

impl TxMerkle {
    pub fn expected_merkle_root(&self, txid: bitcoin::Txid) -> bitcoin::TxMerkleNode {
        let mut index = self.pos;
        let mut cur = txid.to_raw_hash();
        for next_hash in &self.merkle {
            cur = DoubleSHA::from_engine({
                let mut engine = DoubleSHA::engine();
                if index % 2 == 0 {
                    engine.input(cur.as_ref());
                    engine.input(next_hash.as_ref());
                } else {
                    engine.input(next_hash.as_ref());
                    engine.input(cur.as_ref());
                };
                engine
            });
            index /= 2;
        }
        cur.into()
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(transparent)]
pub struct TxidFromPos {
    pub txid: bitcoin::Txid,
}

impl Default for TxidFromPos {
    fn default() -> Self {
        use bitcoin::hashes::Hash;
        Self {
            txid: bitcoin::Txid::all_zeros(),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct FeePair {
    #[serde(deserialize_with = "crate::custom_serde::feerate_from_sat_per_byte")]
    pub fee_rate: bitcoin::FeeRate,
    #[serde(deserialize_with = "crate::custom_serde::weight_from_vb")]
    pub weight: bitcoin::Weight,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ServerFeatures {
    pub hosts: HashMap<String, ServerHostValues>,
    pub genesis_hash: BlockHash,
    pub hash_function: String,
    pub server_version: String,
    pub protocol_max: String,
    pub protocol_min: String,
    pub pruning: Option<u32>,
}

impl Default for ServerFeatures {
    fn default() -> Self {
        use bitcoin::hashes::Hash;
        Self {
            hosts: HashMap::new(),
            genesis_hash: BlockHash::all_zeros(),
            hash_function: String::new(),
            server_version: String::new(),
            protocol_max: String::new(),
            protocol_min: String::new(),
            pruning: None,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ServerHostValues {
    pub ssl_port: Option<u16>,
    pub tcp_port: Option<u16>,
}