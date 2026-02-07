// Copyright (c) Sage Intel Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Side-SUI Consensus: Lightweight BFT consensus for private consortium networks
// Based on sui-core/src/mock_consensus.rs, enhanced for multi-node operation
//
// ARCHITECTURE:
// - Single-leader rotating consensus (similar to Raft)
// - Simple voting protocol between validators
// - Sequential transaction execution (no parallelism overhead)
// - Designed for 3-11 validators in trusted consortium
//
// TODO Phase 2a: Add network transport between validators
// TODO Phase 2b: Implement voting protocol
// TODO Phase 2c: Add persistence/WAL
// TODO Phase 2d: Integrate with epoch management

use std::sync::{Arc, Weak};
use std::time::Duration;
use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn, error};

// Re-export from sui-core for compatibility
use sui_types::committee::EpochId;
use sui_types::error::{SuiError, SuiResult};
use sui_types::base_types::AuthorityName;
use sui_types::executable_transaction::VerifiedExecutableTransaction;
use sui_types::messages_consensus::{
    ConsensusPosition, ConsensusTransaction, ConsensusTransactionKind,
};
use sui_types::transaction::VerifiedTransaction;

// These would come from sui-core, but we define placeholders
// to show the interface we need to implement
pub type BlockStatusReceiver = oneshot::Receiver<BlockStatus>;

#[derive(Debug, Clone)]
pub enum BlockStatus {
    Sequenced(u64),  // Block height
    Failed(String),
}

/// Configuration for Side-SUI consensus
#[derive(Debug, Clone)]
pub struct SideSuiConfig {
    /// This validator's identity
    pub authority: AuthorityName,
    /// List of all validators in the consortium
    pub validators: Vec<ValidatorInfo>,
    /// Network listen address
    pub listen_addr: String,
    /// Block interval (how often to propose blocks)
    pub block_interval_ms: u64,
    /// Maximum transactions per block
    pub max_txns_per_block: usize,
}

#[derive(Debug, Clone)]
pub struct ValidatorInfo {
    pub name: AuthorityName,
    pub address: String,
    pub voting_power: u64,
}

/// The main Side-SUI consensus client
/// 
/// This is a lightweight replacement for Narwhal/Bullshark that:
/// - Uses simple leader-based consensus
/// - Targets 3-11 validators
/// - Runs on 16-32GB nodes instead of 128GB
pub struct SideSuiConsensus {
    config: SideSuiConfig,
    tx_sender: mpsc::Sender<ConsensusTransaction>,
    // Current block height
    block_height: Arc<RwLock<u64>>,
    // Pending transactions waiting for consensus
    pending_txns: Arc<RwLock<Vec<ConsensusTransaction>>>,
    // Handle to the consensus loop
    _consensus_handle: JoinHandle<()>,
}

impl SideSuiConsensus {
    /// Create a new Side-SUI consensus instance
    pub fn new(
        config: SideSuiConfig,
        // In full implementation, this would be Weak<AuthorityState>
        // For now we just sequence transactions
    ) -> Self {
        let (tx_sender, tx_receiver) = mpsc::channel(100_000);
        let block_height = Arc::new(RwLock::new(0));
        let pending_txns = Arc::new(RwLock::new(Vec::new()));
        
        let consensus_handle = Self::run_consensus_loop(
            config.clone(),
            tx_receiver,
            block_height.clone(),
            pending_txns.clone(),
        );
        
        info!(
            "Side-SUI consensus started for {} with {} validators",
            config.authority,
            config.validators.len()
        );
        
        Self {
            config,
            tx_sender,
            block_height,
            pending_txns,
            _consensus_handle: consensus_handle,
        }
    }
    
    /// Main consensus loop
    fn run_consensus_loop(
        config: SideSuiConfig,
        mut tx_receiver: mpsc::Receiver<ConsensusTransaction>,
        block_height: Arc<RwLock<u64>>,
        pending_txns: Arc<RwLock<Vec<ConsensusTransaction>>>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("Consensus loop started");
            
            loop {
                tokio::select! {
                    // Receive new transactions
                    Some(tx) = tx_receiver.recv() => {
                        let mut pending = pending_txns.write().await;
                        pending.push(tx);
                        debug!("Transaction added to pending, count: {}", pending.len());
                    }
                    
                    // Block production interval
                    _ = tokio::time::sleep(Duration::from_millis(config.block_interval_ms)) => {
                        let mut pending = pending_txns.write().await;
                        if !pending.is_empty() {
                            // TODO Phase 2b: Implement actual voting here
                            // For now, just sequence directly (single-node mode)
                            let txns_to_process: Vec<_> = pending
                                .drain(..pending.len().min(config.max_txns_per_block))
                                .collect();
                            
                            let mut height = block_height.write().await;
                            *height += 1;
                            
                            info!(
                                "Block {} produced with {} transactions",
                                *height,
                                txns_to_process.len()
                            );
                            
                            // TODO: Actually execute/sequence these transactions
                            // This is where we'd call into AuthorityState
                        }
                    }
                }
            }
        })
    }
    
    /// Submit transactions to consensus
    fn submit_impl(
        &self,
        transactions: &[ConsensusTransaction],
    ) -> SuiResult<(Vec<ConsensusPosition>, BlockStatusReceiver)> {
        for tx in transactions {
            self.tx_sender
                .try_send(tx.clone())
                .map_err(|_| SuiError::from("SideSuiConsensus channel overflowed"))?;
        }
        
        // Return placeholder position (will be updated when block is produced)
        let (status_tx, status_rx) = oneshot::channel();
        let _ = status_tx.send(BlockStatus::Sequenced(0));
        
        Ok((
            transactions.iter().enumerate().map(|(i, _)| {
                ConsensusPosition {
                    epoch: EpochId::MIN,
                    block: Default::default(),  // TODO: proper block ref
                    index: i as u32,
                }
            }).collect(),
            status_rx,
        ))
    }
    
    /// Get current block height
    pub async fn block_height(&self) -> u64 {
        *self.block_height.read().await
    }
    
    /// Get number of pending transactions
    pub async fn pending_count(&self) -> usize {
        self.pending_txns.read().await.len()
    }
}

// ============================================================================
// Trait Implementations (matching sui-core interfaces)
// ============================================================================

/// Trait for submitting to consensus (sync)
/// This matches sui-core::consensus_adapter::SubmitToConsensus
pub trait SubmitToConsensus: Sync + Send + 'static {
    fn submit_to_consensus(
        &self,
        transactions: &[ConsensusTransaction],
    ) -> SuiResult;

    fn submit_best_effort(
        &self,
        transaction: &ConsensusTransaction,
        timeout: Duration,
    ) -> SuiResult;
}

impl SubmitToConsensus for SideSuiConsensus {
    fn submit_to_consensus(
        &self,
        transactions: &[ConsensusTransaction],
    ) -> SuiResult {
        self.submit_impl(transactions).map(|_| ())
    }

    fn submit_best_effort(
        &self,
        transaction: &ConsensusTransaction,
        _timeout: Duration,
    ) -> SuiResult {
        self.submit_impl(std::slice::from_ref(transaction)).map(|_| ())
    }
}

/// Trait for consensus client (async)
/// This matches sui-core::consensus_adapter::ConsensusClient
#[async_trait]
pub trait ConsensusClient: Sync + Send + 'static {
    async fn submit(
        &self,
        transactions: &[ConsensusTransaction],
    ) -> SuiResult<(Vec<ConsensusPosition>, BlockStatusReceiver)>;
}

#[async_trait]
impl ConsensusClient for SideSuiConsensus {
    async fn submit(
        &self,
        transactions: &[ConsensusTransaction],
    ) -> SuiResult<(Vec<ConsensusPosition>, BlockStatusReceiver)> {
        self.submit_impl(transactions)
    }
}

// ============================================================================
// Network Layer (TODO Phase 2a)
// ============================================================================

/// Network transport between validators
/// 
/// TODO: Implement using tonic (gRPC) or raw TCP
#[allow(dead_code)]
mod network {
    use super::*;
    
    /// Messages between validators
    #[derive(Debug, Clone)]
    pub enum ConsensusMessage {
        /// Leader proposes a block
        Propose {
            height: u64,
            transactions: Vec<ConsensusTransaction>,
            leader: AuthorityName,
        },
        /// Validator votes on a proposal
        Vote {
            height: u64,
            approve: bool,
            voter: AuthorityName,
        },
        /// Block is committed (2/3+ votes)
        Commit {
            height: u64,
        },
    }
    
    /// Network layer stub
    pub struct ConsensusNetwork {
        // TODO: Add actual network implementation
    }
    
    impl ConsensusNetwork {
        pub fn new(_config: &SideSuiConfig) -> Self {
            Self {}
        }
        
        pub async fn broadcast(&self, _msg: ConsensusMessage) -> SuiResult {
            // TODO: Broadcast to all validators
            Ok(())
        }
        
        pub async fn send_to(&self, _validator: &AuthorityName, _msg: ConsensusMessage) -> SuiResult {
            // TODO: Send to specific validator
            Ok(())
        }
    }
}

// ============================================================================
// Persistence Layer (TODO Phase 2c)
// ============================================================================

/// Write-ahead log for crash recovery
/// 
/// TODO: Implement using RocksDB or simple file-based WAL
#[allow(dead_code)]
mod persistence {
    use super::*;
    
    /// Persisted block
    #[derive(Debug, Clone)]
    pub struct Block {
        pub height: u64,
        pub transactions: Vec<ConsensusTransaction>,
        pub timestamp: u64,
    }
    
    /// WAL for consensus
    pub struct ConsensusWal {
        // TODO: Add actual persistence
    }
    
    impl ConsensusWal {
        pub fn new(_path: &str) -> Self {
            Self {}
        }
        
        pub async fn append_block(&self, _block: &Block) -> SuiResult {
            // TODO: Persist block
            Ok(())
        }
        
        pub async fn get_last_block(&self) -> Option<Block> {
            // TODO: Load from disk
            None
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_basic_consensus() {
        let config = SideSuiConfig {
            authority: AuthorityName::default(),
            validators: vec![],
            listen_addr: "127.0.0.1:8080".to_string(),
            block_interval_ms: 100,
            max_txns_per_block: 1000,
        };
        
        let consensus = SideSuiConsensus::new(config);
        
        // Should start with zero height
        assert_eq!(consensus.block_height().await, 0);
        assert_eq!(consensus.pending_count().await, 0);
    }
}
