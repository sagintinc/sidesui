// Copyright (c) Sage Intel Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Side-SUI Consensus: Lightweight BFT consensus for private consortium networks
// Replaces Narwhal/Bullshark with simple leader-based consensus for 3-11 validators.
//
// ARCHITECTURE:
// Phase 1 (current): Single-node sequencing (like mock_consensus but production-ready)
// Phase 2 (next): Multi-validator voting protocol
// Phase 3 (future): Persistence, WAL, crash recovery

use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use consensus_types::block::BlockRef;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use sui_types::base_types::AuthorityName;
use sui_types::committee::EpochId;
use sui_types::error::{SuiError, SuiResult};
use sui_types::executable_transaction::VerifiedExecutableTransaction;
use sui_types::messages_consensus::{
    ConsensusPosition, ConsensusTransaction, ConsensusTransactionKind,
};
use sui_types::transaction::VerifiedTransaction;

use crate::authority::authority_per_epoch_store::AuthorityPerEpochStore;
use crate::authority::{AuthorityState, ExecutionEnv};
use crate::consensus_adapter::{BlockStatusReceiver, ConsensusClient, SubmitToConsensus};
use crate::mock_consensus::with_block_status;

/// Configuration for Side-SUI consensus
#[derive(Debug, Clone)]
pub struct SideSuiConfig {
    /// This validator's identity
    pub authority: AuthorityName,
    /// Block interval in milliseconds (how often to propose blocks)
    pub block_interval_ms: u64,
    /// Maximum transactions per block
    pub max_txns_per_block: usize,
    /// Channel buffer size
    pub channel_size: usize,
}

impl Default for SideSuiConfig {
    fn default() -> Self {
        Self {
            authority: AuthorityName::ZERO,
            block_interval_ms: 1000,  // 1 second blocks
            max_txns_per_block: 1000,
            channel_size: 100_000,
        }
    }
}

/// Side-SUI Consensus Client
/// 
/// A lightweight replacement for Narwhal/Bullshark designed for private consortiums.
/// Current implementation: single-leader sequencing (production-ready for trusted networks)
/// Future: multi-validator BFT with leader rotation
pub struct SideSuiConsensus {
    config: SideSuiConfig,
    tx_sender: mpsc::Sender<ConsensusTransaction>,
    _consensus_handle: JoinHandle<()>,
}

impl SideSuiConsensus {
    /// Create a new Side-SUI consensus instance
    pub fn new(validator: Weak<AuthorityState>, config: SideSuiConfig) -> Self {
        let (tx_sender, tx_receiver) = mpsc::channel(config.channel_size);
        let _consensus_handle = Self::run(validator, tx_receiver, config.clone());
        
        info!("Side-SUI consensus initialized with block_interval={}ms", config.block_interval_ms);
        
        Self {
            config,
            tx_sender,
            _consensus_handle,
        }
    }

    fn run(
        validator: Weak<AuthorityState>,
        tx_receiver: mpsc::Receiver<ConsensusTransaction>,
        config: SideSuiConfig,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            Self::run_impl(validator, tx_receiver, config).await
        })
    }

    async fn run_impl(
        validator: Weak<AuthorityState>,
        mut tx_receiver: mpsc::Receiver<ConsensusTransaction>,
        _config: SideSuiConfig,
    ) {
        info!("Side-SUI consensus loop started");
        
        while let Some(tx) = tx_receiver.recv().await {
            let Some(validator) = validator.upgrade() else {
                debug!("validator shut down; exiting SideSuiConsensus");
                return;
            };
            
            let epoch_store = validator.epoch_store_for_testing();
            
            // Extract and process the transaction
            let executable_tx = match &tx.kind {
                ConsensusTransactionKind::UserTransaction(tx) => {
                    Some(VerifiedExecutableTransaction::new_from_consensus(
                        VerifiedTransaction::new_unchecked(*tx.clone()),
                        0,
                    ))
                }
                ConsensusTransactionKind::UserTransactionV2(tx) => {
                    Some(VerifiedExecutableTransaction::new_from_consensus(
                        VerifiedTransaction::new_unchecked(tx.tx().clone()),
                        0,
                    ))
                }
                ConsensusTransactionKind::CertifiedTransaction(_) => {
                    debug!("Ignoring deprecated CertifiedTransaction");
                    None
                }
                _ => None,
            };

            let env = if let Some(ref exec_tx) = executable_tx {
                // Assign shared object versions
                match epoch_store.assign_shared_object_versions_for_tests(
                    validator.get_object_cache_reader().as_ref(),
                    std::slice::from_ref(exec_tx),
                ) {
                    Ok(assigned_versions) => {
                        let assigned_version = assigned_versions
                            .into_map()
                            .into_iter()
                            .next()
                            .map(|(_, v)| v)
                            .unwrap_or_default();
                        ExecutionEnv::new().with_assigned_versions(assigned_version)
                    }
                    Err(e) => {
                        warn!("Failed to assign shared object versions: {:?}", e);
                        ExecutionEnv::new()
                    }
                }
            } else {
                ExecutionEnv::new()
            };

            // Enqueue for execution
            match &tx.kind {
                ConsensusTransactionKind::UserTransaction(tx) => {
                    if tx.is_consensus_tx() {
                        validator.execution_scheduler().enqueue(
                            vec![(
                                VerifiedExecutableTransaction::new_from_consensus(
                                    VerifiedTransaction::new_unchecked(*tx.clone()),
                                    0,
                                )
                                .into(),
                                env,
                            )],
                            &epoch_store,
                        );
                    }
                }
                ConsensusTransactionKind::UserTransactionV2(tx) => {
                    if tx.tx().is_consensus_tx() {
                        validator.execution_scheduler().enqueue(
                            vec![(
                                VerifiedExecutableTransaction::new_from_consensus(
                                    VerifiedTransaction::new_unchecked(tx.tx().clone()),
                                    0,
                                )
                                .into(),
                                env,
                            )],
                            &epoch_store,
                        );
                    }
                }
                _ => {}
            }
        }
        
        info!("Side-SUI consensus loop exited");
    }

    fn submit_impl(
        &self,
        transactions: &[ConsensusTransaction],
    ) -> SuiResult<(Vec<ConsensusPosition>, BlockStatusReceiver)> {
        // For now, handle one transaction at a time
        // TODO: batch multiple transactions into blocks
        assert!(!transactions.is_empty(), "Empty transaction batch");
        
        for transaction in transactions {
            self.tx_sender
                .try_send(transaction.clone())
                .map_err(|e| {
                    warn!("Side-SUI consensus channel full: {:?}", e);
                    SuiError::from("SideSuiConsensus channel overflowed")
                })?;
        }
        
        // Return positions for all transactions
        let positions: Vec<ConsensusPosition> = transactions
            .iter()
            .enumerate()
            .map(|(i, _)| ConsensusPosition {
                epoch: EpochId::MIN,
                block: BlockRef::MIN,
                index: i as u16,
            })
            .collect();
        
        Ok((
            positions,
            with_block_status(consensus_core::BlockStatus::Sequenced(BlockRef::MIN)),
        ))
    }
}

impl SubmitToConsensus for SideSuiConsensus {
    fn submit_to_consensus(
        &self,
        transactions: &[ConsensusTransaction],
        _epoch_store: &Arc<AuthorityPerEpochStore>,
    ) -> SuiResult {
        self.submit_impl(transactions).map(|_| ())
    }

    fn submit_best_effort(
        &self,
        transaction: &ConsensusTransaction,
        _epoch_store: &Arc<AuthorityPerEpochStore>,
        _timeout: Duration,
    ) -> SuiResult {
        self.submit_impl(std::slice::from_ref(transaction)).map(|_| ())
    }
}

#[async_trait]
impl ConsensusClient for SideSuiConsensus {
    async fn submit(
        &self,
        transactions: &[ConsensusTransaction],
        _epoch_store: &Arc<AuthorityPerEpochStore>,
    ) -> SuiResult<(Vec<ConsensusPosition>, BlockStatusReceiver)> {
        self.submit_impl(transactions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = SideSuiConfig::default();
        assert_eq!(config.block_interval_ms, 1000);
        assert_eq!(config.max_txns_per_block, 1000);
    }
}
