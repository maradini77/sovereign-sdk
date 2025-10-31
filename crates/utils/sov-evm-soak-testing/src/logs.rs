use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use alloy::{network::Network, providers::Provider};
use alloy_primitives::U256;
use anyhow::Result;
use sov_test_utils::SimpleStorage;
use sov_test_utils::Submit;

use crate::logs;

/// The number of logs emitted by accepted txs so far.
pub static LOGS_RECEIVED_VIA_TX_SUBMIT: AtomicUsize = AtomicUsize::new(0);

pub struct LogsSoakTest<P, N> {
    contract: SimpleStorage::SimpleStorageInstance<P, N>,
    #[allow(dead_code)]
    idx: usize,
}

impl<P, N> LogsSoakTest<P, N>
where
    P: Provider<N> + Clone + Send + Sync,
    N: Network + Send + Sync,
{
    pub async fn new(client: P, idx: usize) -> Result<Self> {
        let address = SimpleStorage::deploy_builder(client.clone())
            .gas(30_000_000)
            .deploy()
            .await?;
        let contract = SimpleStorage::new(address, client);
        Ok(Self { contract, idx })
    }

    pub async fn run(self, tx_count: usize, logs_per_tx: usize) -> Result<()> {
        for i in 1..=tx_count {
            // println!("{}: Sending tx {i} with {logs_per_tx} logs", self.idx);
            self.contract
                .emitLogs(U256::ZERO, U256::from(logs_per_tx))
                .submit()
                .await?;
            LOGS_RECEIVED_VIA_TX_SUBMIT.fetch_add(logs_per_tx, Ordering::Relaxed);
        }
        Ok(())
    }
}
