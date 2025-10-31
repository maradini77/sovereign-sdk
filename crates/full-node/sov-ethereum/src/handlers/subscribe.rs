use crate::to_jsonrpsee_error_object;
use crate::Ethereum;
use alloy_rpc_types::pubsub::Params;
use alloy_rpc_types::pubsub::SubscriptionKind;
use alloy_rpc_types::Filter;
use alloy_rpc_types::FilterBlockOption;
use jsonrpsee::types::Params as JRpcParams;
use jsonrpsee::PendingSubscriptionSink;
use jsonrpsee::SubscriptionMessage;
use jsonrpsee::{Extensions, SubscriptionSink};
use sov_address::{EthereumAddress, FromVmAddress};
pub use sov_evm::EthereumAuthenticator;
use sov_evm::Evm;
use sov_modules_api::capabilities::HasKernel;
use sov_modules_api::Spec;
use sov_sequencer::Sequencer;
use std::sync::Arc;
use thiserror::Error;

use crate::handlers::ETH_RPC_ERROR;

pub async fn eth_subscribe<S, Seq>(
    parameters: JRpcParams<'static>,
    pending: PendingSubscriptionSink,
    ethereum: Arc<Ethereum<S, Seq>>,
    _: Extensions,
) -> jsonrpsee::core::SubscriptionResult
where
    S: Spec,
    Seq: Sequencer<Spec = S>,
    S::Address: FromVmAddress<EthereumAddress>,
    Seq::Rt: HasKernel<S> + EthereumAuthenticator<S> + Default + Send + Sync + 'static,
{
    let mut parameters = parameters.sequence();
    let kind: SubscriptionKind = parameters.next()?;
    let params: Params = parameters.optional_next()?.unwrap_or_default();

    let log_filter = match validate_params_for_log_subscription(kind, params) {
        Ok(log_filter) => log_filter,
        Err(e) => {
            let rpc_err = to_jsonrpsee_error_object(e, ETH_RPC_ERROR);
            pending.reject(rpc_err).await;
            return Ok(());
        }
    };

    let accepted_sink = pending.accept().await?;

    let _task = tokio::spawn(async move {
        stream_logs(accepted_sink, log_filter, ethereum.clone()).await;
    });

    Ok(())
}

async fn stream_logs<S, Seq>(
    mut accepted_sink: SubscriptionSink,
    filter: Box<Filter>,
    ethereum: Arc<Ethereum<S, Seq>>,
) where
    S: Spec,
    Seq: Sequencer<Spec = S>,
    S::Address: FromVmAddress<EthereumAddress>,
    Seq::Rt: HasKernel<S> + EthereumAuthenticator<S> + Default + Send + Sync + 'static,
{
    let evm = Evm::<S>::default();
    let state = &mut ethereum.api_state_accessor();

    let pending_block = evm.pending_block(state);
    let mut prev_last_tx_index = pending_block.transactions.end;

    // Fetch the initial block. If it’s stale, it will be replaced below.
    let start_block = pending_block.header.number - 1;
    let Some(mut block) = evm.get_maybe_sealed_block(start_block, state) else {
        tracing::error!(start_block, "Block does not exist");
        return;
    };

    let state_updates = &mut ethereum.sequencer.api_state().checkpoint_receiver();

    let mut iters = 0;
    loop {
        tokio::select! {
             _ = accepted_sink.closed() => {
                break;
            }
            updated = state_updates.changed() => {
                if updated.is_err() {
                    break;
                }
                let start = std::time::Instant::now();
                let state = &mut ethereum.api_state_accessor();
                let state_clone_time = start.elapsed();

                let pending_block = evm.pending_block(state);
                let pending_block_time = start.elapsed() - state_clone_time;
                let curr_last_tx_index = pending_block.transactions.end;

                if curr_last_tx_index <= prev_last_tx_index {
                    continue;
                }

                let mut receipt_fetch = std::time::Duration::ZERO;
                let mut process_receipt_time = std::time::Duration::ZERO;
                let mut serde_time = std::time::Duration::ZERO;
                let mut actual_send_time = std::time::Duration::ZERO;
                let num_txs = curr_last_tx_index - prev_last_tx_index;
                for index in prev_last_tx_index..curr_last_tx_index {

                    let inner_start = std::time::Instant::now();
                    let Some(receipt) = evm.receipt(index, state) else {
                        // This can happen if the state was pruned.
                        tracing::error!(index, "Receipt does not exist");
                        return;
                    };
                    let receipt_fetch_inner = inner_start.elapsed();
                    receipt_fetch += receipt_fetch_inner;

                    if block.number() != receipt.block_number {
                        match evm.get_maybe_sealed_block(receipt.block_number, state) {
                            Some(b) => block = b,
                            None => {
                                tracing::error!(
                                    block_number = receipt.block_number,
                                    "Block does not exist"
                                );
                                return;
                            }
                        }
                    }

                    let transaction_index = index - block.transactions_start();

                    let process_receipt_start = std::time::Instant::now();
                    for (log_index_in_tx, log) in receipt.receipt.logs.into_iter().enumerate() {
                        let serde_start = std::time::Instant::now();
                        if filter.matches(&log) {
                            let rpc_log = alloy_rpc_types::Log {
                                inner: log,
                                block_hash: block.hash(),
                                block_number: Some(block.number()),
                                block_timestamp: Some(block.timestamp()),
                                transaction_hash: Some(receipt.transaction_hash),
                                transaction_index: Some(receipt.transaction_index),
                                log_index: Some(receipt.log_index_start + log_index_in_tx as u64),
                                removed: false,
                            };

                            assert_eq!(receipt.transaction_index, transaction_index);

                            let msg = SubscriptionMessage::new(
                                accepted_sink.method_name(),
                                accepted_sink.subscription_id(),
                                &rpc_log,
                            )
                            .unwrap_or_else(|err| {
                                panic!("Impossible: can't serialize log. Log: {rpc_log:?}, Err: {err:?}",)
                            });
                            serde_time += serde_start.elapsed();

                            let send_start = std::time::Instant::now();
                            if let Err(err) = accepted_sink.send(msg).await {
                                // if let jsonrpsee::TrySendError::Full(_) = err {
                                //     // tracing::info!("The subscription channel Filled up. Dropping the message.");
                                //     continue;
                                // }
                                tracing::info!(%err, "The subscription client disconnected from the server.");
                                return;
                            }
                            actual_send_time += send_start.elapsed();
                        }
                    }
                    process_receipt_time += process_receipt_start.elapsed();
                }
                iters += 1;
                    let state_clone_time = state_clone_time.as_micros();
                    let pending_block_time = pending_block_time.as_micros();
                    let receipt_fetch = receipt_fetch.as_micros();
                    let process_receipt_time = process_receipt_time.as_micros();
                    let serde_time = serde_time.as_micros();
                    let actual_send_time = actual_send_time.as_micros();
                    tracing::info!("Iteration {iters}. Did {num_txs} txs: State clone time: {state_clone_time}µs, Pending block time: {pending_block_time}µs, Receipt fetch time: {receipt_fetch}µs, Process receipt time: {process_receipt_time}µs, Serde time: {serde_time}µs, Actual send time: {actual_send_time}µs");
                prev_last_tx_index = curr_last_tx_index;
            }
        }
    }
}

#[derive(Error, Debug)]
enum ParamsValidationError {
    #[error("Block Option parameters are not supported in LOG subscriptions. Please use eth_getLogs or eth_getLogsWithCursor")]
    BlockOptionParam,
    #[error("Boolean parameters are not supported in LOG subscriptions")]
    BoolParam,
    #[error("Only LOG subscriptions are supported")]
    OnlyLogSubscription,
}

fn validate_params_for_log_subscription(
    kind: SubscriptionKind,
    params: Params,
) -> Result<Box<Filter>, ParamsValidationError> {
    if kind != SubscriptionKind::Logs {
        return Err(ParamsValidationError::OnlyLogSubscription);
    }
    match params {
        Params::Logs(filter) => {
            if filter.block_option == FilterBlockOption::default() {
                Ok(filter)
            } else {
                Err(ParamsValidationError::BlockOptionParam)
            }
        }
        Params::Bool(_) => Err(ParamsValidationError::BoolParam),
        Params::None => Ok(Default::default()),
    }
}
