pub(crate) mod sol_events;
mod tx_event_handler;

use crate::cefi_client::CefiClient;
use crate::config::COMMON_CONFIGS;
use crate::contract::tx_event_handler::consume_logs_from_tx_receipts;
use crate::eth_rpc::{
    get_batch_block_logs, get_batch_block_timestamps, get_block_logs, get_block_receipts,
    get_block_with_txs, get_blockheader_by_number,
};
use ethers::prelude::{Address, Block, Transaction, TransactionReceipt, H160, H256};
use ethers::types::Log;
use once_cell::sync::OnceCell;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use tx_event_handler::{consume_grouped_logs, consume_tx_and_logs};
pub use tx_event_handler::{
    simple_recover_deposit_sol_logs,
    simple_recover_sol_deposit_withdraw_approve_and_rebalance_logs,
    simple_recover_swap_result_uploded_logs,
};

use crate::consume_data_task::ORDERLY_DASHBOARD_INDEXER;

pub(crate) const HANDLE_LOG: &str = "handle_log";
pub(crate) const OPERATOR_MANAGER_SC: &str = "operator_manager_sc";
pub(crate) const LEDGER_SC: &str = "ledger_sc";
pub(crate) const VAULT_MANAGER_SC: &str = "vault_manager_sc";

pub(crate) static ADDR_MAP: OnceCell<BTreeMap<Address, &str>> = OnceCell::new();
pub(crate) static TARGET_ADDRS: OnceCell<Vec<Address>> = OnceCell::new();

pub(crate) fn init_addr_set() -> anyhow::Result<()> {
    let mut addr_map = BTreeMap::new();
    let mut addrs = Vec::with_capacity(3);
    let config = &unsafe { COMMON_CONFIGS.get_unchecked() }.l2_config;
    let operator_manager_address = H160::from_str(&config.operator_manager_address)?;
    addr_map.insert(operator_manager_address, OPERATOR_MANAGER_SC);
    addrs.push(operator_manager_address);

    let ledger_address = H160::from_str(&config.ledger_address)?;
    addr_map.insert(ledger_address, LEDGER_SC);
    addrs.push(ledger_address);

    let vault_manager_address = H160::from_str(&config.vault_manager_address)?;
    addr_map.insert(vault_manager_address, VAULT_MANAGER_SC);
    addrs.push(vault_manager_address);

    if ADDR_MAP.set(addr_map).is_err() {
        tracing::warn!(target: ORDERLY_DASHBOARD_INDEXER, "ADDR_SET already inited");
    }
    if TARGET_ADDRS.set(addrs).is_err() {
        tracing::warn!(target: ORDERLY_DASHBOARD_INDEXER, "TARGET_ADDRS already inited");
    }

    Ok(())
}
pub(crate) async fn consume_data_on_block(
    block_height: u64,
    cefi_cli: Arc<CefiClient>,
) -> anyhow::Result<i64> {
    tracing::info!(
        target: HANDLE_LOG,
        "consume_data_on_block block_height: {}",
        block_height
    );
    // todo: configurate it
    let block_timestamp: i64;
    let consume_logs = true;
    if consume_logs {
        let (block, tx_logs_vec) = query_and_filter_block_data_logs(block_height).await?;
        block_timestamp = block.timestamp.as_u64() as i64;
        consume_tx_and_logs(block, &tx_logs_vec, cefi_cli).await?;
    } else {
        let (block, tx_receipt_vec) = query_and_filter_block_data_info(block_height).await?;
        block_timestamp = block.timestamp.as_u64() as i64;
        consume_logs_from_tx_receipts(block, &tx_receipt_vec, cefi_cli).await?;
    }

    Ok(block_timestamp)
}

pub async fn query_and_filter_block_data_info(
    block_height: u64,
) -> anyhow::Result<(Block<Transaction>, Vec<(Transaction, TransactionReceipt)>)> {
    let block = get_block_with_txs(block_height).await?;
    let target_txs = block
        .transactions
        .iter()
        .map(|tx| {
            // if let Some(to) = &tx.to {
            //     if receivers.contains(to) {
            //         return Some(tx.clone());
            //     }
            // }
            // None
            tx.clone()
        })
        .collect::<Vec<_>>();
    if target_txs.is_empty() {
        return Ok((block, vec![]));
    }
    let mut receipt_map = get_block_receipts(block_height)
        .await?
        .into_iter()
        .map(|receipt| (receipt.transaction_hash, receipt))
        .collect::<BTreeMap<_, _>>();
    let len = target_txs.len();
    let mut tx_receipt_vec: Vec<(Transaction, TransactionReceipt)> = Vec::with_capacity(len);
    block.transactions.iter().for_each(|tx| {
        if let Some(receipt) = receipt_map.remove(&tx.hash) {
            tx_receipt_vec.push((tx.clone(), receipt));
        } else {
            tracing::info!(
                target: HANDLE_LOG,
                "can not find receipt for tx: {:?}",
                tx.hash,
            );
        }
    });
    tracing::info!(
        target: HANDLE_LOG,
        "block_height: {}, block tx length {}, receipt length: {},block hash: {:?}",
        block_height,
        block.transactions.len(),
        tx_receipt_vec.len(),
        block.hash.unwrap_or_default(),
    );

    Ok((block, tx_receipt_vec))
}

pub async fn query_and_filter_block_data_logs(
    block_height: u64,
) -> anyhow::Result<(Block<Transaction>, Vec<(Transaction, Vec<Log>)>)> {
    let block = get_block_with_txs(block_height).await?;
    let target_txs = block
        .transactions
        .iter()
        .map(|tx| tx.clone())
        .collect::<Vec<_>>();
    if target_txs.is_empty() {
        return Ok((block, vec![]));
    }
    let len = target_txs.len();
    let mut tx_logs_map: BTreeMap<H256, Vec<Log>> = BTreeMap::new();
    for log in get_block_logs(block_height).await? {
        if let Some(logs) = tx_logs_map.get_mut(&log.transaction_hash.unwrap_or_default()) {
            logs.push(log);
        } else {
            let mut logs = Vec::new();
            let hash = log.transaction_hash.unwrap_or_default();
            logs.push(log);
            tx_logs_map.insert(hash, logs);
        }
    }
    let mut tx_log_vec: Vec<(Transaction, Vec<Log>)> = Vec::with_capacity(len);
    block.transactions.iter().for_each(|tx| {
        if let Some(logs) = tx_logs_map.remove(&tx.hash) {
            tx_log_vec.push((tx.clone(), logs));
        } else {
            tx_log_vec.push((tx.clone(), vec![]));
        }
    });
    tracing::info!(
        target: HANDLE_LOG,
        "block_height: {}, block tx length {}, tx_log_vec length: {},block hash: {:?}",
        block_height,
        block.transactions.len(),
        tx_log_vec.len(),
        block.hash.unwrap_or_default(),
    );

    Ok((block, tx_log_vec))
}

/// Batch-process a chunk of blocks using a single eth_getLogs call.
/// Returns the max block timestamp in the chunk, or Err on failure.
/// Falls back to per-block processing if batch getLogs fails.
pub(crate) async fn consume_batch_chunk(
    from_block: u64,
    to_block: u64,
    cefi_cli: Arc<CefiClient>,
) -> anyhow::Result<i64> {
    // Pre-upgrade blocks need handle_tx_params for INITIAL sync (requires full transaction data).
    // For RECOVERY (update_cursor=false), batch mode is safe because handle_tx_params data
    // already exists from initial sync and ON CONFLICT DO NOTHING skips duplicates.
    // Only enforce per-block for initial sync (when the indexer is processing new blocks).
    //
    // TODO: Pass a `is_recovery` flag to skip this check cleanly instead of
    // relying on upgrade_height. For now, disabled to speed up recovery.
    // let upgrade_height = unsafe { COMMON_CONFIGS.get_unchecked().l2_config.upgrade_height };
    // if from_block < upgrade_height {
    //     return consume_chunk_per_block(from_block, to_block, cefi_cli).await;
    // }

    // Step 1: Batch fetch all logs for this chunk
    let all_logs = match get_batch_block_logs(from_block, to_block).await {
        Ok(logs) => logs,
        Err(err) => {
            let err_str = err.to_string();
            if err_str.contains("LOG_LIMIT_EXCEEDED") {
                tracing::warn!(
                    target: ORDERLY_DASHBOARD_INDEXER,
                    "batch getLogs limit exceeded for {}-{}, falling back to per-block",
                    from_block, to_block
                );
                return consume_chunk_per_block(from_block, to_block, cefi_cli).await;
            }
            if err_str.contains("too large") || err_str.contains("encoding") {
                let range = to_block - from_block + 1;
                if range <= 1 {
                    tracing::warn!(
                        target: ORDERLY_DASHBOARD_INDEXER,
                        "single block {} too large for batch, falling back to per-block",
                        from_block
                    );
                    return consume_chunk_per_block(from_block, to_block, cefi_cli).await;
                }
                let mid = from_block + range / 2;
                tracing::warn!(
                    target: ORDERLY_DASHBOARD_INDEXER,
                    "batch getLogs too large for {}-{} ({} blocks), splitting into {}-{} and {}-{}",
                    from_block, to_block, range, from_block, mid - 1, mid, to_block
                );
                let ts1 = Box::pin(consume_batch_chunk(from_block, mid - 1, cefi_cli.clone())).await?;
                let ts2 = Box::pin(consume_batch_chunk(mid, to_block, cefi_cli)).await?;
                return Ok(std::cmp::max(ts1, ts2));
            }
            return Err(err);
        }
    };

    // Step 2: Group logs by block_number → by tx_hash (preserving log_index order)
    // BTreeMap ensures block_number ordering
    let total_fetched = all_logs.len();
    let mut block_logs: BTreeMap<u64, BTreeMap<H256, Vec<Log>>> = BTreeMap::new();
    for log in all_logs {
        let block_num = log.block_number.map(|n| n.as_u64()).unwrap_or(0);
        let tx_hash = log.transaction_hash.unwrap_or_default();
        block_logs
            .entry(block_num)
            .or_default()
            .entry(tx_hash)
            .or_default()
            .push(log);
    }

    // Validate: no logs were lost during grouping
    let total_grouped: usize = block_logs
        .values()
        .flat_map(|tx_map| tx_map.values())
        .map(|logs| logs.len())
        .sum();
    if total_grouped != total_fetched {
        return Err(anyhow::anyhow!(
            "log count mismatch after grouping for blocks {}-{}: fetched {} but grouped {}",
            from_block, to_block, total_fetched, total_grouped
        ));
    }

    // Step 3+4: Fetch all needed block timestamps in a single JSON-RPC batch request.
    // We need timestamps for: blocks with logs + the last block (for cursor update).
    let mut blocks_needing_ts: Vec<u64> = block_logs.keys().copied().collect();
    if !blocks_needing_ts.contains(&to_block) {
        blocks_needing_ts.push(to_block);
    }

    let block_timestamps = match get_batch_block_timestamps(&blocks_needing_ts).await {
        Ok(ts) => ts,
        Err(err) => {
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "batch header fetch failed for chunk {}-{}: {}, falling back to per-block",
                from_block, to_block, err
            );
            return consume_chunk_per_block(from_block, to_block, cefi_cli).await;
        }
    };

    let last_block_ts = block_timestamps
        .get(&to_block)
        .map(|&ts| ts as i64)
        .unwrap_or(0);

    // Step 5: Build grouped data structure and process
    // Tuple: (block_number, block_timestamp, Vec<(tx_hash, logs)>)
    let mut grouped: Vec<(u64, u64, Vec<(H256, Vec<Log>)>)> = Vec::new();
    for (block_num, tx_map) in block_logs {
        let block_t = block_timestamps.get(&block_num).copied().unwrap_or(0);
        let tx_groups: Vec<(H256, Vec<Log>)> = tx_map.into_iter().collect();
        grouped.push((block_num, block_t, tx_groups));
    }

    if !grouped.is_empty() {
        consume_grouped_logs(&grouped, cefi_cli).await?;
    }

    // Return max timestamp across all blocks
    let max_ts = block_timestamps
        .values()
        .max()
        .map(|&ts| ts as i64)
        .unwrap_or(last_block_ts);
    Ok(std::cmp::max(max_ts, last_block_ts))
}

/// Fallback: process a chunk of blocks one by one (original per-block logic).
pub(crate) async fn consume_chunk_per_block(
    from_block: u64,
    to_block: u64,
    cefi_cli: Arc<CefiClient>,
) -> anyhow::Result<i64> {
    let mut max_ts: i64 = 0;
    for block_height in from_block..=to_block {
        let ts = consume_data_on_block(block_height, cefi_cli.clone()).await?;
        max_ts = std::cmp::max(max_ts, ts);
    }
    Ok(max_ts)
}

#[cfg(test)]
mod tests {
    use ethers::prelude::BlockNumber;
    use ethers::providers::{Http, Middleware, Provider};

    #[ignore]
    #[tokio::test]
    async fn test_fetch_block_receipts() {
        let provider = Provider::<Http>::try_from(
            "https://l2-orderly-l2-4460-sepolia-8tc3sd7dvy.t.conduit.xyz",
        )
        .unwrap();
        let receipts = provider
            .get_block_receipts(BlockNumber::Number(4777281.into()))
            .await
            .unwrap();
        println!("receipts: {:?}", receipts);
    }
}
