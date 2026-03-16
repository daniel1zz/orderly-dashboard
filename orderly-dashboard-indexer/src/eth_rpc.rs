use crate::{
    bindings::{
        market_manager::{FundingDataFilter, MarketDataFilter},
        operator_manager::{
            EventUpload1Filter, EventUpload2Filter, FuturesTradeUpload1Filter,
            FuturesTradeUpload2Filter,
        },
        user_ledger::{
            AccountDeposit1Filter, AccountDeposit2Filter, AccountDepositSolFilter,
            AccountWithdrawApprove1Filter, AccountWithdrawApprove2Filter,
            AccountWithdrawFail1Filter, AccountWithdrawFail2Filter, AccountWithdrawFinish1Filter,
            AccountWithdrawFinish2Filter, AccountWithdrawSolApproveFilter,
            AccountWithdrawSolFailFilter, AdlResultFilter, AdlResultV2Filter,
            BalanceTransferFilter, FeeDistributionFilter,
            LiquidationResultFilter, LiquidationResultV2Filter, LiquidationTransferFilter,
            LiquidationTransferV2Filter, ProcessValidatedFutures1Filter,
            ProcessValidatedFutures2Filter, ProcessValidatedFuturesV3Filter,
            SettlementExecutionFilter, SettlementExecutionV3Filter,
            SettlementResultFilter, SettlementResultV3Filter,
            SwapResultUploadedFilter,
            AdlResultV3Filter, LiquidationResultV3Filter,
            LiquidationTransferV3Filter, MarginTransferV3Filter,
        },
        vault_manager::{
            RebalanceBurnFilter, RebalanceBurnResultFilter, RebalanceMintFilter,
            RebalanceMintResultFilter,
        },
    },
    config::COMMON_CONFIGS,
    contract::TARGET_ADDRS,
};
use anyhow::Result;
use ethers::prelude::{
    Block, BlockId, BlockNumber, Filter, Log, Transaction, TransactionReceipt, H256,
};
use ethers::providers::{Http, Middleware, Provider};
use ethers_contract::EthEvent;
use once_cell::sync::OnceCell;
use std::time::Duration;
use tokio::time::timeout;

use crate::consume_data_task::ORDERLY_DASHBOARD_INDEXER;
pub(crate) static PROVIDER: OnceCell<Provider<Http>> = OnceCell::new();
/// Raw HTTP client + RPC URL for JSON-RPC batch requests (not supported by ethers Provider).
static RPC_CLIENT: OnceCell<reqwest::Client> = OnceCell::new();
static RPC_URL: OnceCell<String> = OnceCell::new();

pub(crate) fn init_provider() -> Result<()> {
    let rpc = if let Ok(orderly_rpc) = std::env::var("ORDERLY_RPC") {
        orderly_rpc
    } else {
        unsafe { &COMMON_CONFIGS.get_unchecked().l2_config.rpc_url }.clone()
    };
    tracing::info!(target: ORDERLY_DASHBOARD_INDEXER, "rpc wrapped: {}", rpc[0..8].to_string() + "***" + &rpc[rpc.len() - 5..]);
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;
    RPC_CLIENT.set(client.clone()).ok();
    RPC_URL.set(rpc.clone()).ok();
    let http_client = Http::new_with_client(url::Url::parse(&rpc)?, client);
    let provider = Provider::new(http_client);
    PROVIDER.set(provider).ok();
    Ok(())
}

pub(crate) fn clone_provider() -> Provider<Http> {
    unsafe { PROVIDER.get_unchecked() }.clone()
}

pub async fn get_latest_block_num() -> Result<u64> {
    Ok(get_blocknumber_with_timeout().await?)
}

pub async fn get_blocknumber_with_timeout() -> Result<u64> {
    let provider = unsafe { PROVIDER.get_unchecked() };

    let result = timeout(Duration::from_secs(3), provider.get_block_number()).await;
    match result {
        Err(_) => {
            return Err(anyhow::anyhow!("request elapsed"));
        }
        Ok(Ok(number)) => {
            return Ok(number.as_u64());
        }
        Ok(Err(err)) => {
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "get_blocknumber query err: {}",
                err
            );
            return Err(anyhow::anyhow!("query err"));
        }
    }
}

pub async fn get_block_with_txs(block_num: u64) -> Result<Block<Transaction>> {
    let provider = unsafe { PROVIDER.get_unchecked() };

    let result = timeout(
        Duration::from_secs(8),
        provider.get_block_with_txs(BlockId::Number(BlockNumber::Number(block_num.into()))),
    )
    .await;
    match result {
        Err(_) => {
            return Err(anyhow::anyhow!("get_block_with_txs request elapsed for 8s"));
        }
        Ok(Ok(block)) => {
            return Ok(
                block.ok_or_else(|| anyhow::anyhow!("block not found in calling block api"))?
            );
        }
        Ok(Err(err)) => {
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "get_block_with_txs query err: {}",
                err
            );
            return Err(anyhow::anyhow!("query err"));
        }
    }
}

#[allow(dead_code)]
pub async fn get_tx_receipt(tx_hash: H256) -> Result<TransactionReceipt> {
    let provider = unsafe { PROVIDER.get_unchecked() };

    let result = timeout(
        Duration::from_secs(3),
        provider.get_transaction_receipt(tx_hash),
    )
    .await;
    match result {
        Err(_) => {
            return Err(anyhow::anyhow!("get_tx_receipt request elapsed"));
        }
        Ok(Ok(receipt)) => {
            return Ok(
                receipt.ok_or_else(|| anyhow::anyhow!("receipt not found in calling block api"))?
            );
        }
        Ok(Err(err)) => {
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "get_tx_receipt query err: {}",
                err
            );
            return Err(anyhow::anyhow!("get_tx_receipt query err:{}", err));
        }
    }
}

pub async fn get_block_receipts(block_num: u64) -> Result<Vec<TransactionReceipt>> {
    let provider = unsafe { PROVIDER.get_unchecked() };

    let result = timeout(
        Duration::from_secs(8),
        provider.get_block_receipts(BlockNumber::Number(block_num.into())),
    )
    .await;
    match result {
        Err(_) => {
            return Err(anyhow::anyhow!("get_block_receipts request elapsed"));
        }
        Ok(Ok(receipts)) => {
            return Ok(receipts);
        }
        Ok(Err(err)) => {
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "get_block_receipts query err: {}",
                err
            );
            return Err(anyhow::anyhow!("get_block_receipts query err:{}", err));
        }
    }
}

pub async fn get_block_logs(block_num: u64) -> Result<Vec<Log>> {
    let provider = unsafe { PROVIDER.get_unchecked() };
    let topic0 = vec![
        AccountDeposit1Filter::signature(),
        AccountDeposit2Filter::signature(),
        AccountWithdrawFinish1Filter::signature(),
        AccountWithdrawFinish2Filter::signature(),
        AccountWithdrawApprove1Filter::signature(),
        AccountWithdrawApprove2Filter::signature(),
        AccountWithdrawSolFailFilter::signature(),
        AccountWithdrawFail1Filter::signature(),
        AccountWithdrawFail2Filter::signature(),
        ProcessValidatedFutures1Filter::signature(),
        ProcessValidatedFutures2Filter::signature(),
        ProcessValidatedFuturesV3Filter::signature(),
        EventUpload1Filter::signature(),
        EventUpload2Filter::signature(),
        FuturesTradeUpload1Filter::signature(),
        FuturesTradeUpload2Filter::signature(),
        MarketDataFilter::signature(),
        FundingDataFilter::signature(),
        FeeDistributionFilter::signature(),
        SettlementResultFilter::signature(),
        SettlementExecutionFilter::signature(),
        LiquidationTransferFilter::signature(),
        LiquidationResultFilter::signature(),
        LiquidationResultV2Filter::signature(),
        LiquidationTransferV2Filter::signature(),
        AdlResultFilter::signature(),
        AdlResultV2Filter::signature(),
        AdlResultV3Filter::signature(),
        BalanceTransferFilter::signature(),
        AccountDepositSolFilter::signature(),
        AccountWithdrawSolApproveFilter::signature(),
        RebalanceBurnFilter::signature(),
        RebalanceBurnResultFilter::signature(),
        RebalanceMintFilter::signature(),
        RebalanceMintResultFilter::signature(),
        SwapResultUploadedFilter::signature(),
        LiquidationResultV3Filter::signature(),
        LiquidationTransferV3Filter::signature(),
        SettlementExecutionV3Filter::signature(),
        SettlementResultV3Filter::signature(),
        MarginTransferV3Filter::signature(),
    ];
    let address = unsafe { TARGET_ADDRS.get_unchecked() }.clone();
    let filter = Filter::new()
        .from_block(block_num)
        .to_block(block_num)
        .address(address)
        .topic0(topic0);
    let result = timeout(Duration::from_secs(8), provider.get_logs(&filter)).await;
    match result {
        Err(_) => {
            return Err(anyhow::anyhow!("get_block_receipts request elapsed"));
        }
        Ok(Ok(receipts)) => {
            return Ok(receipts);
        }
        Ok(Err(err)) => {
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "get_block_receipts query err: {}",
                err
            );
            return Err(anyhow::anyhow!("get_block_receipts query err:{}", err));
        }
    }
}

/// Batch version of get_block_logs: fetches logs for a range of blocks in a single RPC call.
/// Uses the same address + topic0 filters as get_block_logs.
pub async fn get_batch_block_logs(from_block: u64, to_block: u64) -> Result<Vec<Log>> {
    let provider = unsafe { PROVIDER.get_unchecked() };
    let topic0 = vec![
        AccountDeposit1Filter::signature(),
        AccountDeposit2Filter::signature(),
        AccountWithdrawFinish1Filter::signature(),
        AccountWithdrawFinish2Filter::signature(),
        AccountWithdrawApprove1Filter::signature(),
        AccountWithdrawApprove2Filter::signature(),
        AccountWithdrawSolFailFilter::signature(),
        AccountWithdrawFail1Filter::signature(),
        AccountWithdrawFail2Filter::signature(),
        ProcessValidatedFutures1Filter::signature(),
        ProcessValidatedFutures2Filter::signature(),
        ProcessValidatedFuturesV3Filter::signature(),
        EventUpload1Filter::signature(),
        EventUpload2Filter::signature(),
        FuturesTradeUpload1Filter::signature(),
        FuturesTradeUpload2Filter::signature(),
        MarketDataFilter::signature(),
        FundingDataFilter::signature(),
        FeeDistributionFilter::signature(),
        SettlementResultFilter::signature(),
        SettlementExecutionFilter::signature(),
        LiquidationTransferFilter::signature(),
        LiquidationResultFilter::signature(),
        LiquidationResultV2Filter::signature(),
        LiquidationTransferV2Filter::signature(),
        AdlResultFilter::signature(),
        AdlResultV2Filter::signature(),
        AdlResultV3Filter::signature(),
        BalanceTransferFilter::signature(),
        AccountDepositSolFilter::signature(),
        AccountWithdrawSolApproveFilter::signature(),
        RebalanceBurnFilter::signature(),
        RebalanceBurnResultFilter::signature(),
        RebalanceMintFilter::signature(),
        RebalanceMintResultFilter::signature(),
        SwapResultUploadedFilter::signature(),
        LiquidationResultV3Filter::signature(),
        LiquidationTransferV3Filter::signature(),
        SettlementExecutionV3Filter::signature(),
        SettlementResultV3Filter::signature(),
        MarginTransferV3Filter::signature(),
    ];
    let address = unsafe { TARGET_ADDRS.get_unchecked() }.clone();
    let filter = Filter::new()
        .from_block(from_block)
        .to_block(to_block)
        .address(address)
        .topic0(topic0);
    let result = timeout(Duration::from_secs(30), provider.get_logs(&filter)).await;
    match result {
        Err(_) => Err(anyhow::anyhow!(
            "get_batch_block_logs request elapsed for blocks {}-{}",
            from_block,
            to_block
        )),
        Ok(Ok(logs)) => Ok(logs),
        Ok(Err(err)) => {
            let err_str = err.to_string();
            if err_str.contains("limit exceeded") || err_str.contains("-32005") {
                return Err(anyhow::anyhow!("LOG_LIMIT_EXCEEDED"));
            }
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "get_batch_block_logs query err for blocks {}-{}: {}",
                from_block, to_block, err
            );
            Err(anyhow::anyhow!("get_batch_block_logs query err: {}", err))
        }
    }
}

pub async fn get_blockheader_by_number(number: u64) -> Result<Block<H256>> {
    let provider = unsafe { PROVIDER.get_unchecked() };

    let result = timeout(
        Duration::from_secs(3),
        provider.get_block(BlockNumber::Number(number.into())),
    )
    .await;
    match result {
        Err(_) => {
            return Err(anyhow::anyhow!("request elapsed"));
        }
        Ok(Ok(Some(block))) => {
            return Ok(block);
        }
        Ok(Ok(None)) => {
            return Err(anyhow::anyhow!("block number found for number: {}", number));
        }
        Ok(Err(err)) => {
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "get_blockheader_by_number query err: {}",
                err
            );
            return Err(anyhow::anyhow!("query err"));
        }
    }
}

/// Batch fetch block headers (timestamps) for multiple block numbers in a single JSON-RPC batch request.
/// Returns a map of block_number → timestamp (as u64 seconds).
/// This replaces N individual `eth_getBlockByNumber` calls with 1 HTTP request.
pub async fn get_batch_block_timestamps(block_numbers: &[u64]) -> Result<std::collections::BTreeMap<u64, u64>> {
    use serde_json::json;

    if block_numbers.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }

    let client = unsafe { RPC_CLIENT.get_unchecked() };
    let url = unsafe { RPC_URL.get_unchecked() };

    // Build JSON-RPC batch request
    let batch: Vec<serde_json::Value> = block_numbers
        .iter()
        .enumerate()
        .map(|(i, &num)| {
            json!({
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [format!("0x{:x}", num), false],
                "id": i + 1
            })
        })
        .collect();

    let response = timeout(
        Duration::from_secs(30),
        client.post(url).json(&batch).send(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("batch block headers request elapsed"))?
    .map_err(|err| anyhow::anyhow!("batch block headers HTTP error: {}", err))?;

    let results: Vec<serde_json::Value> = response
        .json()
        .await
        .map_err(|err| anyhow::anyhow!("batch block headers parse error: {}", err))?;

    // JSON-RPC batch responses may arrive in any order — match by `id` field, not array index.
    // We sent id = i+1 where i is the index into block_numbers.
    let mut timestamps = std::collections::BTreeMap::new();
    for result in &results {
        let id = result
            .get("id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| anyhow::anyhow!("batch header: response missing id field"))?;
        let idx = (id as usize).checked_sub(1).ok_or_else(|| {
            anyhow::anyhow!("batch header: invalid id {}", id)
        })?;
        if idx >= block_numbers.len() {
            continue;
        }
        let block_num = block_numbers[idx];

        if let Some(error) = result.get("error") {
            tracing::warn!(
                target: ORDERLY_DASHBOARD_INDEXER,
                "batch header error for block {}: {:?}",
                block_num, error
            );
            return Err(anyhow::anyhow!(
                "batch header RPC error for block {}: {:?}",
                block_num, error
            ));
        }

        let block_obj = result.get("result").ok_or_else(|| {
            anyhow::anyhow!("batch header: no result for block {}", block_num)
        })?;

        if block_obj.is_null() {
            return Err(anyhow::anyhow!(
                "batch header: block {} not found",
                block_num
            ));
        }

        let ts_hex = block_obj
            .get("timestamp")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                anyhow::anyhow!("batch header: no timestamp for block {}", block_num)
            })?;

        let ts = u64::from_str_radix(ts_hex.trim_start_matches("0x"), 16)
            .map_err(|e| anyhow::anyhow!("batch header: bad timestamp hex for block {}: {}", block_num, e))?;

        timestamps.insert(block_num, ts);
    }

    Ok(timestamps)
}
