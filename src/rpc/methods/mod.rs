pub mod accounts;
pub mod assets;
pub mod blocks;
pub mod tokens;
pub mod transactions;

use anyhow::Result;
use jsonrpsee::RpcModule;

use crate::rpc::server::RpcContext;

pub type RpcResult = Result<serde_json::Value, jsonrpsee::types::ErrorObjectOwned>;

/// Build the unified RPC module with all method handlers registered.
pub fn build_rpc_module(context: RpcContext) -> Result<RpcModule<RpcContext>> {
    let mut module = RpcModule::new(context);

    // Block / history methods (replaces Alpamayo)
    blocks::register(&mut module)?;

    // Transaction methods
    transactions::register(&mut module)?;

    // Account state methods (replaces DAS getAccountInfo/getProgramAccounts)
    accounts::register(&mut module)?;

    // Token methods (replaces DAS getTokenAccounts etc)
    tokens::register(&mut module)?;

    // Asset methods (DAS-specific: getAsset, getAssetsByOwner, etc)
    assets::register(&mut module)?;

    tracing::info!(
        methods = module.method_names().count(),
        "rpc module initialized"
    );

    Ok(module)
}
