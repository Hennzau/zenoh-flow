use std::path::PathBuf;

use anyhow::anyhow;
use clap::Parser;

use zenoh::{key_expr::KeyExpr, Result};

#[derive(Parser)]
struct TopicSender {
    /// The path to a Zenoh configuration to manage the connection to the Zenoh
    /// network.
    ///
    /// If no configuration is provided, `zfctl` will default to connecting as
    /// a peer with multicast scouting enabled.
    #[arg(short = 'z', long, verbatim_doc_comment)]
    zenoh_configuration: Option<PathBuf>,

    /// The key_expression to use for the `put` operation.
    #[arg(verbatim_doc_comment)]
    key_expression: KeyExpr<'static>,

    /// The payload to use for the `put` operation.
    #[arg(verbatim_doc_comment)]
    payload: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = TopicSender::parse();

    let zenoh_config = match args.zenoh_configuration {
        Some(path) => zenoh::config::Config::from_file(path.clone()).map_err(|e| {
            anyhow!(
                "Failed to parse the Zenoh configuration from < {} >:\n{e:?}",
                path.display()
            )
        })?,
        None => zenoh::config::Config::default(),
    };

    let session = zenoh::open(zenoh_config)
        .await
        .map_err(|e| anyhow!("Failed to open Zenoh session:\n{:?}", e))?;

    session
        .put(&args.key_expression, args.payload)
        .await
        .map_err(|e| anyhow!("Failed to put a sample:\n{:?}", e))?;

    Ok(())
}
