//
// Copyright © 2021 ZettaScale Technology <contact@zettascale.tech>
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use std::{path::PathBuf, time::Duration};

use anyhow::{anyhow, Context};
use async_std::io::ReadExt;
use clap::Subcommand;
use comfy_table::{Row, Table};

use zenoh::prelude::r#async::*;
use zenoh_flow_commons::{parse_vars, Result, RuntimeId, Vars};
use zenoh_flow_daemon::queries::*;
use zenoh_flow_descriptors::{DataFlowDescriptor, FlattenedDataFlowDescriptor};
use zenoh_flow_records::DataFlowRecord;
use zenoh_flow_runtime::{Extensions, Runtime};

use super::row;
use crate::{
    utils::{get_all_runtimes, get_runtime_by_name},
    ZENOH_FLOW_INTERNAL_ERROR,
};

#[derive(Subcommand)]
pub(crate) enum RuntimeCommand {
    /// List all the Zenoh-Flow runtimes reachable on the Zenoh network.
    List,
    /// Returns the status of the provided Zenoh-Flow runtime.
    ///
    /// The status consists of general information regarding the runtime and the
    /// machine it runs on:
    /// - the name associated with the Zenoh-Flow runtime,
    /// - the number of CPUs the machine running the Zenoh-Flow runtime has,
    /// - the total amount of RAM the machine running the Zenoh-Flow runtime has,
    /// - for each data flow the Zenoh-Flow runtime manages (partially or not):
    ///   - its unique identifier,
    ///   - its name,
    ///   - its status.
    #[command(verbatim_doc_comment)]
    #[group(required = true, multiple = false)]
    Status {
        /// The unique identifier of the Zenoh-Flow runtime to contact.
        /// If no identifier is provided, a random Zenoh-Flow runtime is
        /// selected among those reachable.
        #[arg(short = 'i', long = "id", group = "runtime")]
        runtime_id: Option<RuntimeId>,
        /// The name of the Zenoh-Flow runtime to contact.
        ///
        /// Note that if several runtimes share the same name, the first to
        /// answer will be selected.
        #[arg(short = 'n', long = "name", group = "runtime")]
        runtime_name: Option<String>,
    },

    /// Launch a Zenoh-Flow runtime with a DataFlow in standalone mode.
    #[command(verbatim_doc_comment)]
    Run {
        /// The data flow to execute.
        flow: PathBuf,
        /// The, optional, location of the configuration to load nodes implemented not in Rust.
        #[arg(short, long, value_name = "path")]
        extensions: Option<PathBuf>,
        /// Variables to add / overwrite in the `vars` section of your data
        /// flow, with the form `KEY=VALUE`. Can be repeated multiple times.
        ///
        /// Example:
        ///     --vars HOME_DIR=/home/zenoh-flow --vars BUILD=debug
        #[arg(long, value_parser = parse_vars::<String, String>, verbatim_doc_comment)]
        vars: Option<Vec<(String, String)>>,
        /// The path to a Zenoh configuration to manage the connection to the Zenoh
        /// network.
        ///
        /// If no configuration is provided, `zfctl` will default to connecting as
        /// a peer with multicast scouting enabled.
        #[arg(short = 'z', long, verbatim_doc_comment)]
        zenoh_configuration: Option<PathBuf>,
    },
}

impl RuntimeCommand {
    pub async fn run(self, session: Session) -> Result<()> {
        match self {
            RuntimeCommand::List => {
                let runtimes = get_all_runtimes(&session).await;

                let mut table = Table::new();
                table.set_width(80);
                table.set_header(Row::from(vec!["Identifier", "Name"]));
                runtimes.iter().for_each(|info| {
                    table.add_row(Row::from(vec![&info.id.to_string(), info.name.as_ref()]));
                });

                println!("{table}");
            }

            RuntimeCommand::Status {
                runtime_id,
                runtime_name,
            } => {
                let runtime_id = match (runtime_id, runtime_name) {
                    (Some(id), _) => id,
                    (None, Some(name)) => get_runtime_by_name(&session, &name).await,
                    (None, None) => {
                        // This code is indeed unreachable because:
                        // (1) The `group` macro has `required = true` which indicates that clap requires an entry for
                        //     any group.
                        // (2) The `group` macro has `multiple = false` which indicates that only a single entry for
                        //     any group is accepted.
                        // (3) The `runtime_id` and `runtime_name` fields belong to the same group "runtime".
                        //
                        // => A single entry for the group "runtime" is required (and mandatory).
                        unreachable!()
                    }
                };

                let selector = selector_runtimes(&runtime_id);

                let value = serde_json::to_vec(&RuntimesQuery::Status).map_err(|e| {
                    tracing::error!(
                        "serde_json failed to serialize `RuntimeQuery::Status`: {:?}",
                        e
                    );
                    anyhow!(ZENOH_FLOW_INTERNAL_ERROR)
                })?;

                let reply = session
                    .get(selector)
                    .with_value(value)
                    .timeout(Duration::from_secs(5))
                    .res()
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "Failed to query Zenoh-Flow runtime < {} >: {:?}",
                            runtime_id,
                            e
                        )
                    })?;

                while let Ok(reply) = reply.recv_async().await {
                    match reply.sample {
                        Ok(sample) => {
                            match serde_json::from_slice::<RuntimeStatus>(
                                &sample.payload.contiguous(),
                            ) {
                                Ok(runtime_status) => {
                                    let mut table = Table::new();
                                    table.set_width(80);
                                    table.add_row(row!("Identifier", runtime_id));
                                    table.add_row(row!("Name", runtime_status.name));
                                    table.add_row(row!(
                                        "Host name",
                                        runtime_status.hostname.unwrap_or_else(|| "N/A".into())
                                    ));
                                    table.add_row(row!(
                                        "Operating System",
                                        runtime_status
                                            .operating_system
                                            .unwrap_or_else(|| "N/A".into())
                                    ));
                                    table.add_row(row!(
                                        "Arch",
                                        runtime_status.architecture.unwrap_or_else(|| "N/A".into())
                                    ));
                                    table.add_row(row!("# CPUs", runtime_status.cpus));
                                    table.add_row(row!(
                                        "# RAM",
                                        bytesize::to_string(runtime_status.ram_total, true)
                                    ));
                                    println!("{table}");

                                    table = Table::new();
                                    table.set_width(80);
                                    table.set_header(row!("Instance Uuid", "Name", "Status"));
                                    runtime_status.data_flows_status.iter().for_each(
                                        |(uuid, (name, status))| {
                                            table.add_row(row!(uuid, name, status));
                                        },
                                    );
                                    println!("{table}");
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to parse reply as a `RuntimeStatus`: {:?}",
                                        e
                                    )
                                }
                            }
                        }

                        Err(e) => tracing::error!("Reply to runtime status failed with: {:?}", e),
                    }
                }
            }
            RuntimeCommand::Run {
                flow,
                extensions,
                vars,
                zenoh_configuration,
            } => {
                let extensions = match extensions {
                    Some(extensions_path) => {
                        let (extensions, _) =
                            zenoh_flow_commons::try_parse_from_file::<Extensions>(
                                extensions_path.as_os_str(),
                                Vars::default(),
                            )
                            .context(format!(
                                "Failed to load Loader configuration from < {} >",
                                &extensions_path.display()
                            ))
                            .unwrap();

                        extensions
                    }
                    None => Extensions::default(),
                };

                let vars = match vars {
                    Some(v) => Vars::from(v),
                    None => Vars::default(),
                };

                let (data_flow, vars) =
                    zenoh_flow_commons::try_parse_from_file::<DataFlowDescriptor>(
                        flow.as_os_str(),
                        vars,
                    )
                    .context(format!(
                        "Failed to load data flow descriptor from < {} >",
                        &flow.display()
                    ))
                    .unwrap();

                let flattened_flow = FlattenedDataFlowDescriptor::try_flatten(data_flow, vars)
                    .context(format!(
                        "Failed to flattened data flow extracted from < {} >",
                        &flow.display()
                    ))
                    .unwrap();

                let mut runtime_builder = Runtime::builder("zenoh-flow-standalone-runtime")
                    .add_extensions(extensions)
                    .expect("Failed to add extensions")
                    .session(session.into_arc());

                if let Some(path) = zenoh_configuration {
                    let zenoh_config = zenoh_flow_runtime::zenoh::Config::from_file(path.clone())
                        .unwrap_or_else(|e| {
                            panic!(
                                "Failed to parse the Zenoh configuration from < {} >:\n{e:?}",
                                path.display()
                            )
                        });
                    let zenoh_session = zenoh_flow_runtime::zenoh::open(zenoh_config)
                        .res_async()
                        .await
                        .unwrap_or_else(|e| panic!("Failed to open a Zenoh session: {e:?}"))
                        .into_arc();

                    runtime_builder = runtime_builder.session(zenoh_session);
                }

                let runtime = runtime_builder
                    .build()
                    .await
                    .expect("Failed to build the Zenoh-Flow runtime");

                let record = DataFlowRecord::try_new(&flattened_flow, runtime.id())
                    .context("Failed to create a Record from the flattened data flow descriptor")
                    .unwrap();

                let instance_id = record.instance_id().clone();
                let record_name = record.name().clone();
                runtime
                    .try_load_data_flow(record)
                    .await
                    .context("Failed to load Record")
                    .unwrap();

                runtime
                    .try_start_instance(&instance_id)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("Failed to start data flow < {} >: {:?}", &instance_id, e)
                    });

                let mut stdin = async_std::io::stdin();
                let mut input = [0_u8];
                println!(
                    r#"
                The flow ({}) < {} > was successfully started.

                To abort its execution, simply enter 'q'.
                "#,
                    record_name, instance_id
                );

                loop {
                    let _ = stdin.read_exact(&mut input).await;
                    if input[0] == b'q' {
                        break;
                    }
                }

                runtime
                    .try_delete_instance(&instance_id)
                    .await
                    .unwrap_or_else(|e| {
                        panic!("Failed to delete data flow < {} >: {:?}", &instance_id, e)
                    });
            }
        }

        Ok(())
    }
}
