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

use std::sync::Arc;

use anyhow::bail;
use flume::{Receiver, Sender};
use futures::select;
use zenoh::Session;
use zenoh_flow_commons::Result;
use zenoh_flow_runtime::Runtime;

use crate::queries::{
    instances::InstancesQuery, runtime::RuntimesQuery, selectors, validate_query,
};

/// Spawns an async task to answer queries received on `zenoh-flow/{runtime_id}/instances`.
pub(crate) async fn spawn_instances_queryable(
    zenoh_session: Session,
    runtime: Arc<Runtime>,
    abort_rx: Receiver<()>,
    abort_ack_tx: Sender<()>,
) -> Result<()> {
    let ke_instances = selectors::selector_instances(runtime.id());
    let queryable = match zenoh_session.declare_queryable(ke_instances.clone()).await {
        Ok(queryable) => {
            tracing::trace!("declared queryable: {}", ke_instances);
            queryable
        }
        Err(e) => {
            bail!("Failed to declare Zenoh queryable 'instances': {:?}", e)
        }
    };

    async_std::task::spawn(async move {
        loop {
            select!(
                _ = abort_rx.recv_async() => {
                    tracing::trace!("Received abort signal");
                    break;
                }

                query = queryable.recv_async() => {
                    match query {
                        Ok(query) => {
                            let instance_query: InstancesQuery = match validate_query(&query).await {
                                Ok(instance_query) => instance_query,
                                Err(e) => {
                                    tracing::error!("Unable to parse `InstancesQuery`: {:?}", e);
                                    return;
                                }
                            };

                            let runtime = runtime.clone();
                            async_std::task::spawn(async move {
                                instance_query.process(query, runtime).await;
                            });
                        }
                        Err(e) => {
                            tracing::error!("Queryable 'instances' dropped: {:?}", e);
                            return;
                        }
                    }
                }
            )
        }

        abort_ack_tx.send_async(()).await.unwrap_or_else(|e| {
            tracing::error!("Queryable 'instances' failed to acknowledge abort: {:?}", e);
        });
    });

    Ok(())
}

pub(crate) async fn spawn_runtime_queryable(
    zenoh_session: Session,
    runtime: Arc<Runtime>,
    abort_rx: Receiver<()>,
    abort_ack_tx: Sender<()>,
) -> Result<()> {
    let ke_runtime = selectors::selector_runtimes(runtime.id());

    let queryable = match zenoh_session.declare_queryable(ke_runtime.clone()).await {
        Ok(queryable) => {
            tracing::trace!("declared queryable < {} >", ke_runtime);
            queryable
        }
        Err(e) => {
            bail!("Failed to declare Zenoh queryable 'runtimes': {:?}", e)
        }
    };

    async_std::task::spawn(async move {
        loop {
            select!(
                _ = abort_rx.recv_async() => {
                    tracing::trace!("Received abort signal");
                    break;
                }

                query = queryable.recv_async() => {
                    match query {
                        Ok(query) => {
                            let runtime_query: RuntimesQuery = match validate_query(&query).await {
                                Ok(runtime_query) => runtime_query,
                                Err(e) => {
                                    tracing::error!("Unable to parse `RuntimesQuery`: {:?}", e);
                                    return;
                                }
                            };

                            let runtime = runtime.clone();
                            async_std::task::spawn(async move {
                                runtime_query.process(query, runtime).await;
                            });
                        }
                        Err(e) => {
                            tracing::error!("Queryable 'runtimes' dropped: {:?}", e);
                            return;
                        }
                    }
                }
            )
        }

        abort_ack_tx.send_async(()).await.unwrap_or_else(|e| {
            tracing::error!("Queryable 'runtime' failed to acknowledge abort: {:?}", e);
        });
    });

    Ok(())
}
