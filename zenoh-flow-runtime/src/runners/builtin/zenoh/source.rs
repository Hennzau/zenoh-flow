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

use std::{collections::HashMap, pin::Pin, sync::Arc};

use anyhow::{anyhow, Context as ac};
use async_std::sync::Mutex;
use futures::{future::select_all, Future};
use zenoh::{
    handlers::FifoChannelHandler, key_expr::OwnedKeyExpr, pubsub::Subscriber, sample::Sample,
    Session,
};
use zenoh_flow_commons::{NodeId, PortId, Result};
use zenoh_flow_nodes::prelude::{Node, OutputRaw, Outputs};

/// Internal type of pending futures for the ZenohSource
pub(crate) type ZSubFut = Pin<Box<dyn Future<Output = (PortId, Result<Sample>)> + Send + Sync>>;

fn wait_zenoh_sub(id: PortId, sub: &Subscriber<FifoChannelHandler<Sample>>) -> ZSubFut {
    let sub = sub.handler().clone();
    Box::pin(async move { (id, sub.recv_async().await.map_err(|e| anyhow!("{e:?}"))) })
}

pub(crate) struct ZenohSource {
    id: NodeId,
    session: Session,
    outputs: HashMap<PortId, OutputRaw>,
    key_exprs: HashMap<PortId, OwnedKeyExpr>,
    subscribers: Mutex<HashMap<PortId, Subscriber<FifoChannelHandler<Sample>>>>,
    futs: Arc<Mutex<Vec<ZSubFut>>>,
}

impl ZenohSource {
    pub(crate) async fn try_new(
        id: &NodeId,
        session: Session,
        key_exprs: &HashMap<PortId, OwnedKeyExpr>,
        mut outputs: Outputs,
    ) -> Result<ZenohSource> {
        let mut raw_outputs = HashMap::with_capacity(key_exprs.len());

        for (port, key_expr) in key_exprs.iter() {
            raw_outputs.insert(
                port.clone(),
                outputs
                    .take(port.as_ref())
                    .with_context(|| {
                        format!(
                            "{id}: fatal internal error: no channel was created for key \
                             expression < {} >",
                            key_expr
                        )
                    })?
                    .raw(),
            );
        }

        let zenoh_source = Self {
            id: id.clone(),
            session,
            outputs: raw_outputs,
            key_exprs: key_exprs.clone(),
            subscribers: Mutex::new(HashMap::with_capacity(key_exprs.len())),
            futs: Arc::new(Mutex::new(Vec::with_capacity(key_exprs.len()))),
        };

        Ok(zenoh_source)
    }
}

#[async_trait::async_trait]
impl Node for ZenohSource {
    // When we resume an aborted Zenoh Source, we have to re-subscribe to the key expressions and, possibly, recreate
    // the futures awaiting publications.
    async fn on_resume(&self) -> Result<()> {
        let mut futures = self.futs.lock().await;
        let futures_were_empty = futures.is_empty();

        let mut subscribers = self.subscribers.lock().await;
        for (port, key_expr) in self.key_exprs.iter() {
            let subscriber = self
                .session
                .declare_subscriber(key_expr)
                .await
                .map_err(|e| {
                    anyhow!(
                        r#"fatal internal error: failed to declare a subscriber on < {} >
Caused by:
{:?}"#,
                        key_expr,
                        e
                    )
                })?;

            // NOTE: Even though it is more likely that the node was aborted while the `futures` were swapped (and thus
            // empty), there is still a possibility that the `abort` happened outside of this scenario.
            // In this rare case, we should not push new futures.
            if futures_were_empty {
                futures.push(wait_zenoh_sub(port.clone(), &subscriber));
            }

            subscribers.insert(port.clone(), subscriber);
        }

        Ok(())
    }

    // When we abort a Zenoh Source we drop the subscribers to remove them from the Zenoh network.
    //
    // This action is motivated by two factors:
    // 1. It prevents receiving publications that happened while the Zenoh Source was not active.
    // 2. It prevents impacting the other subscribers / publishers on the same resource.
    async fn on_abort(&self) {
        let mut subscribers = self.subscribers.lock().await;
        subscribers.clear();
    }

    // The iteration of a Zenoh Source polls, concurrently, the subscribers and forwards the first publication received
    // on the associated port.
    async fn iteration(&self) -> Result<()> {
        let mut subscribers_futures = self.futs.lock().await;
        let subs = std::mem::take(&mut (*subscribers_futures));

        let ((id, result), _index, mut remaining) = select_all(subs).await;

        match result {
            Ok(sample) => {
                let payload = sample.payload().to_bytes();
                let ke = sample.key_expr();
                tracing::trace!("received subscription on {ke}");
                let output = self.outputs.get(&id).ok_or(anyhow!(
                    "{}: internal error, unable to find output < {} >",
                    self.id,
                    id
                ))?;
                output.send(&*payload, None).await?;
            }
            Err(e) => tracing::error!("subscriber for output {id} failed with: {e:?}"),
        }

        // Add back the subscriber that got polled
        let subscribers = self.subscribers.lock().await;
        let sub = subscribers
            .get(&id)
            .ok_or_else(|| anyhow!("[{}] Cannot find port < {} >", self.id, id))?;

        remaining.push(wait_zenoh_sub(id.clone(), sub));

        // Setting back a complete list for the next iteration
        *subscribers_futures = remaining;

        Ok(())
    }
}
