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

use zenoh::Session;
use zenoh_flow_commons::{InstanceId, RuntimeId};
use zenoh_flow_runtime::Runtime;

use super::{InstancesQuery, Origin};
use crate::queries::selectors;

pub(crate) fn abort(runtime: Arc<Runtime>, origin: Origin, instance_id: InstanceId) {
    async_std::task::spawn(async move {
        if matches!(origin, Origin::Client) {
            match runtime.try_get_record(&instance_id).await {
                Ok(record) => {
                    query_abort(
                        runtime.session(),
                        record
                            .mapping()
                            .keys()
                            .filter(|&runtime_id| runtime_id != runtime.id()),
                        &instance_id,
                    )
                    .await
                }
                Err(e) => {
                    tracing::error!(
                        "Could not get record of data flow < {} >: {e:?}",
                        instance_id
                    );
                    return;
                }
            }
        }

        if let Err(e) = runtime.try_abort_instance(&instance_id).await {
            tracing::error!("Failed to abort instance < {} >: {:?}", instance_id, e);
        }
    });
}

pub(crate) async fn query_abort(
    session: &Session,
    runtimes: impl Iterator<Item = &RuntimeId>,
    instance_id: &InstanceId,
) {
    let abort_query = match serde_json::to_vec(&InstancesQuery::Abort {
        origin: Origin::Daemon,
        instance_id: instance_id.clone(),
    }) {
        Ok(query) => query,
        Err(e) => {
            tracing::error!(
                "serde_json failed to serialize InstancesQuery::Abort: {:?}",
                e
            );
            return;
        }
    };

    for runtime_id in runtimes {
        let selector = selectors::selector_instances(runtime_id);

        if let Err(e) = session.get(selector).payload(abort_query.clone()).await {
            tracing::error!(
                "Sending abort query to runtime < {} > failed with error: {:?}",
                runtime_id,
                e
            );
        }
        tracing::trace!("Sent abort query to runtime < {} >", runtime_id);
    }
}
