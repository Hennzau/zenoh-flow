//
// Copyright (c) 2021 - 2023 ZettaScale Technology
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

mod instance;
pub use instance::{DataFlowInstance, InstanceState, InstanceStatus};

mod loader;
pub use loader::{Extension, Extensions, Loader};

#[cfg(feature = "shared-memory")]
mod shared_memory;

mod runners;

mod runtime;
pub use runtime::{DataFlowErr, Runtime};

#[cfg(feature = "zenoh")]
pub mod zenoh {
    pub use zenoh::config::{client, empty, peer};
    pub use zenoh::open;
    pub use zenoh::prelude::r#async::AsyncResolve;
    pub use zenoh::prelude::{Config, Session};
}
