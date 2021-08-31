//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::runtime::message::ZFDataMessage;
use crate::{Token, ZFComponentOutput, ZFPortID, ZFResult};
use async_std::sync::Arc;
use async_trait::async_trait;
use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;

#[typetag::serde(tag = "zf_data_type", content = "value")]
pub trait ZFDataTrait: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

// TODO Create a Derive macro for this
#[typetag::serde(tag = "zf_state_type", content = "value")]
pub trait ZFStateTrait: Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

pub trait ZFComponentState {
    fn initial_state(&self) -> Box<dyn ZFStateTrait>;
}

pub trait ZFComponentInputRule {
    fn input_rule(
        &self,
        state: &mut Box<dyn ZFStateTrait>,
        tokens: &mut HashMap<String, Token>,
    ) -> ZFResult<bool>;
}

pub trait ZFComponentOutputRule {
    fn output_rule(
        &self,
        state: &mut Box<dyn ZFStateTrait>,
        outputs: &HashMap<String, Arc<dyn ZFDataTrait>>,
    ) -> ZFResult<HashMap<ZFPortID, ZFComponentOutput>>;
}

pub trait ZFOperatorTrait:
    ZFComponentState + ZFComponentInputRule + ZFComponentOutputRule + Send + Sync
{
    fn run(
        &self,
        state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<String, ZFDataMessage>,
    ) -> ZFResult<HashMap<ZFPortID, Arc<dyn ZFDataTrait>>>;
}

#[async_trait]
pub trait ZFSourceTrait: ZFComponentState + ZFComponentOutputRule + Send + Sync {
    async fn run(
        &self,
        state: &mut Box<dyn ZFStateTrait>,
    ) -> ZFResult<HashMap<ZFPortID, Arc<dyn ZFDataTrait>>>;
}

#[async_trait]
pub trait ZFSinkTrait: ZFComponentState + ZFComponentInputRule + Send + Sync {
    async fn run(
        &self,
        state: &mut Box<dyn ZFStateTrait>,
        inputs: &mut HashMap<String, ZFDataMessage>,
    ) -> ZFResult<()>;
}
