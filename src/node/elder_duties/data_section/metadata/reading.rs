// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Read operations on data.

use super::{
    blob_register::BlobRegister, elder_stores::ElderStores, map_storage::MapStorage,
    sequence_storage::SequenceStorage,
};
use crate::node::node_ops::{IntoNodeOp, NodeMessagingDuty, NodeOperation};
use crate::Result;
use sn_messaging::{
    client::{BlobRead, DataQuery, MapRead, SequenceRead},
    location::User,
    MessageId,
};

pub(super) async fn get_result(
    query: DataQuery,
    msg_id: MessageId,
    origin: User,
    stores: &ElderStores,
) -> Result<NodeOperation> {
    use DataQuery::*;
    match &query {
        Blob(read) => blob(read, stores.blob_register(), msg_id, origin).await,
        Map(read) => map(read, stores.map_storage(), msg_id, origin).await,
        Sequence(read) => sequence(read, stores.sequence_storage(), msg_id, origin).await,
    }
    .convert()
}

async fn blob(
    read: &BlobRead,
    register: &BlobRegister,
    msg_id: MessageId,
    origin: User,
) -> Result<NodeMessagingDuty> {
    register.read(read, msg_id, origin).await // since the data is sent on to adults, the entire msg is passed in
}

async fn map(
    read: &MapRead,
    storage: &MapStorage,
    msg_id: MessageId,
    origin: User,
) -> Result<NodeMessagingDuty> {
    storage.read(read, msg_id, origin).await // map data currently stay at elders, so the msg is not needed
}

async fn sequence(
    read: &SequenceRead,
    storage: &SequenceStorage,
    msg_id: MessageId,
    origin: User,
) -> Result<NodeMessagingDuty> {
    storage.read(read, msg_id, origin).await // sequence data currently stay at elders, so the msg is not needed
}
