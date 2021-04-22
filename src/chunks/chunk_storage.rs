// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    chunk_store::{BlobChunkStore, UsedSpace},
    error::convert_to_error_message,
    node_ops::{NodeDuty, OutgoingMsg},
    Error, Result,
};
use log::{error, info};
use sn_data_types::{Blob, BlobAddress};
use sn_messaging::{
    client::{
        CmdError, Error as ErrorMessage, Message, NodeDataQueryResponse, NodeQueryResponse,
        ProcessMsg, QueryResponse,
    },
    Aggregation, DstLocation, EndUser, MessageId,
};
use std::{
    fmt::{self, Display, Formatter},
    path::Path,
};
use xor_name::XorName;

/// Storage of data chunks.
pub(crate) struct ChunkStorage {
    chunks: BlobChunkStore,
}

impl ChunkStorage {
    pub(crate) async fn new(path: &Path, used_space: UsedSpace) -> Result<Self> {
        let chunks = BlobChunkStore::new(path, used_space).await?;
        Ok(Self { chunks })
    }

    pub(crate) async fn store(
        &mut self,
        data: &Blob,
        msg_id: MessageId,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        if let Err(error) = self.try_store(data, origin).await {
            Ok(NodeDuty::Send(OutgoingMsg {
                msg: Message::NodeCmdResult {
                    result: Err(CmdError::Data(convert_to_error_message(error)?)),
                    id: msg_id,
                },
                section_source: false, // sent as single node
                // Data's metadata section
                dst: DstLocation::Section(*data.address().name()),
                aggregation: Aggregation::None, // TODO: to_be_aggregated: Aggregation::AtDestination,
            }))
        } else {
            Ok(NodeDuty::Send(OutgoingMsg {
                msg: Message::NodeCmdResult {
                    result: Ok(()),
                    id: msg_id,
                    target_section_pk: None,
                },
                section_source: false,
                // Data's metadata section
                dst: DstLocation::Section(*data.address().name()),
                aggregation: Aggregation::None,
            }))
        }
    }

    async fn try_store(&mut self, data: &Blob, origin: EndUser) -> Result<()> {
        info!("TRYING TO STORE BLOB");
        if data.is_private() {
            let data_owner = data
                .owner()
                .ok_or_else(|| Error::InvalidOwners(*origin.id()))?;
            info!("Blob is unpub");
            info!("DATA OWNER: {:?}", data_owner);
            info!("ORIGIN: {:?}", origin);
            if data_owner != origin.id() {
                info!("INVALID OWNER! Returning error");
                return Err(Error::InvalidOwners(*origin.id()));
            }
        }

        if self.chunks.has(data.address()) {
            info!(
                "{}: Immutable chunk already exists, not storing: {:?}",
                self,
                data.address()
            );
            return Err(Error::DataExists);
        }
        self.chunks.put(&data).await
    }

    pub(crate) async fn get(
        &self,
        address: &BlobAddress,
        msg_id: MessageId,
        _origin: EndUser,
    ) -> Result<NodeDuty> {
        let result = self
            .chunks
            .get(address)
            .map_err(|_| ErrorMessage::NoSuchData);
        Ok(NodeDuty::Send(OutgoingMsg {
            msg: ProcessMsg::QueryResponse {
                id: MessageId::in_response_to(&msg_id),
                response: QueryResponse::GetBlob(result),
                correlation_id: msg_id,
            },
            section_source: false, // sent as single node
            // Respond to the chunk's metadata elders
            dst: DstLocation::Section(*address.name()),
            aggregation: Aggregation::None, // TODO: to_be_aggregated: Aggregation::AtDestination,
        }))
    }

    /// Returns a chunk to the Elders of a section.
    pub async fn get_for_replication(
        &self,
        address: BlobAddress,
        msg_id: MessageId,
        section: XorName,
    ) -> Result<NodeDuty> {
        let result = match self.chunks.get(&address) {
            Ok(res) => Ok(res),
            Err(error) => Err(convert_to_error_message(error)?),
        };

        if let Ok(data) = result {
            Ok(NodeDuty::Send(OutgoingMsg {
                msg: ProcessMsg::NodeQueryResponse {
                    response: NodeQueryResponse::Data(NodeDataQueryResponse::GetChunk(Ok(data))),
                    id: MessageId::in_response_to(&msg_id),
                    correlation_id: msg_id,
                },
                section_source: false,              // sent as single node
                dst: DstLocation::Section(section), // send it back to section Elders
                aggregation: Aggregation::None,
            }))
        } else {
            log::warn!("Could not read chunk for replication: {:?}", result);
            Ok(NodeDuty::NoOp)
        }
    }

    /// Stores a chunk that Elders sent to it for replication.
    pub async fn store_for_replication(&mut self, blob: Blob) -> Result<()> {
        if self.chunks.has(blob.address()) {
            info!(
                "{}: Immutable chunk already exists, not storing: {:?}",
                self,
                blob.address()
            );
            return Ok(());
        }

        self.chunks.put(&blob).await?;

        Ok(())
    }

    pub async fn used_space_ratio(&self) -> f64 {
        self.chunks.used_space_ratio().await
    }

    pub(crate) async fn delete(
        &mut self,
        address: BlobAddress,
        msg_id: MessageId,
        origin: EndUser,
    ) -> Result<NodeDuty> {
        if !self.chunks.has(&address) {
            info!("{}: Immutable chunk doesn't exist: {:?}", self, address);
            return Ok(NodeDuty::NoOp);
        }

        let result = match self.chunks.get(&address) {
            Ok(Blob::Private(data)) => {
                if data.owner() == origin.id() {
                    self.chunks
                        .delete(&address)
                        .await
                        .map_err(|_error| ErrorMessage::FailedToDelete)
                } else {
                    Err(ErrorMessage::InvalidOwners(*origin.id()))
                }
            }
            Ok(_) => {
                error!(
                    "{}: Invalid DeletePrivate(Blob::Public) encountered: {:?}",
                    self, msg_id
                );
                Err(ErrorMessage::InvalidOperation)
            }
            _ => Err(ErrorMessage::NoSuchKey),
        };

        Ok(NodeDuty::Send(OutgoingMsg {
            msg: Message::NodeCmdResult {
                result: result.map_err(CmdError::Data),
                id: msg_id,
                target_section_pk: None,
            },
            section_source: false, // sent as single node
            // respond to data's metadata elders
            dst: DstLocation::Section(*address.name()),
            aggregation: Aggregation::None, // TODO: to_be_aggregated: Aggregation::AtDestination,
        }))
    }
}

impl Display for ChunkStorage {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "ChunkStorage")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk_store::UsedSpace;
    use crate::error::Error::InvalidOwners;
    use crate::error::Result;
    use bls::SecretKey;
    use sn_data_types::{PrivateBlob, PublicBlob, PublicKey};
    use std::path::PathBuf;
    use tempdir::TempDir;

    fn temp_dir() -> Result<TempDir> {
        TempDir::new("test").map_err(|e| Error::TempDirCreationFailed(e.to_string()))
    }

    fn get_random_pk() -> PublicKey {
        PublicKey::from(SecretKey::random().public_key())
    }

    #[tokio::test]
    pub async fn try_store_stores_public_blob() -> Result<()> {
        let path = PathBuf::from(temp_dir()?.path());
        let mut storage = ChunkStorage::new(&path, UsedSpace::new(u64::MAX)).await?;
        let value = "immutable data value".to_owned().into_bytes();
        let blob = Blob::Public(PublicBlob::new(value));
        assert!(storage
            .try_store(&blob, EndUser::AllClients(get_random_pk()))
            .await
            .is_ok());
        assert!(storage.chunks.has(blob.address()));

        Ok(())
    }

    #[tokio::test]
    pub async fn try_store_stores_private_blob() -> Result<()> {
        let path = PathBuf::from(temp_dir()?.path());
        let mut storage = ChunkStorage::new(&path, UsedSpace::new(u64::MAX)).await?;
        let value = "immutable data value".to_owned().into_bytes();
        let key = get_random_pk();
        let blob = Blob::Private(PrivateBlob::new(value, key));
        assert!(storage
            .try_store(&blob, EndUser::AllClients(key))
            .await
            .is_ok());
        assert!(storage.chunks.has(blob.address()));

        Ok(())
    }

    #[tokio::test]
    pub async fn try_store_errors_if_end_user_doesnt_own_data() -> Result<()> {
        let path = PathBuf::from(temp_dir()?.path());
        let mut storage = ChunkStorage::new(&path, UsedSpace::new(u64::MAX)).await?;
        let value = "immutable data value".to_owned().into_bytes();
        let data_owner = get_random_pk();
        let end_user = get_random_pk();
        let blob = Blob::Private(PrivateBlob::new(value, data_owner));
        let result = storage
            .try_store(&blob, EndUser::AllClients(end_user))
            .await;
        assert!(matches!(result, Err(InvalidOwners(_))));
        Ok(())
    }
}
