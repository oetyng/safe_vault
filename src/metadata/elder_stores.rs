// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::{
    chunk_records::ChunkRecords, map_storage::MapStorage, register_storage::RegisterStorage,
    sequence_storage::SequenceStorage,
};
use crate::{Error, Result};
use sn_messaging::client::DataExchange;

/// The various data type stores,
/// that are only managed at Elders.
pub(super) struct ElderStores {
    chunk_records: ChunkRecords,
    map_storage: MapStorage,
    sequence_storage: SequenceStorage,
    register_storage: RegisterStorage,
}

impl ElderStores {
    pub fn new(
        chunk_records: ChunkRecords,
        map_storage: MapStorage,
        sequence_storage: SequenceStorage,
        register_storage: RegisterStorage,
    ) -> Self {
        Self {
            chunk_records,
            map_storage,
            sequence_storage,
            register_storage,
        }
    }

    pub fn map_storage(&self) -> &MapStorage {
        &self.map_storage
    }

    pub fn sequence_storage(&self) -> &SequenceStorage {
        &self.sequence_storage
    }

    pub fn register_storage(&self) -> &RegisterStorage {
        &self.register_storage
    }

    pub fn chunk_records_mut(&mut self) -> &mut ChunkRecords {
        &mut self.chunk_records
    }

    pub fn map_storage_mut(&mut self) -> &mut MapStorage {
        &mut self.map_storage
    }

    pub fn sequence_storage_mut(&mut self) -> &mut SequenceStorage {
        &mut self.sequence_storage
    }

    pub fn register_storage_mut(&mut self) -> &mut RegisterStorage {
        &mut self.register_storage
    }

    // NB: Not yet including Register metadata.
    pub async fn get_all_data(&self) -> Result<DataExchange> {
        // Prepare chunk_records, map and sequence data
        let chunk_data = self.chunk_records.get_all_data().await?;
        let map_data = self.map_storage.get_all_data()?;
        let seq_data = self.sequence_storage.get_all_data()?;
        Ok(DataExchange {
            chunk_data,
            map_data,
            seq_data,
        })
    }

    pub async fn update(&mut self, data: DataExchange) -> Result<(), Error> {
        self.map_storage.update(data.map_data).await?;
        self.sequence_storage.update(data.seq_data).await?;
        self.chunk_records.update(data.chunk_data).await?;
        Ok(())
    }
}
