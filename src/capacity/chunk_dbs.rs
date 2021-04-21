// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{utils, Result};
use futures::lock::Mutex;
use pickledb::PickleDb;
use std::path::Path;
use std::sync::Arc;

const BLOB_META_DB_NAME: &str = "immutable_data.db";
const HOLDER_META_DB_NAME: &str = "holder_data.db";
const FULL_ADULTS_DB_NAME: &str = "full_adults.db";

#[derive(Clone)]
pub struct ChunkHolderDbs {
    pub metadata: Arc<Mutex<PickleDb>>,
    pub holders: Arc<Mutex<PickleDb>>,
    pub full_adults: Arc<Mutex<PickleDb>>,
}

impl ChunkHolderDbs {
    ///
    pub fn new(path: &Path) -> Result<Self> {
        let metadata = utils::new_auto_dump_db(path, BLOB_META_DB_NAME)?;
        let holders = utils::new_auto_dump_db(path, HOLDER_META_DB_NAME)?;
        let full_adults = utils::new_auto_dump_db(path, FULL_ADULTS_DB_NAME)?;
        Ok(Self {
            metadata: Arc::new(Mutex::new(metadata)),
            holders: Arc::new(Mutex::new(holders)),
            full_adults: Arc::new(Mutex::new(full_adults)),
        })
    }
}
