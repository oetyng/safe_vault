// Copyright 2019 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! A simple, persistent, disk-based key-value store.

mod chunk;
pub(super) mod error;
mod blob;
mod login_packet;
mod map;
mod sequence;
#[cfg(test)]
mod tests;
mod used_space;

use crate::{node::Init, utils};
use chunk::{Chunk, ChunkId};
use error::{Error, Result};
use log::trace;
use safe_nd::{Account, Blob, Map, Sequence};
use std::{
    cell::Cell,
    fs::{self, DirEntry, File, Metadata},
    io::{Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    rc::Rc,
};
use used_space::UsedSpace;

const CHUNK_STORE_DIR: &str = "chunks";

/// The max name length for a chunk file.
const MAX_CHUNK_FILE_NAME_LENGTH: usize = 104;

pub(crate) type BlobChunkStore = ChunkStore<Blob>;
pub(crate) type MapChunkStore = ChunkStore<Map>;
pub(crate) type SequenceChunkStore = ChunkStore<Sequence>;
pub(crate) type AccountChunkStore = ChunkStore<Account>;

/// `ChunkStore` is a store of data held as serialised files on disk, implementing a maximum disk
/// usage to restrict storage.
pub(crate) struct ChunkStore<T: Chunk> {
    dir: PathBuf,
    // Maximum space allowed for all `ChunkStore`s to consume.
    max_capacity: u64,
    used_space: UsedSpace,
    _phantom: PhantomData<T>,
}

impl<T> ChunkStore<T>
where
    T: Chunk,
    Self: Subdir,
{
    /// Creates a new `ChunkStore` at location `root/CHUNK_STORE_DIR/<chunk type>`.
    ///
    /// If the location specified already exists, the previous ChunkStore there is opened, otherwise
    /// the required folder structure is created.
    ///
    /// The maximum storage space is defined by `max_capacity`.  This specifies the max usable by
    /// _all_ `ChunkStores`, not per `ChunkStore`.
    pub fn new<P: AsRef<Path>>(
        root: P,
        max_capacity: u64,
        total_used_space: Rc<Cell<u64>>,
        init_mode: Init,
    ) -> Result<Self> {
        let dir = root.as_ref().join(CHUNK_STORE_DIR).join(Self::subdir());

        match init_mode {
            Init::New => Self::create_new_root(&dir)?,
            Init::Load => trace!("Loading ChunkStore at {}", dir.display()),
        }

        let used_space = UsedSpace::new(&dir, total_used_space, init_mode)?;
        Ok(ChunkStore {
            dir,
            max_capacity,
            used_space,
            _phantom: PhantomData,
        })
    }
}

impl<T: Chunk> ChunkStore<T> {
    fn create_new_root(root: &Path) -> Result<()> {
        trace!("Creating ChunkStore at {}", root.display());
        fs::create_dir_all(root)?;

        // Verify that chunk files can be created.
        let temp_file_path = root.join("0".repeat(MAX_CHUNK_FILE_NAME_LENGTH));
        let _ = File::create(&temp_file_path)?;
        fs::remove_file(temp_file_path)?;

        Ok(())
    }

    /// Stores a new data chunk.
    ///
    /// If there is not enough storage space available, returns `Error::NotEnoughSpace`.  In case of
    /// an IO error, it returns `Error::Io`.
    ///
    /// If a chunk with the same id already exists, it will be overwritten.
    pub fn put(&mut self, chunk: &T) -> Result<()> {
        let serialised_chunk = utils::serialise(chunk);
        let consumed_space = serialised_chunk.len() as u64;
        if self.used_space.total().saturating_add(consumed_space) > self.max_capacity {
            return Err(Error::NotEnoughSpace);
        }

        let file_path = self.file_path(chunk.id())?;
        let _ = self.do_delete(&file_path);

        let mut file = File::create(&file_path)?;
        file.write_all(&serialised_chunk)?;
        file.sync_data()?;

        self.used_space.increase(consumed_space)
    }

    /// Deletes the data chunk stored under `id`.
    ///
    /// If the data doesn't exist, it does nothing and returns `Ok`.  In the case of an IO error, it
    /// returns `Error::Io`.
    pub fn delete(&mut self, id: &T::Id) -> Result<()> {
        self.do_delete(&self.file_path(id)?)
    }

    /// Returns a data chunk previously stored under `id`.
    ///
    /// If the data file can't be accessed, it returns `Error::NoSuchChunk`.
    pub fn get(&self, id: &T::Id) -> Result<T> {
        let mut file = File::open(self.file_path(id)?).map_err(|_| Error::NoSuchChunk)?;
        let mut contents = vec![];
        let _ = file.read_to_end(&mut contents)?;
        let chunk = bincode::deserialize::<T>(&contents)?;
        // Check it's the requested chunk variant.
        if chunk.id() == id {
            Ok(chunk)
        } else {
            Err(Error::NoSuchChunk)
        }
    }

    /// Tests if a data chunk has been previously stored under `id`.
    pub fn has(&self, id: &T::Id) -> bool {
        if let Ok(path) = self.file_path(id) {
            fs::metadata(path)
                .as_ref()
                .map(Metadata::is_file)
                .unwrap_or(false)
        } else {
            false
        }
    }

    /// Lists all keys of currently stored data.
    #[cfg_attr(not(test), allow(unused))]
    pub fn keys(&self) -> Vec<T::Id> {
        fs::read_dir(&self.dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| to_chunk_id(&entry.ok()?))
                    .collect()
            })
            .unwrap_or_else(|_| Vec::new())
    }

    fn do_delete(&mut self, file_path: &Path) -> Result<()> {
        if let Ok(metadata) = fs::metadata(file_path) {
            self.used_space.decrease(metadata.len())?;
            fs::remove_file(file_path).map_err(From::from)
        } else {
            Ok(())
        }
    }

    fn file_path(&self, id: &T::Id) -> Result<PathBuf> {
        Ok(self.dir.join(&hex::encode(utils::serialise(id))))
    }
}

pub(crate) trait Subdir {
    fn subdir() -> &'static Path;
}

impl Subdir for BlobChunkStore {
    fn subdir() -> &'static Path {
        Path::new("blob")
    }
}

impl Subdir for MapChunkStore {
    fn subdir() -> &'static Path {
        Path::new("map")
    }
}

impl Subdir for SequenceChunkStore {
    fn subdir() -> &'static Path {
        Path::new("sequence")
    }
}

impl Subdir for AccountChunkStore {
    fn subdir() -> &'static Path {
        Path::new("login_packets")
    }
}

fn to_chunk_id<T: ChunkId>(entry: &DirEntry) -> Option<T> {
    let file_name = entry.file_name();
    let file_name = file_name.into_string().ok()?;
    let bytes = hex::decode(file_name).ok()?;
    bincode::deserialize(&bytes).ok()
}
