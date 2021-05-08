// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use std::ffi::OsString;
use sysinfo::{DiskType, NetworkExt, ProcessExt, ProcessStatus, System, SystemExt};

/// Struct containing a disk information.
#[derive(Debug)]
pub struct Disk {
    pub type_: DiskType,
    pub name: OsString,
    pub file_system: Vec<u8>,
    pub mount_point: String,
    pub total_space: u64,
    pub available_space: u64,
}

#[derive(Debug)]
pub struct DiskUsage {
    /// Total number of written bytes.
    pub total_written_bytes: u64,
    /// Number of written bytes since the last refresh.
    pub written_bytes: u64,
    /// Total number of read bytes.
    pub total_read_bytes: u64,
    /// Number of read bytes since the last refresh.
    pub read_bytes: u64,
}

/// Struct containing a process' information.
#[derive(Debug)]
pub struct Process {
    pub memory: u64,
    pub virtual_memory: u64,
    pub cpu_usage: f32,
    pub disk_usage: DiskUsage,
}

impl Process {
    pub fn map(process: &sysinfo::Process) -> Process {
        let usage = process.disk_usage();
        Process {
            memory: process.memory(),
            virtual_memory: process.virtual_memory(),
            cpu_usage: process.cpu_usage(),
            disk_usage: DiskUsage {
                total_written_bytes: usage.total_written_bytes,
                written_bytes: usage.written_bytes,
                total_read_bytes: usage.total_read_bytes,
                read_bytes: usage.read_bytes,
            },
        }
    }
}
