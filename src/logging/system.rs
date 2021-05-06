// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, thread, time::Duration};
use thiserror::Error;
use sysinfo::{ProcessExt, NetworkExt, SystemExt, System};

/// Struct containing a disk information.
pub struct Disk {
    pub type_: DiskType,
    pub name: OsString,
    pub file_system: Vec<u8>,
    pub mount_point: String,
    pub total_space: u64,
    pub available_space: u64,
}

/// Struct containing a process' information.
pub struct Process {
    pub name: String,
    pub cmd: Vec<String>,
    pub exe: PathBuf,
    pub pid: Pid,
    //environ: Vec<String>,
    //cwd: PathBuf,
    //root: PathBuf,
    pub memory: u64,
    pub virtual_memory: u64,
    pub parent: Option<Pid>,
    pub status: ProcessStatus,
    cpu_calc_values: CPUsageCalculationValues,
    pub start_time: u64,
    pub cpu_usage: f32,
    pub updated: bool,
    pub disk_usage: sysinfo::DiskUsage,
}


impl Process {
    pub fn map(process: sysinfo::Process) {
        Process {
            name: process.name(),
            cmd: process.cmd(),
            exe: process.exe(),
            pid: process.pid(),
            //environ: Vec<String>,
            //cwd: PathBuf,
            //root: PathBuf,
            memory: process.memory(),
            virtual_memory: process.virtual_memory(),
            parent: Option<Pid>,
            status: ProcessStatus,
            start_time: u64,
            cpu_usage: f32,
            updated: bool,
            disk_usage: sysinfo::DiskUsage,
        }
    }
}