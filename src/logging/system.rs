// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, thread, time::Duration};
use systemstat::{saturating_sub_bytes, Platform, System};
use thiserror::Error;

#[derive(Serialize, Deserialize, Debug)]
pub struct SystemStats {
    pub cpu_load: Result<Vec<CpuLoad>>,
    pub load_average: Result<LoadAverage>,
    pub memory: Result<Memory>,
    pub battery_life: Result<BatteryLife>,
    pub on_ac_power: Result<bool>,
    pub mounts: Result<Vec<Filesystem>>,
    pub block_device_stats: Result<BTreeMap<String, BlockDeviceStats>>,
    pub network_stats: Result<BTreeMap<String, NetworkStats>>,
    pub cpu_temp: Result<f32>,
    pub socket_stats: Result<SocketStats>,
    pub uptime_secs: Result<u64>,
    pub boot_time: Result<String>,
    pub cpu_load_aggregate: Result<CpuLoadAggregate>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LoadAverage {
    pub one: f32,
    pub five: f32,
    pub fifteen: f32,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct NetworkStats {
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
    pub rx_errors: u64,
    pub tx_errors: u64,
    pub addressess: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SocketStats {
    pub tcp_sockets_in_use: usize,
    pub tcp_sockets_orphaned: usize,
    pub udp_sockets_in_use: usize,
    pub tcp6_sockets_in_use: usize,
    pub udp6_sockets_in_use: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Filesystem {
    pub files: usize,
    pub files_total: usize,
    pub files_avail: usize,
    pub free: u64,
    pub avail: u64,
    pub total: u64,
    pub name_max: usize,
    pub fs_type: String,
    pub fs_mounted_from: String,
    pub fs_mounted_on: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockDeviceStats {
    pub name: String,
    pub read_ios: usize,
    pub read_merges: usize,
    pub read_sectors: usize,
    pub read_ticks: usize,
    pub write_ios: usize,
    pub write_merges: usize,
    pub write_sectors: usize,
    pub write_ticks: usize,
    pub in_flight: usize,
    pub io_ticks: usize,
    pub time_in_queue: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CpuLoad {
    pub user: f32,
    pub nice: f32,
    pub system: f32,
    pub interrupt: f32,
    pub idle: f32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BatteryLife {
    remaining_capacity: f32,
    remaining_hours: u64,
    remaining_minutes: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Memory {
    used: u64,
    total_bytes: u64,
    platform_memory: PlatformMemory,
}

fn map(mem: systemstat::PlatformMemory) -> PlatformMemory {
    #[cfg(target_os = "windows")]
    return PlatformMemory {
        load: mem.load,
        total_phys: mem.total_phys.as_u64(),
        avail_phys: mem.avail_phys.as_u64(),
        total_pagefile: mem.total_pagefile.as_u64(),
        avail_pagefile: mem.avail_pagefile.as_u64(),
        total_virt: mem.total_virt.as_u64(),
        avail_virt: mem.avail_virt.as_u64(),
        avail_ext: mem.avail_ext.as_u64(),
    };

    #[cfg(target_os = "freebsd")]
    return PlatformMemory {
        active: mem.active,
        inactive: mem.inactive,
        wired: mem.wired,
        cache: mem.cache,
        zfs_arc: mem.zfs_arc,
        free: mem.free,
    };

    #[cfg(target_os = "openbsd")]
    return PlatformMemory {
        active: mem.active,
        inactive: mem.inactive,
        wired: mem.wired,
        cache: mem.cache,
        free: mem.free,
        paging: mem.paging,
    };

    #[cfg(target_os = "macos")]
    return PlatformMemory {
        active: mem.active,
        inactive: mem.inactive,
        wired: mem.wired,
        cache: mem.cache,
        free: mem.free,
    };

    #[cfg(any(target_os = "linux", target_os = "android"))]
    return PlatformMemory {
        meminfo: mem.meminfo,
    };
}

#[cfg(target_os = "windows")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PlatformMemory {
    pub load: u32,
    pub total_phys: u64,
    pub avail_phys: u64,
    pub total_pagefile: u64,
    pub avail_pagefile: u64,
    pub total_virt: u64,
    pub avail_virt: u64,
    pub avail_ext: u64,
}

#[cfg(target_os = "freebsd")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PlatformMemory {
    pub active: u64,
    pub inactive: u64,
    pub wired: u64,
    pub cache: u64,
    pub zfs_arc: u64,
    pub free: u64,
}

#[cfg(target_os = "openbsd")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PlatformMemory {
    pub active: u64,
    pub inactive: u64,
    pub wired: u64,
    pub cache: u64,
    pub free: u64,
    pub paging: u64,
}

#[cfg(target_os = "macos")]
#[derive(Serialize, Deserialize, Debug)]
pub struct PlatformMemory {
    pub active: u64,
    pub inactive: u64,
    pub wired: u64,
    pub cache: u64,
    pub free: u64,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[derive(Serialize, Deserialize, Debug)]
pub struct PlatformMemory {
    pub meminfo: BTreeMap<String, u64>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CpuLoadAggregate {
    user: f32,
    nice: f32,
    system: f32,
    interrupt: f32,
    idle: f32,
}

/// Main error type for the crate.
#[derive(Error, Debug, Serialize, Deserialize)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum Error {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(String),
}

/// A specialised `Result` type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn get_stats() -> SystemStats {
    let sys = System::new();

    SystemStats {
        cpu_load: match sys.cpu_load() {
            Ok(delayed) => {
                thread::sleep(Duration::from_secs(1));
                match delayed.done() {
                    Ok(cpu) => Ok(cpu
                        .into_iter()
                        .map(|cpu| CpuLoad {
                            user: cpu.user,
                            nice: cpu.nice,
                            system: cpu.system,
                            interrupt: cpu.interrupt,
                            idle: cpu.idle,
                        })
                        .collect()),
                    Err(e) => map_err(Err(e)),
                }
            }
            Err(e) => map_err(Err(e)),
        },
        load_average: match sys.load_average() {
            Ok(avg) => Ok(LoadAverage {
                one: avg.one,
                five: avg.five,
                fifteen: avg.fifteen,
            }),
            Err(e) => map_err(Err(e)),
        },
        memory: match sys.memory() {
            Ok(mem) => Ok(Memory {
                used: saturating_sub_bytes(mem.total, mem.free).as_u64(),
                total_bytes: mem.total.as_u64(),
                platform_memory: map(mem.platform_memory),
            }),
            Err(e) => map_err(Err(e)),
        },
        battery_life: match sys.battery_life() {
            Ok(battery) => Ok(BatteryLife {
                remaining_capacity: battery.remaining_capacity * 100.0,
                remaining_hours: battery.remaining_time.as_secs() / 3600,
                remaining_minutes: battery.remaining_time.as_secs() % 60,
            }),
            Err(e) => map_err(Err(e)),
        },
        on_ac_power: map_err(sys.on_ac_power()),
        mounts: match sys.mounts() {
            Ok(mounts) => Ok(mounts
                .into_iter()
                .map(|fs| Filesystem {
                    files: fs.files,
                    files_total: fs.files_total,
                    files_avail: fs.files_avail,
                    free: fs.free.as_u64(),
                    avail: fs.avail.as_u64(),
                    total: fs.total.as_u64(),
                    name_max: fs.name_max,
                    fs_type: fs.fs_type,
                    fs_mounted_from: fs.fs_mounted_from,
                    fs_mounted_on: fs.fs_mounted_on,
                })
                .collect()),
            Err(e) => map_err(Err(e)),
        },
        block_device_stats: match sys.block_device_statistics() {
            Ok(map) => Ok(map
                .into_iter()
                .map(|(key, stats)| {
                    (
                        key,
                        BlockDeviceStats {
                            name: stats.name,
                            read_ios: stats.read_ios,
                            read_merges: stats.read_merges,
                            read_sectors: stats.read_sectors,
                            read_ticks: stats.read_ticks,
                            write_ios: stats.write_ios,
                            write_merges: stats.write_merges,
                            write_sectors: stats.write_sectors,
                            write_ticks: stats.write_ticks,
                            in_flight: stats.in_flight,
                            io_ticks: stats.io_ticks,
                            time_in_queue: stats.time_in_queue,
                        },
                    )
                })
                .collect()),
            Err(e) => map_err(Err(e)),
        },
        network_stats: match sys.networks() {
            Ok(netifs) => {
                let mut map = BTreeMap::new();
                for netif in netifs.values() {
                    if let Ok(stats) = sys.network_stats(&netif.name) {
                        let _ = map.insert(
                            netif.name.to_string(),
                            NetworkStats {
                                rx_bytes: stats.rx_bytes.as_u64(),
                                tx_bytes: stats.tx_bytes.as_u64(),
                                rx_packets: stats.rx_packets,
                                tx_packets: stats.tx_packets,
                                rx_errors: stats.rx_errors,
                                tx_errors: stats.tx_errors,
                                addressess: netif
                                    .addrs
                                    .iter()
                                    .map(|a| format!("{:?}", a))
                                    .collect(),
                            },
                        );
                    }
                }
                Ok(map)
            }
            Err(e) => map_err(Err(e)),
        },
        cpu_temp: map_err(sys.cpu_temp()),
        socket_stats: match sys.socket_stats() {
            Ok(stats) => Ok(SocketStats {
                tcp_sockets_in_use: stats.tcp_sockets_in_use,
                tcp_sockets_orphaned: stats.tcp_sockets_orphaned,
                udp_sockets_in_use: stats.udp_sockets_in_use,
                tcp6_sockets_in_use: stats.tcp6_sockets_in_use,
                udp6_sockets_in_use: stats.udp6_sockets_in_use,
            }),
            Err(e) => map_err(Err(e)),
        },
        uptime_secs: match sys.uptime() {
            Ok(dur) => Ok(dur.as_secs()),
            Err(e) => map_err(Err(e)),
        },
        boot_time: match sys.boot_time() {
            Ok(time) => Ok(time.to_rfc3339()),
            Err(e) => map_err(Err(e)),
        },
        cpu_load_aggregate: match sys.cpu_load_aggregate() {
            Ok(delayed) => {
                thread::sleep(Duration::from_secs(1));
                match delayed.done() {
                    Ok(cpu) => Ok(CpuLoadAggregate {
                        user: cpu.user * 100.0,
                        nice: cpu.nice * 100.0,
                        system: cpu.system * 100.0,
                        interrupt: cpu.interrupt * 100.0,
                        idle: cpu.idle * 100.0,
                    }),
                    Err(e) => map_err(Err(e)),
                }
            }
            Err(e) => map_err(Err(e)),
        },
    }
}

fn map_err<T>(e: std::io::Result<T>) -> Result<T> {
    e.map_err(|e| Error::Io(e.to_string()))
}
