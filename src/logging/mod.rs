// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub mod system;

use log::trace;
use std::{
    thread,
    time::{Duration, Instant},
};

const LOG_INTERVAL: Duration = std::time::Duration::from_secs(10);

pub fn run_system_logger() {
    let mut last_log = Instant::now();
    log(&mut last_log);
    let _ = thread::spawn(move || loop {
        let elapsed = last_log.elapsed();
        if elapsed >= LOG_INTERVAL {
            log(&mut last_log);
        } else {
            thread::sleep(LOG_INTERVAL - elapsed)
        }
    });
}

fn log(last_log: &mut Instant) {
    *last_log = Instant::now();
    let stats = system::get_stats();
    if let Ok(battery_life) = stats.battery_life {
        trace!("{:?}", battery_life);
    }
    if let Ok(block_device_stats) = stats.block_device_stats {
        trace!("BlockDeviceStats {{ devices: {:?} }}", block_device_stats);
    }
    if let Ok(boot_time) = stats.boot_time {
        trace!("BootTime {{ utc_time: {} }}", boot_time);
    }
    if let Ok(cpu_load) = stats.cpu_load {
        trace!("CpuLoads {{ list: {:?} }}", cpu_load);
    }
    if let Ok(cpu_load_aggregate) = stats.cpu_load_aggregate {
        trace!("{:?}", cpu_load_aggregate);
    }
    if let Ok(cpu_temp) = stats.cpu_temp {
        trace!("cpu temp: {}", cpu_temp);
    }
    if let Ok(load_average) = stats.load_average {
        trace!("{:?}", load_average);
    }
    if let Ok(memory) = stats.memory {
        trace!("{:?}", memory);
    }
    if let Ok(mounts) = stats.mounts {
        trace!("Mounts {{ list: {:?} }}", mounts);
    }
    if let Ok(network_stats) = stats.network_stats {
        trace!("NetworkStats {{ nics: {:?} }}", network_stats);
    }
    if let Ok(on_ac_power) = stats.on_ac_power {
        trace!("AcPower {{ on: {} }}", on_ac_power);
    }
    if let Ok(socket_stats) = stats.socket_stats {
        trace!("{:?}", socket_stats);
    }
    if let Ok(uptime_secs) = stats.uptime_secs {
        trace!("Uptime {{ secs: {} }}", uptime_secs);
    }
}
