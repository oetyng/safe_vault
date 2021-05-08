// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

mod system;

use log::trace;
use std::{
    thread,
    time::{Duration, Instant},
};
use sysinfo::{
    ComponentExt, DiskExt, NetworkExt, NetworksExt, ProcessExt, ProcessorExt, System, SystemExt,
    UserExt,
};
use system::Process;

const LOG_INTERVAL: Duration = std::time::Duration::from_secs(30);

pub fn run_system_logger() {
    let mut system = System::new_all();
    let mut last_log = Instant::now();
    initial_log(&mut system);
    log(&mut last_log, &mut system);
    let _ = thread::spawn(move || loop {
        let elapsed = last_log.elapsed();
        if elapsed >= LOG_INTERVAL {
            log(&mut last_log, &mut system);
        } else {
            thread::sleep(LOG_INTERVAL - elapsed)
        }
    });
}

fn initial_log(system: &mut System) {
    // Display system information:
    trace!(
        "System {{ name: {}, kernel_version: {}, os_version: {}, host_name: {}, }}",
        fmt(system.get_name()),
        fmt(system.get_kernel_version()),
        fmt(system.get_os_version()),
        fmt(system.get_host_name())
    );
}

fn fmt(string: Option<String>) -> String {
    string.unwrap_or("Unknown".to_string())
}

fn log(last_log: &mut Instant, system: &mut System) {
    *last_log = Instant::now();

    system.refresh_all();
    let our_pid = &(std::process::id() as usize);

    // Every sn_node process' info
    for (pid, proc_) in system.get_processes() {
        if pid != our_pid {
            continue;
        }
        trace!("{:?}", Process::map(proc_));
    }

    // The temperature of the different components
    let list = system.get_components();
    if !list.is_empty() {
        trace!("ComponentTemperatures {{ list: {:?} }}", list);
    }

    // All disks' information
    let list = system.get_disks();
    if !list.is_empty() {
        trace!("Disks {{ list: {:?} }}", list);
    }

    // RAM and SWAP information
    trace!(
        "Memory {{ total_memory_kb: {}, used_memory_kb: {}, total_swap_kb: {}, used_swap_kb: {} }}",
        system.get_total_memory(),
        system.get_used_memory(),
        system.get_total_swap(),
        system.get_used_swap()
    );

    for (network_name, data) in system.get_networks() {
        trace!("Network {{ name: {}, received: {}, total_received: {}, transmitted: {}, total_transmitted: {}, packets_received: {}, total_packets_received: {}, packets_transmitted: {}, total_packets_transmitted: {}, errors_on_received: {}, total_errors_on_received: {}, errors_on_transmitted: {}, total_errors_on_transmitted: {}, }}", network_name, data.get_received(), data.get_total_received(), data.get_transmitted(), data.get_total_transmitted(), data.get_packets_received(), data.get_total_packets_received(), data.get_packets_transmitted(), data.get_total_packets_transmitted(), data.get_errors_on_received(), data.get_total_errors_on_received(), data.get_errors_on_transmitted(), data.get_total_errors_on_transmitted());
    }

    // let stats = system::get_stats();
    // if let Ok(battery_life) = stats.battery_life {
    //     trace!("{:?}", battery_life);
    // }
    // if let Ok(block_device_stats) = stats.block_device_stats {
    //     trace!("BlockDeviceStats {{ devices: {:?} }}", block_device_stats);
    // }
    // if let Ok(boot_time) = stats.boot_time {
    //     trace!("BootTime {{ utc_time: {} }}", boot_time);
    // }
    // if let Ok(cpu_load) = stats.cpu_load {
    //     trace!("CpuLoads {{ list: {:?} }}", cpu_load);
    // }
    // if let Ok(cpu_load_aggregate) = stats.cpu_load_aggregate {
    //     trace!("{:?}", cpu_load_aggregate);
    // }
    // if let Ok(cpu_temp) = stats.cpu_temp {
    //     trace!("CpuTemp {{ celsius: {} }}", cpu_temp);
    // }
    // if let Ok(load_average) = stats.load_average {
    //     trace!("{:?}", load_average);
    // }
    // if let Ok(memory) = stats.memory {
    //     trace!("{:?}", memory);
    // }
    // if let Ok(mounts) = stats.mounts {
    //     trace!("Mounts {{ list: {:?} }}", mounts);
    // }
    // if let Ok(network_stats) = stats.network_stats {
    //     trace!("NetworkStats {{ nics: {:?} }}", network_stats);
    // }
    // if let Ok(on_ac_power) = stats.on_ac_power {
    //     trace!("AcPower {{ on: {} }}", on_ac_power);
    // }
    // if let Ok(socket_stats) = stats.socket_stats {
    //     trace!("{:?}", socket_stats);
    // }
    // if let Ok(uptime_secs) = stats.uptime_secs {
    //     trace!("Uptime {{ secs: {} }}", uptime_secs);
    // }
}
