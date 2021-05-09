// Copyright 2021 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

pub mod log_ctx;
mod system;

use self::{
    log_ctx::LogCtx,
    system::{Disk, Network},
};
use futures::executor::block_on as block;
use log::trace;
use std::{
    thread,
    time::{Duration, Instant},
};
use sysinfo::{DiskExt, NetworkExt, NetworksExt, System, SystemExt};
use system::Process;

const LOG_INTERVAL: Duration = std::time::Duration::from_secs(30);

pub async fn run_system_logger(ctx: LogCtx) {
    let mut system = System::new_all();
    let mut last_log = Instant::now();
    initial_log(&mut system, &ctx).await;
    log(&mut last_log, &mut system, &ctx).await;
    let _ = thread::spawn(move || loop {
        let elapsed = last_log.elapsed();
        if elapsed >= LOG_INTERVAL {
            block(log(&mut last_log, &mut system, &ctx));
        } else {
            thread::sleep(LOG_INTERVAL - elapsed)
        }
    });
}

async fn initial_log(system: &mut System, ctx: &LogCtx) {
    // Display system information:
    trace!(
        "{:?}: System {{ name: {}, kernel_version: {}, os_version: {}, host_name: {}, }}",
        ctx.prefix().await,
        fmt(system.get_name()),
        fmt(system.get_kernel_version()),
        fmt(system.get_os_version()),
        fmt(system.get_host_name())
    );
}

fn fmt(string: Option<String>) -> String {
    string.unwrap_or_else(|| "Unknown".to_string())
}

async fn log(last_log: &mut Instant, system: &mut System, ctx: &LogCtx) {
    *last_log = Instant::now();

    system.refresh_all();

    let prefix = ctx.prefix().await;

    let our_pid = &(std::process::id() as usize);
    let processors = system.get_processors();
    let processor_count = processors.len();

    trace!("{:?}: Processors {{ list: {:?} }}", prefix, processors);

    // Every sn_node process' info
    for (pid, proc_) in system.get_processes() {
        if pid != our_pid {
            continue;
        }
        trace!("{:?}: {:?}", prefix, Process::map(proc_, processor_count));
    }

    // The temperature of the different components
    let list = system.get_components();
    if !list.is_empty() {
        trace!("{:?}: ComponentTemperatures {{ list: {:?} }}", prefix, list);
    }

    // All disks' information
    let list: Vec<_> = system
        .get_disks()
        .iter()
        .map(|disk| Disk {
            type_: disk.get_type(),
            name: disk.get_name().to_os_string(),
            file_system: String::from_utf8(disk.get_file_system().to_vec())
                .unwrap_or_else(|_| "Unknown".to_string()),
            mount_point: disk.get_mount_point().as_os_str().to_os_string(),
            total_space: disk.get_total_space(),
            available_space: disk.get_available_space(),
        })
        .collect();
    if !list.is_empty() {
        trace!("{:?}: Disks {{ list: {:?} }}", prefix, list);
    }

    // RAM and SWAP information
    trace!(
        "{:?}: Memory {{ total_memory_kb: {}, used_memory_kb: {}, total_swap_kb: {}, used_swap_kb: {} }}",
        prefix,
        system.get_total_memory(),
        system.get_used_memory(),
        system.get_total_swap(),
        system.get_used_swap()
    );

    let networks: Vec<_> = system
        .get_networks()
        .iter()
        .map(|(name, data)| Network {
            name: name.clone(),
            received: data.get_received(),
            total_received: data.get_total_received(),
            transmitted: data.get_transmitted(),
            total_transmitted: data.get_total_transmitted(),
            packets_received: data.get_packets_received(),
            total_packets_received: data.get_total_packets_received(),
            packets_transmitted: data.get_packets_transmitted(),
            total_packets_transmitted: data.get_total_packets_transmitted(),
            errors_on_received: data.get_errors_on_received(),
            total_errors_on_received: data.get_total_errors_on_received(),
            errors_on_transmitted: data.get_errors_on_transmitted(),
            total_errors_on_transmitted: data.get_total_errors_on_transmitted(),
        })
        .collect();

    if !networks.is_empty() {
        trace!("{:?}: Networks {{ list: {:?} }}", prefix, networks);
    }

    let uptime = system.get_uptime();
    let boot_time = system.get_boot_time();
    let load_avg = system.get_load_average();

    trace!(
        "{:?}: MachineTime {{ up: {}, booted: {} }}",
        prefix,
        uptime,
        boot_time
    );
    trace!(
        "{:?}: LoadAvg {{ one_minute: {}, five_minute: {}, fifteen_minutes: {}, }}",
        prefix,
        load_avg.one / processor_count as f64,
        load_avg.five / processor_count as f64,
        load_avg.fifteen / processor_count as f64,
    );
}
