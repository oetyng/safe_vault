// Copyright 2019 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

//! Implementation of the "Vault" node for the SAFE Network.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/maidsafe/QA/master/Images/maidsafe_logo.png",
    html_favicon_url = "https://maidsafe.net/img/favicon.ico",
    test(attr(forbid(warnings)))
)]
// For explanation of lint checks, run `rustc -W help`.
#![forbid(unsafe_code)]
#![warn(
    // TODO: add missing debug implementations for structs?
    // missing_debug_implementations,
    missing_docs,
    trivial_casts,
    trivial_numeric_casts,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results
)]
// For quick_error
#![recursion_limit = "128"]

mod action;
mod adult;
mod bank;
mod bank_error;
mod chunk_store;
mod client_handler;
mod config_handler;
mod data_handler;
mod error;
mod money_handler;
mod rpc;
mod to_db_key;
mod utils;
mod vault;

pub(crate) use to_db_key::ToDbKey;

/// Mock version of Routing
#[cfg(feature = "mock")]
pub mod mock_routing;

#[cfg(feature = "mock")]
pub use crate::mock_routing as routing;
#[cfg(not(feature = "mock"))]
pub use routing;

pub use crate::{
    chunk_store::error::Error as ChunkStoreError,
    client_handler::COST_OF_PUT,
    config_handler::{write_connection_info, Config},
    error::{Error, Result},
    vault::{Command, Vault},
};
