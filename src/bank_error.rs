// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under the MIT license <LICENSE-MIT
// https://opensource.org/licenses/MIT> or the Modified BSD license <LICENSE-BSD
// https://opensource.org/licenses/BSD-3-Clause>, at your option. This file may not be copied,
// modified, or distributed except according to those terms. Please review the Licences for the
// specific language governing permissions and limitations relating to use of the SAFE Network
// Software.

use safe_nd::PublicKey;
use serde::{Deserialize, Serialize};
use std::{
    error,
    fmt::{self, Debug, Display, Formatter},
    result,
};

/// A specialised `Result` type for safecoin.
pub type Result<T> = result::Result<T, Error>;

/// Error debug struct
pub struct ErrorDebug<'a, T>(pub &'a Result<T>);

impl<'a, T> Debug for ErrorDebug<'a, T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Err(error) = self.0 {
            write!(f, "{:?}", error)
        } else {
            write!(f, "Success")
        }
    }
}

/// Main error type for the crate.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Error {
    /// Access is denied for a given requester
    AccessDenied,
    /// Requested account not found
    NoSuchAccount(PublicKey),
    /// Attempt to open an account when it already exists
    AccountExists(PublicKey),
    /// Invalid Operation such as a POST on ImmutableData
    InvalidOperation,
    /// Mismatch between key type and signature type.
    SigningKeyTypeMismatch,
    /// Failed signature validation.
    InvalidSignature,
    /// Received a request with a duplicate MessageId
    DuplicateMessageId,
    /// Network error occurring at Vault level which has no bearing on clients, e.g. serialisation
    /// failure or database failure
    NetworkOther(String),
    /// While parsing, precision would be lost.
    LossOfPrecision,
    /// Failed to parse the string as [`Money`](struct.Money.html).
    FailedToParse(String),
    /// Transaction ID already exists.
    TransactionIdExists,
    /// Insufficient balance.
    InsufficientBalance {
        account: PublicKey,
        balance: u128,
        amount: u128,
    },
}

impl<T: Into<String>> From<T> for Error {
    fn from(err: T) -> Self {
        Error::NetworkOther(err.into())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Error::AccessDenied => write!(f, "Access denied"),
            Error::NoSuchAccount(public_key) => {
                write!(f, "Account does not exist: {:?}", public_key)
            }
            Error::AccountExists(public_key) => {
                write!(f, "Account already exists: {:?}", public_key)
            }
            Error::InsufficientBalance { balance, .. } => write!(
                f,
                "Not enough balance to complete this operation: {:?}",
                balance
            ),
            Error::TransactionIdExists { id, .. } => {
                write!(f, "Transaction with a given ID already exists: {:?}", id)
            }
            Error::InvalidOperation => write!(f, "Requested operation is not allowed"),
            Error::SigningKeyTypeMismatch => {
                write!(f, "Mismatch between key type and signature type")
            }
            Error::InvalidSignature => write!(f, "Failed signature validation"),
            Error::NetworkOther(ref error) => write!(f, "Error on Vault network: {}", error),
            Error::LossOfPrecision => {
                write!(f, "Lost precision on the number of coins during parsing")
            }
            Error::FailedToParse(ref error) => {
                write!(f, "Failed to parse from a string: {}", error)
            }
            Error::DuplicateMessageId => write!(f, "MessageId already exists"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::AccessDenied => "Access denied",
            Error::InvalidOperation => "Invalid operation",
            Error::SigningKeyTypeMismatch => "Key type and signature type mismatch",
            Error::InvalidSignature => "Invalid signature",
            Error::NetworkOther(ref error) => error,
            Error::LossOfPrecision => "Lost precision on the number of coins during parsing",
            Error::FailedToParse(_) => "Failed to parse entity",
            Error::TransactionIdExists(_) => "Transaction with a given ID already exists",
            Error::InsufficientBalance(_) => "Not enough coins to complete this operation",
            Error::NoSuchAccount(_) => "Account does not exist",
            Error::AccountExists(_) => "Account already exists",
            Error::DuplicateMessageId => "MessageId already exists",
        }
    }
}
