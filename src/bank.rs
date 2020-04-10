// Copyright 202 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::{
    bank_error::{Error, Result},
    utils,
    vault::Init,
};
use pickledb::PickleDb;
use safe_nd::{NodePublicId, PublicKey, XorName};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fmt::{self, Display, Formatter},
    path::Path,
};

const ACCOUNTS_DB_NAME: &str = "accounts.db";

pub(crate) struct Bank {
    id: NodePublicId,
    // The account balances held in this section.
    accounts: PickleDb,
}

pub type AccountId = PublicKey;

impl Bank {
    pub fn new<P: AsRef<Path>>(id: NodePublicId, root_dir: P, init_mode: Init) -> Result<Self> {
        let accounts = utils::new_db(root_dir, ACCOUNTS_DB_NAME, init_mode)?;
        Ok(Self { id, accounts })
    }

    // Cmd - can fail.
    pub fn open_account(
        &mut self,
        amount: Money,
        from: AccountId,
        to: AccountId,
    ) -> Result<AccountEvent> {
        if from == to {
            Err(Error::InvalidOperation)
        } else if self.exists(to) {
            Err(Error::AccountExists(to))
        } else {
            // did not exist, so we return the change in form of a new fact
            Ok(AccountEvent::Opened { amount, from, to })
        }
    }

    // Cmd - can fail.
    pub fn withdraw(&self, amount: Money, from: AccountId, to: AccountId) -> Result<AccountEvent> {
        if from == to {
            Err(Error::InvalidOperation)
        } else if !self.exists(from) {
            Err(Error::NoSuchAccount(from))
        } else if !self.can_withdraw(from, amount) {
            println!(
                "Not enough money in {:?} account to transfer {:?} to {:?}. (balance: {:?})",
                from,
                amount,
                to,
                self.balance(from)?,
            );
            Err(Error::InsufficientBalance {
                account: from,
                balance: self.balance(from)?.amount,
                amount: amount.amount,
            })
        } else {
            Ok(AccountEvent::Withdrawn { amount, from, to })
        }
    }

    // Cmd - can fail.
    pub fn deposit(&self, amount: Money, from: AccountId, to: AccountId) -> Result<AccountEvent> {
        if from == to {
            Err(Error::InvalidOperation)
        } else if !self.exists(to) {
            Err(Error::NoSuchAccount(to))
        } else {
            Ok(AccountEvent::Deposited { amount, from, to })
        }
    }

    // Query - has some or none.
    pub fn initial_balance(&self, account: AccountId) -> Option<Money> {
        let amount = self
            .accounts
            .lget::<AccountEvent>(account.to_key(), 0)?
            .amount();
        Some(amount)
    }

    // Query - has some or none.
    pub fn balance(&self, account: AccountId) -> Option<Money> {
        // TODO: in the paper, when we read from an account, we union the account
        //       history with the deps, I don't see a use for this since anything
        //       in deps is already in the account history. Think this through a
        //       bit more carefully.
        if !self.exists(account) {
            return None;
        }
        let h = self.history(account);
        let outgoing: u128 = h
            .iter()
            .filter(|t| t.from() == account)
            .map(|t| t.amount().amount)
            .sum();
        let incoming: u128 = h
            .iter()
            .filter(|t| t.to() == account)
            .map(|t| t.amount().amount)
            .sum();
        let balance = Money::from(incoming - outgoing);

        Some(balance)
    }

    // Query
    fn history(&self, account: AccountId) -> Vec<AccountEvent> {
        self.accounts
            .liter(account.to_key())
            .filter_map(|c| c.get_item())
            .collect()
    }

    /// Check balance is sufficient.
    fn can_withdraw(&self, account: AccountId, amount: Money) -> bool {
        match self.balance(account) {
            Some(balance) => {
                if amount > balance {
                    println!(
                        "[INVALID] balance of account is not sufficient for withdrawal: {:?} > {:?}",
                        amount, balance
                    );
                    false
                } else {
                    true
                }
            }
            None => false,
        }
    }

    pub fn exists(&self, account: AccountId) -> bool {
        self.accounts.exists(account.to_key())
    }

    /// Called once a cmd has been validated and produced some event(s).
    /// Cannot fail (then there is a bug), thus no return value. An event is a fact.
    pub fn apply(&mut self, event: AccountEvent) {
        match event {
            AccountEvent::Opened { to, .. } => {
                self.accounts.lcreate(to.to_key()).unwrap().ladd(&event);
            }
            AccountEvent::Withdrawn { from, .. } => {
                self.accounts.ladd(from.to_key(), &event);
            }
            AccountEvent::Deposited { to, .. } => {
                self.accounts.ladd(to.to_key(), &event);
            }
        }
    }
}

impl Display for Bank {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self.id)
    }
}

#[derive(Serialize, Deserialize)]
pub enum AccountEvent {
    Opened {
        amount: Money,
        from: AccountId,
        to: AccountId,
    }, // amount, from, to
    Withdrawn {
        amount: Money,
        from: AccountId,
        to: AccountId,
    }, // amount, from, to
    Deposited {
        amount: Money,
        from: AccountId,
        to: AccountId,
    }, // amount, from, to
}

#[derive(PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct Money {
    amount: u128,
}

impl Money {
    pub fn zero() -> Money {
        Self::from(0)
    }
    pub fn from(amount: u128) -> Money {
        Money { amount }
    }
}

impl Display for Money {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self.amount)
    }
}

trait DbKey {
    fn to_key(&self) -> &str;
}

impl DbKey for AccountId {
    fn to_key(&self) -> &str {
        self.encode_to_zbase32().trim()
    }
}

impl AccountEvent {
    fn from(self) -> AccountId {
        match self {
            Self::Opened { from, .. } => from,
            Self::Withdrawn { from, .. } => from,
            Self::Deposited { from, .. } => from,
        }
    }

    fn to(self) -> AccountId {
        match self {
            Self::Opened { to, .. } => to,
            Self::Withdrawn { to, .. } => to,
            Self::Deposited { to, .. } => to,
        }
    }

    fn amount(self) -> Money {
        match self {
            Self::Opened { amount, .. } => amount,
            Self::Withdrawn { amount, .. } => amount,
            Self::Deposited { amount, .. } => amount,
        }
    }
}

// Snapshot of Money account
struct Account {
    id: AccountId,
    // The initial balance that the account is created with (probably zero in most
    // cases)
    initial_balance: Money,
    // Set of transfers involving this account (both incoming and outgoing)
    history: BTreeSet<AccountEvent>,
    // Number of validated outgoing transfers from this account
    seq_nr: u64,
    // Set of incoming transfers since the last outgoing transfer (the
    // dependencies of the next outgoing transfer)
    deps: BTreeSet<TransferId>,
}

// Money transfer message
struct Transfer {
    // Public key of the source account
    src: PublicKey,
    // Name of the destination account
    dst: XorName,
    // The amount to transfer
    amount: Money,
    // transfer sequence number (the number of validated outgoing transfers
    // from the `src` account + 1)
    seq_nr: u64,
    // dependencies of this transfer - the set of incoming transfers to the
    // `src` account since the last outgoing transfer
    deps: BTreeSet<TransferId>,
}

// Unique identifier of a Transfer
struct TransferId {
    // The source account of the transfer
    src_account: XorName,
    // The sequence number of the transfer
    seq_nr: u64,
}

// Info about an account needed to initiate a transfer.
struct AccountSnapshot {
    // Account balance
    balance: Money,
    // Account sequence number - the number of validated outgoing transfers
    // from the account.
    seq_nr: u64,
    // Set of incoming transfers into the account since the last validated
    // outgoing transfer.
    deps: BTreeSet<TransferId>,
}
