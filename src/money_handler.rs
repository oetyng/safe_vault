// Copyright 2019 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::bank::{AccountEvent, AccountId, Bank, Money};
use crate::{vault::Init, Result};
use safe_nd::NodePublicId;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    path::Path,
};

pub(crate) struct MoneyHandler {
    id: NodePublicId,
    bank: Bank,
}

#[derive(Serialize, Deserialize)]
pub enum AccountCmd {
    Open {
        amount: Money,
        from: AccountId,
        to: AccountId,
    }, // amount, from, to
    Withdraw {
        amount: Money,
        from: AccountId,
        to: AccountId,
    }, // amount, from, to
    Deposit {
        amount: Money,
        from: AccountId,
        to: AccountId,
    }, // amount, from, to
}

impl MoneyHandler {
    pub fn new<P: AsRef<Path>>(id: NodePublicId, root_dir: P, init_mode: Init) -> Result<Self> {
        let bank = Bank::new(id, root_dir, init_mode)?;
        Ok(Self { id, bank })
    }

    /// Opens an account.
    pub fn open_account(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        match self.bank.open_account(amount, from, to) {
            Ok(event) => Ok(self.bank.apply(event)),
            Err(err) => Err(err), // todo: return the error
        }
    }

    /// Initiates a transfer.
    pub fn initiate_transfer(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        // if !self.is_valid_request(requester, from) { [ERROR] }
        match self.bank.withdraw(amount, from, to) {
            Ok(event) => self.complete_transfer(event),
            Err(err) => Err(err),
        }
    }

    /// Deposits a withdrawn amount.
    pub fn deposit(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        match self.bank.deposit(amount, from, to) {
            Ok(event) => Ok(self.bank.apply(event)),
            Err(err) => Err(err),
        }
    }

    fn complete_transfer(&self, event: AccountEvent) -> Result<()> {
        match event {
            AccountEvent::Withdrawn { amount, from, to } => {
                match self.try_deposit(amount, from, to) {
                    Ok(_) => Ok(self.bank.apply(event)),
                    Err(err) => Err(err),
                }
            }
            _ => panic!(), // todo: return an error
        }
    }

    fn try_deposit(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        if self.bank.exists(to) {
            match self.bank.deposit(amount, from, to) {
                Ok(event) => Ok(self.bank.apply(event)),
                Err(err) => Err(err),
            }
        } else {
            self.order_deposit(amount, from, to)
        }
    }

    /// Sends Deposit cmd to other section.
    fn order_deposit(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        // send cmd to other section
        let _cmd = AccountCmd::Deposit { amount, from, to };
        Ok(())
    }
}

impl Display for MoneyHandler {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self.id)
    }
}
    
// TODO:
// /// Protection against Byzantines
// fn is_valid_request(&self, requester: AccountId, cmd: &AccountCmd) -> bool {
//     if cmd.from() != requester {
//         println!(
//             "[INVALID] Transfer from {:?} was was initiated by a proc that does not own this account: {:?}",
//             requester, cmd.from()
//         );
//         false
//     } else {
//         true
//     }
// }