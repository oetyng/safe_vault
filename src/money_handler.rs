// Copyright 2020 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use crate::bank::{AccountEvent, AccountId, Bank};
use crate::{vault::Init, Result};
use safe_nd::{Money, NodePublicId, Signature};
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
    },
    Withdraw {
        amount: Money,
        from: AccountId,
        to: AccountId,
    },
    Deposit {
        amount: Money,
        from: AccountId,
        to: AccountId,
    },
}

impl AccountCmd {
    fn from(self) -> AccountId {
        match self {
            Self::Open { from, .. } => from,
            Self::Withdraw { from, .. } => from,
            Self::Deposit { from, .. } => from,
        }
    }
}

impl MoneyHandler {
    pub fn new<P: AsRef<Path>>(
        section_key: PublicKey,
        id: NodePublicId,
        root_dir: P,
        init_mode: Init,
    ) -> Result<Self> {
        let bank = Bank::new(section_key, id, root_dir, init_mode)?;
        Ok(Self { id, bank })
    }

    /// Handles a signed AccountCmd.
    pub fn handle(&self, cmd: AccountCmd, signature: Signature) -> Result<()> {
        match self.check_signature(cmd, signature) {
            Ok(_) => (), // all good, continue
            Err(error) => return Err(error),
        }
        match cmd {
            Open { amount, from, to } => self.open_account(amount, from, to),
            Withdraw { amount, from, to } => self.withdraw(amount, from, to),
            Deposit { amount, from, to } => self.deposit(amount, from, to),
        }
    }

    /// The from-account must have signed the cmd for it to be valid.
    fn check_signature(&self, cmd: AccountCmd, signature: Signature) -> Result<()> {
        if let Deposit { .. } = cmd {
            // no need for sender to sign this
            return Ok(());
        }
        let sender = cmd.from();
        match sender.verify(&signature, utils::serialise(&cmd)) {
            Ok(_) => Ok(()),
            Err(error) => {
                warn!(
                    "{}: ({:?}/{:?}) from {} is invalid: {}",
                    self, cmd, signature, sender, error
                );
                Err(error.into())
            }
        }
    }

    /// Opens an account. TODO: initiate this at sender
    fn open_account(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        match self.bank.open_account(amount, from, to) {
            Ok(event) => Ok(self.bank.apply(event)),
            Err(err) => Err(err.into()),
        }
    }

    /// Initiates a transfer; withdraws an amount.
    fn withdraw(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        match self.bank.withdraw(amount, from, to) {
            Ok(event) => self.complete_transfer(event),
            Err(err) => Err(err.into()),
        }
    }

    fn complete_transfer(&self, event: AccountEvent) -> Result<()> {
        match event {
            AccountEvent::Withdrawn { amount, from, to } => {
                match self.try_deposit(amount, from, to) {
                    Ok(_) => Ok(self.bank.apply(event)),
                    error => error,
                }
            }
            _ => panic!(), // todo: return an error
        }
    }

    fn try_deposit(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        if self.bank.exists(to) {
            match self.bank.deposit(amount, from, to) {
                Ok(event) => Ok(self.bank.apply(event)),
                Err(err) => Err(err.into()),
            }
        } else {
            self.order_deposit(amount, from, to)
        }
    }

    /// Sends Deposit cmd to other section. TODO: Return Cmd/Action
    fn order_deposit(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        // send cmd to other section
        let _cmd = AccountCmd::Deposit { amount, from, to };
        Ok(())
    }

    /// Concludes a transfer; Deposits a withdrawn amount.
    fn deposit(&self, amount: Money, from: AccountId, to: AccountId) -> Result<()> {
        match self.bank.deposit(amount, from, to) {
            Ok(event) => Ok(self.bank.apply(event)),
            Err(err) => Err(err.into()),
        }
    }
}

impl Display for MoneyHandler {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        write!(formatter, "{}", self.id)
    }
}
