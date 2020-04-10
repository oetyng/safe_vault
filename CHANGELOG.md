# Safe Vault - Change Log

## [0.23.0]
- Enable required features in self-update dependency to support untar and unzip for packages
- Add tarpaulin to GHA and push result to coveralls
- Update to latest routing

## [0.22.0]
- Support a --update-only flag which only attempts to self-update binary
- Update readme to link to contributing guidelines doc
- Don't send client requests to PARSEC
- Add dead_code on vote_for_action
- Fix spacing bug for clippy

## [0.21.0]
- improve app authorisation
- introduce ParsecAction
- complete indirection between vote and consensus
- replace other payed action but login
- add basic mock routing functions
- send requests for deleting data via concensus
- send create login packet requests to parsec
- send transfer Money via consensus and rename
- fix root_dir by using project's data dir by default
- Send UpdateLoginPacket req via consensus
- update to safe-nd 0.4.0
- refactor test to verify granulated app permissions
- allow to use multiple vaults for tests
- add consensus votes on CreateAccount requests
- send insert and delete auth keys request via…
- change usage of idata term "kind" to "data"
- introduce IDataRequest to eliminate unwraps
- handle refunds when errors are returned from the data
- deprecate Rpc::Refund to use Rpc::Response instead
- test that refunds are processed
- introduce util function to calculate refund and move
- remove explicitly-listed non-warn lints
- send CreateAccount request via consensus
- add `phase-one` feature for updates
- look up by message id
- notify all connected instances of the client
- Merge pull request #891 from ustulation/lookup-by-msg-id
- integrate with Routing
- fix clippy warnings and test failure
- fix nikita's PR #888
- Merge pull request #892 from ustulation/integrate-with-routing
- forbid unsafe and add badge
- use mock quic-p2p from routing
- add --dump-completions flag similar to that of safe-cli
- rename --dump-completions to --completions for consisten…
- fix test case
- fix smoke test failure.
- make vault rng seeded from routing's
- Merge pull request #912 from maqi/use_same_rng
- add caching and other changes to GHA
- resolve non-producable issue
- Merge pull request #914 from maqi/use_same_rng
- support connection info requests from clients
- support new handshake format
- handle bootstrap request properly
- bootstrap request fixes
- update testing framework to new handshake format
- Merge pull request #911 from octol/new-bootstrap-handshake-format
- use real routing for the integration test
- make client requests handled by vault network
- enable tests with feature of mock
- Small tidy up of imports for routing API cleanup
- fix clippy::needless_pass_by_value
- Re-order use and pub use statements.
- include path info in error strings
- fixup/vault: formatting
- Merge pull request #919 from dirvine/vNext
- Merge pull request #909 from dan-da/completions_pr
- update routing dependecy
- upgrade routing and avoid calling node poll test function
- Merge pull request #924 from jeanphilippeD/upgade_routing_2
- Use mock-quic-p2p crate instead of routing module
- Enable test logging via pretty_env_logger
- Enable reproducible tests
- Remove pretty_env_logger dependency (use env_logger only)
- Merge pull request #925 from madadam/mock-quic-p2p
- Update codeowners
- update to latest routing API
- Merge pull request #928 from nbaksalyar/update-routing
- remove refund for login packet exists errors
- update to use routing exposed event mod
- Update to latest routing
- Update dependencies: routing, safe-nd and mock-quic-p2p
- improve error message and avoid duplicate consensus
- pull routing and quic_p2p with separate client channel
- Merge pull request #939 from jeanphilippeD/client_channel
- use latest version of self_update
- Update version to 0.20.1
- update routing and quic-p2p dependencies
- support --log-dir arg to optionally log to a file instead o…
- remove crate filter for logs, and make self-update to b…
- Update to latest routing
- run cargo update

## [0.20.1]
- Set execution permission on safe_vault binary before packaging for release

## [0.20.0]
- Return `AccessDenied` when apps request to insert, delete or list auth keys instead of ignoring the request.
- Use project's data directory as the root directory.
- Upgrade safe-nd dependency to 0.4.0. This includes granular permissions for applications.
- Change usage of idata term "kind" to "data".
- Introduce IDataRequest to eliminate unwraps.
- Handle refunds when errors are returned from the data handlers.
- Deprecate Rpc::Refund to use Rpc::Response instead.
- Send response to the client that sent the request instead of all connected clients.
- Use GitHub actions for build and release.

## [0.19.2]
- This is a technical release without any changes from `0.19.1`.

## [0.19.1]
- Add `verbose` command line flag.
- Fix the UX problem related to the self-update process (requiring to have a network connectivity even if a user just wanted to display the `--help` menu).
- Improve the release process, adding `.zip` and `.tar.gz` packages to distribution.

## [0.19.0]
- Rewrite the Vault code.
- Support new data types (AppendOnlyData, unpublished sequenced/unsequenced MutableData, and unpublished ImmutableData).
- Support coin operations.
- Use quic-p2p for communication with clients.
- Temporarily remove the Routing dependency.
- Refactor the personas system into a new Vault architecture.
- Use Rust stable / 2018 edition.

## [0.18.0]
- Improve Docker configuration scripts (thanks to @mattmccarty)
- Use rust 1.22.1 stable / 2017-11-23 nightly
- rustfmt 0.9.0 and clippy-0.0.174

## [0.17.2]
- Update dependencies.

## [0.17.1]
- Change test to use value of MAX_MUTABLE_DATA_ENTRIES rather than magic numbers.

## [0.17.0]
- Remove proxy rate exceed event.

## [0.16.1-2]
- Update to use Routing 0.32.2.

## [0.16.0]
- Use Routing definitions for group size and quorum.
- Add dev config options to allow running a local testnet.
- Update to use Routing 0.32.0.
- Update to use Rust Stable 1.19.0 / Nightly 2017-07-20, Clippy 0.0.144, and rustfmt 0.9.0.
- Improve DataManager tests.

## [0.15.0]
- Deprecate and remove support for Structured, PrivAppendable and PubAppendable Data.
- Add support for MutableData instead.
- MaidManagers only charge on put success now.
- MaidManagers charge by directly storing the MsgIds and counting the number of them to determine the account balance.
- MaidManagers support insertion and deletion of auth-keys to support auth-app paradigm in which all mutations on behalf of the owner of the account has to go via the MaidManagers.

## [0.14.0]
- Upgrade to routing 0.28.5.
- Migrate from rustc-serialize to serde.
- Migrate from docopt to clap.
- Implement invitation-based account creation.

## [0.13.2]
- Upgrade to routing 0.28.4.

## [0.13.1]
- Upgrade to routing 0.28.2.

## [0.13.0]
- Migrate to routing 0.28.0.
- Use a single event loop for routing and safe_vault.
- Fix issues with account creation and data requests.

## [0.12.1]
- Enforce data size caps.
- Enable new delete mechanism.

## [0.12.0]
- Handle appendable data types in data manager.
- Fix a synchronisation problem in Put/Post handling.

## [0.11.0]
- Use rust_sodium instead of sodiumoxide.
- Upgrade to routing 0.23.4, with merged safe_network_common.

## [0.10.6]
- Revert to routing 0.23.3.

## [0.10.5]
- Update the crate documentation.
- Upgrade to routing 0.25.0.

## [0.10.4]
- Remove spammy trace statement.

## [0.10.3]
- Set default Put limit to 500 and default chunk store limit to 2 GB.

## [0.10.2]
- Prevent vaults from removing existing chunk_store when terminating.

## [0.10.1]
- Fix chunk store directory handling.
- Remove remaining uses of the thread-local random number generator to make
  tests deterministic.
- Make data manager statistics less verbose to reduce spam in the logs.

## [0.10.0]
- Merge chunk_store into safe_vault and make its root directory configurable.
- Implement caching for immutable data.

## [0.9.0]
- Migrate to the mio-based Crust and the new Routing Request/Response API.
- Handle `GetAccountInfo` requests to provide information about a client's used
  and remaining chunk count.

## [0.8.1]
- Allow passing `--first` via command line to start the first Vault of a new network.
- Updated dependencies.

## [0.8.0]
- Several tweaks to churn handling in data_manager.
- Implement process to automatically build release binaries.
- Re-organise the tests to use mock Crust instead of mock Routing.
- Improve logging.
- Fix several bugs.

## [0.7.0]
- Restart routing if it failed to join the network.
- Reimplement the refresh algorithm for structured and immutable data to make it
  less wasteful and more reliable.

## [0.6.0]
- Major change of persona strategy regarding `ImmutableData` (removal of three personas)
- Major refactoring of integration tests (uses mock Crust feature)
- Default test runner to unit tests (previously run using the mock Routing feature)

## [0.5.0]
- Replaced use of local Client errors for those in safe_network_common
- Swapped dependency on mpid_messaging crate for safe_network_common dependency
- Removed Mpid tests from CI suite
- Updated some message flows
- Completed churn-handling for ImmutableDataManager
- Added many unit tests
- Fixed Clippy warnings
- Several bugfixes

## [0.4.0]
- Accommodated updates to dependencies' APIs
- Ensured that the network can correctly handle Clients doing a Get for ImmutableData immediately after doing a Put
- Reduced `REPLICANTS` and `MIN_REPLICANTS` to 4

## [0.3.0]
- Major refactor to accommodate changed Routing

## [0.1.6]
- Default to use real Routing rather than the mock
- Updated config file to match Crust changes
- Refactored flow for put_response
- Added churn tests
- Refactored returns from most persona functions to not use Result

## [0.1.5]
- Major refactor of production code and tests to match Routing's new API, allowing testing on a real network rather than a mock
- Updated installers to match Crust's config/bootstrap file changes
- Added tarball to packages being generated
- Dropped usage of feature-gated items

## [0.1.4]
- [MAID-1283](https://maidsafe.atlassian.net/browse/MAID-1283) Rename repositories from "maidsafe_" to "safe_"

## [0.1.3]
- [MAID-1186](https://maidsafe.atlassian.net/browse/MAID-1186) Handling of unified Structrued Data
    - [MAID-1187](https://maidsafe.atlassian.net/browse/MAID-1187) Updating Version Handler
    - [MAID-1188](https://maidsafe.atlassian.net/browse/MAID-1188) Updating other personas if required

## [0.1.2] - code clean up
- [MAID 1185](https://maidsafe.atlassian.net/browse/MAID-1185) using unwrap unsafely

## [0.1.1]
- Updated dependencies' versions
- Fixed lint warnings caused by latest Rust nightly
- [Issue 117](https://github.com/maidsafe/safe_vault/issues/117) meaningful type_tag
- [PR 124](https://github.com/maidsafe/safe_vault/pull/124) integration test with client
    - client log in / log out
    - complete put flow
    - complete get flow

## [0.1.0] - integrate with routing and safecoin farming initial work [rust-2 Sprint]
- [MAID-1107](https://maidsafe.atlassian.net/browse/MAID-1107) Rename actions (changes in routing v0.1.60)
- [MAID-1008](https://maidsafe.atlassian.net/browse/MAID-1008) Documentation
    - [MAID-1009](https://maidsafe.atlassian.net/browse/MAID-1009) Personas
        - ClientManager : MaidManager
        - NodeManager : PmidManager
        - Node : PmidNode
        - NAE : DataManager, VersionHandler
    - [MAID-1011](https://maidsafe.atlassian.net/browse/MAID-1011) Accounting
        - MaidAccount : create, update and monitor
        - PmidAccount : create, update and monitor
    - [MAID-1010](https://maidsafe.atlassian.net/browse/MAID-1010) Flows
        - PutData / PutResponse
        - GetData / GetResponse
        - PostData
- [MAID-1013](https://maidsafe.atlassian.net/browse/MAID-1013) Complete unfinished code (if it will be covered by the later-on tasks in this sprint, explicitly mention it as in-code TODO comment), especially in vault.rs
    - [MAID-1109](https://maidsafe.atlassian.net/browse/MAID-1109) handle_get_key
    - [MAID-1112](https://maidsafe.atlassian.net/browse/MAID-1112) handle_put_response
    - [MAID-1113](https://maidsafe.atlassian.net/browse/MAID-1113) handle_cache_get
    - [MAID-1113](https://maidsafe.atlassian.net/browse/MAID-1113) handle_cache_put
- [MAID-1014](https://maidsafe.atlassian.net/browse/MAID-1014) Integration test with new routing and crust (vaults bootstrap and network setup)
    - [MAID-1028](https://maidsafe.atlassian.net/browse/MAID-1028) local joining test (process counting)
    - [MAID-1016](https://maidsafe.atlassian.net/browse/MAID-1016) network example (nodes populating)
- [MAID-1012](https://maidsafe.atlassian.net/browse/MAID-1012) SafeCoin farming (new persona may need to be introduced, the task needs to be ‘expandable’ ) documentation
    - farming
    - account
- [MAID-1021](https://maidsafe.atlassian.net/browse/MAID-1021) Implement handling for Safecoin farming rate
    - Farming rate determined by the Sacrificial copies.
    - Farming rate drops when more copies are available and rises when less copies are available.

## [0.0.0 - 0.0.3]
- VaultFacade initial implementation
- Chunkstore implementation and test
- Initial Persona implementation :
    - Implement MaidManager and test
    - Implement DataManager and test
    - Implement PmidManager and test
    - Implement PmidNode and test
    - Implement VersionHandler
- Flow related work :
    - Complete simple Put flow and test
    - Complete simple Get flow and test
    - Complete Create Maid Account Flow
- Installers (linux deb/rpm 32/64 bit, Windows 32 / 64. OSX)
- Coverage analysis
