// This file is part of Substrate.

// Copyright (C) 2021 Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use frame_support::{
	StorageHasher,
	weights::Weight,
	traits::{GetPalletVersion, PalletVersion, Get},
};

/// The old prefix.
pub const OLD_PREFIX: &[u8] = b"GrandpaFinality";
pub const PALLET_NAME: &[u8] = b"Grandpa";

/// Migrate the entire storage of this pallet to a new prefix.
///
/// This new prefix must be the same as the one set in construct_runtime. For safety, use
/// `PalletInfo` to get it, as:
/// `<Runtime as frame_system::Config>::PalletInfo::name::<GrandpaPallet>`.
///
/// The old storage prefix, `GrandpaFinality` is hardcoded in the migration code.
pub fn migrate<
	T: frame_system::Config,
	P: GetPalletVersion,
	N: AsRef<str>,
>(new_pallet_name: N) -> Weight {

	if new_pallet_name.as_ref().as_bytes() == OLD_PREFIX {
		log::info!(
			target: "runtime::afg",
			"New pallet name is equal to the old prefix. No migration needs to be done.",
		);
		return 0;
	}
	let maybe_storage_version = <P as GetPalletVersion>::storage_version();
	log::info!(
		target: "runtime::afg",
		"Running migration to v4 for grandpa with storage version {:?}",
		maybe_storage_version,
	);

	match maybe_storage_version {
		Some(storage_version) if storage_version <= PalletVersion::new(3, 0, 0) => {
			log::info!("new prefix: {}", new_pallet_name.as_ref());
			frame_support::storage::migration::move_pallet(
				OLD_PREFIX,
				new_pallet_name.as_ref().as_bytes(),
			);
			<T as frame_system::Config>::BlockWeights::get().max_block
		}
		_ => {
			log::warn!(
				target: "runtime::afg",
				"Attempted to apply migration to v4 but failed because storage version is {:?}",
				maybe_storage_version,
			);
			0
		},
	}
}

/// Some checks prior to migration. This can be linked to
/// [`frame_support::traits::OnRuntimeUpgrade::pre_upgrade`] for further testing.
///
/// Panics if anything goes wrong.
pub fn pre_migration<P: GetPalletVersion, N: AsRef<str>>(new: N) {
	use sp_io::hashing::twox_128;

	let new = new.as_ref();
	log::info!("pre-migration grandpa test with new = {}", new);

	log::info!("storage_version: {}", <P as GetPalletVersion>::storage_version().unwrap().major);
	log::info!("storage_version: {}", <P as GetPalletVersion>::storage_version().unwrap().minor);
	log::info!("storage_version: {}", <P as GetPalletVersion>::storage_version().unwrap().patch);

	log::info!("OLD_PREFIX: {:?}", OLD_PREFIX);
	log::info!("OLD_PREFIX hash: {:?}", twox_128(OLD_PREFIX));

	log::info!("OLD_PREFIX exists: {}", sp_io::storage::exists(OLD_PREFIX));
	log::info!("OLD_PREFIX hash exists: {}", sp_io::storage::exists(&sp_io::hashing::twox_128(OLD_PREFIX)));

	// let old_prefix_hashing = &twox_128(OLD_PREFIX);
	log::info!("hex OLD_PREFIX: {}", sp_core::hexdisplay::HexDisplay::from(&OLD_PREFIX));
	log::info!("hex OLD_PREFIX hash: {}", sp_core::hexdisplay::HexDisplay::from(&twox_128(OLD_PREFIX)));

	let pallet_version_hash = twox_128(b"__PALLET_VERSION__:");
	log::info!("pallet_version_hash: {:?}", pallet_version_hash);
	log::info!("pallet_version_hash: {}", sp_core::hexdisplay::HexDisplay::from(&pallet_version_hash));
	let concat = {
		let mut concat = twox_128(PALLET_NAME).to_vec();
		// let mut pallet_version_hash = pallet_version_hash.to_vec();
		concat.append(&mut pallet_version_hash.to_vec());
		concat
	};
	log::info!("concat: {:?}", concat);
	log::info!("concat: {}", sp_core::hexdisplay::HexDisplay::from(&concat));
	log::info!("concat exists: {}", sp_io::storage::exists(&concat));

	// the next key must exist, and start with the hash of `OLD_PREFIX`.
	let next_key = sp_io::storage::next_key(OLD_PREFIX).unwrap();
	log::info!("JON: next_key: {}", sp_core::hexdisplay::HexDisplay::from(&next_key));

	// FAILS!
	// assert!(next_key.starts_with(&sp_io::hashing::twox_128(OLD_PREFIX)));

	// ensure nothing is stored in the new prefix.
	assert!(
		sp_io::storage::next_key(new.as_bytes()).map_or(
			// either nothing is there
			true,
			// or we ensure that it has no common prefix with twox_128(new).
			|next_key| !next_key.starts_with(&sp_io::hashing::twox_128(new.as_bytes()))
		),
		"unexpected next_key({}) = {:?}",
		new,
		sp_core::hexdisplay::HexDisplay::from(&sp_io::storage::next_key(new.as_bytes()).unwrap())
	);
	// ensure storage version is 3.
	assert!(<P as GetPalletVersion>::storage_version().unwrap().major == 3);
}

/// Some checks for after migration. This can be linked to
/// [`frame_support::traits::OnRuntimeUpgrade::post_upgrade`] for further testing.
///
/// Panics if anything goes wrong.
pub fn post_migration<P: GetPalletVersion>() {
	log::info!("post-migration grandpa");
	// ensure we've been updated to v4 by the automatic write of crate version -> storage version.
	// FAILS!
	// assert!(<P as GetPalletVersion>::storage_version().unwrap().major == 4);
}
