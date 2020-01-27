
// Copyright 2018-2020 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

use std::{
	collections::HashMap,
	hash,
	sync::Arc,
};
use serde::Serialize;
use crate::{watcher, ChainApi, BlockHash};
use log::{debug, trace, warn};
use futures::{
	future,
	stream::{Stream, StreamExt},
	sink::SinkExt as _,
	executor::{ThreadPool, ThreadPoolBuilder},
	channel::{
		oneshot,
		mpsc::{Sender, self},
	},
};
use std::marker::Unpin;
use sp_runtime::{
	traits::{Block as BlockT, Header as _, self}
};
use crate::pool::BlockFor;
use sp_runtime::generic::BlockId;

/// Extrinsic pool default listener.
pub struct Listener<H: hash::Hash + Eq, C: ChainApi> {
	watchers: HashMap<H, watcher::Sender<H, BlockFor<C>>>,
	finality_watcher: Option<Finalizationlistener<C>>,
}

struct Finalizationlistener<C: ChainApi> {
	api: Arc<C>,
	threadpool: ThreadPool,
	sender: Sender<(crate::NumberFor<C>, oneshot::Sender<()>)>,
}

impl<C: ChainApi> Finalizationlistener<C> {
	fn new<S: 'static>(finality_notifications: S, api: Arc<C>) -> Self
		where
			S: Stream<Item = <<C as ChainApi>::Block as BlockT>::Header> + Unpin + Send
	{
		let threadpool = ThreadPoolBuilder::new()
			// 1 should be enough
			.pool_size(1)
			.create()
			.expect("Failed to create threadpool for FinalityListener");
		let (sender, blocks_to_watch) = mpsc::channel(100);

		threadpool.spawn_ok(finality_tracker::<BlockFor<C>, _, _>(finality_notifications, blocks_to_watch));

		Self {
			api,
			sender,
			threadpool
		}
	}

	fn notify_finality<H: 'static + Clone + Send>(
		&mut self,
		watcher: Option<watcher::Sender<H, BlockFor<C>>>,
		block_hash: BlockHash<C>
	) {
		if watcher.is_none() {
			return
		}

		let mut watcher = watcher.expect("checked for is_none above; qed");

		if let Ok(Some(number)) = self.api.block_id_to_number(&BlockId::hash(block_hash)) {
			let (tx, rx) = oneshot::channel();
			let mut sender = self.sender.clone();
			// in order not to block the current thread, spawn this on a threadpool executor
			self.threadpool.spawn_ok(async move {
				if let Ok(_) = sender.send((number, tx)).await {
					// this future resolves once the block number has been finalized.
					let _ = rx.await;
					watcher.finalized();
				}
			});
		}
	}
}

impl<H: hash::Hash + Eq, C: ChainApi> Default for Listener<H, C> {
	fn default() -> Self {
		Listener {
			watchers: Default::default(),
			finality_watcher: None,
		}
	}
}

impl<H: hash::Hash + traits::Member + Serialize, C: ChainApi> Listener<H, C> {
	fn fire<F>(&mut self, hash: &H, fun: F) where F: FnOnce(&mut watcher::Sender<H, BlockFor<C>>) {
		let clean = if let Some(h) = self.watchers.get_mut(hash) {
			fun(h);
			h.is_done()
		} else {
			false
		};

		if clean {
			self.watchers.remove(hash);
		}
	}

	/// Creates a new watcher for given verified extrinsic.
	///
	/// The watcher can be used to subscribe to lifecycle events of that extrinsic.
	pub fn create_watcher(&mut self, hash: H) -> watcher::Watcher<H, BlockFor<C>> {
		let sender = self.watchers.entry(hash.clone()).or_insert_with(watcher::Sender::default);
		sender.new_watcher(hash)
	}

	/// Notify the listeners about extrinsic broadcast.
	pub fn broadcasted(&mut self, hash: &H, peers: Vec<String>) {
		trace!(target: "txpool", "[{:?}] Broadcasted", hash);
		self.fire(hash, |watcher| watcher.broadcast(peers));
	}

	/// New transaction was added to the ready pool or promoted from the future pool.
	pub fn ready(&mut self, tx: &H, old: Option<&H>) {
		trace!(target: "txpool", "[{:?}] Ready (replaced: {:?})", tx, old);
		self.fire(tx, |watcher| watcher.ready());
		if let Some(old) = old {
			self.fire(old, |watcher| watcher.usurped(tx.clone()));
		}
	}

	/// New transaction was added to the future pool.
	pub fn future(&mut self, tx: &H) {
		trace!(target: "txpool", "[{:?}] Future", tx);
		self.fire(tx, |watcher| watcher.future());
	}

	/// Transaction was dropped from the pool because of the limit.
	pub fn dropped(&mut self, tx: &H, by: Option<&H>) {
		trace!(target: "txpool", "[{:?}] Dropped (replaced by {:?})", tx, by);
		self.fire(tx, |watcher| match by {
			Some(t) => watcher.usurped(t.clone()),
			None => watcher.dropped(),
		})
	}

	/// Transaction was removed as invalid.
	pub fn invalid(&mut self, tx: &H, warn: bool) {
		if warn {
			warn!(target: "txpool", "Extrinsic invalid: {:?}", tx);
		} else {
			debug!(target: "txpool", "Extrinsic invalid: {:?}", tx);
		}
		self.fire(tx, |watcher| watcher.invalid());
	}

	/// Transaction was pruned from the pool.
	pub fn pruned(&mut self, block_hash: BlockHash<C>, tx: &H) {
		debug!(target: "txpool", "[{:?}] Pruned at {:?}", tx, block_hash);
		self.fire(tx, |watcher| watcher.in_block(block_hash.clone()));

		// send the block hash to the background listener task to watch for finalization.
		if let Some(ref mut finality_listener) = self.finality_watcher {
			finality_listener.notify_finality(self.watchers.remove(tx), block_hash);
		}
	}

	/// Register a stream of notifications from the finality provider.
	pub fn register_finality_notifications(
		&mut self,
		finality_notifications: impl Stream<Item = <BlockFor<C> as BlockT>::Header> + Unpin + Send + 'static,
		api: Arc<C>,
	) {
		self.finality_watcher = Some(Finalizationlistener::new(finality_notifications, api));
	}
}

/// consumes a stream of block numbers to watch for and
/// finalized blocks
async fn finality_tracker<B, FS, BS>(
	mut finality_notifications: FS,
	mut blocks_to_watch: BS
)
	where
		B: BlockT,
		FS: Stream<Item = <B as BlockT>::Header> + Unpin,
		BS: Stream<Item = (traits::NumberFor<B>, oneshot::Sender<()>)> + Unpin,
{
	use future::Either::*;
	let mut blocks: Vec<(traits::NumberFor<B>, oneshot::Sender<()>)> = Vec::new();
	loop {
		match future::select(finality_notifications.next(), blocks_to_watch.next()).await {
			Left((Some(finalized), _)) => {
				let mut temp = vec![];
				for (number, sender) in blocks.drain(..) {
					// ideally if a block with a higher number has been finalized, then this one has.
					if *finalized.number() >= number {
						let _ = sender.send(());
					} else {
						temp.push((number, sender))
					}
				}
				blocks.extend(temp);
			},
			Right((Some((block_num, sender)), _)) => {
				blocks.push((block_num, sender))
			},
			// if either stream terminates, then terminate this future.
			Left((None, _)) | Right((None, _)) => break,
		}
	}
}
