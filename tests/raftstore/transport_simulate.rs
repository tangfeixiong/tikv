// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, RwLock};
use std::time::{self, Duration, Instant};
use std::thread;
use std::vec::Vec;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use rand;

use kvproto::raft_serverpb::RaftMessage;
use tikv::raftstore::{Result, Error};
use tikv::raftstore::store::Transport;
use tikv::util::HandyRwLock;
use super::util::new_peer;

pub trait Filter: Send + Sync {
    // in a SimulateTransport, if any filter's before return true, msg will be discard
    fn before(&self, msg: &RaftMessage) -> bool;
    // with after provided, one can change the return value arbitrarily
    fn after(&self, Result<()>) -> Result<()>;
}

struct FilterDropPacket {
    rate: u32,
    drop: AtomicBool,
}

struct FilterDelay {
    duration: time::Duration,
}

impl Filter for FilterDropPacket {
    fn before(&self, _: &RaftMessage) -> bool {
        let drop = rand::random::<u32>() % 100u32 < self.rate;
        self.drop.store(drop, Ordering::Relaxed);
        drop
    }

    fn after(&self, x: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            return Err(Error::Timeout("drop by FilterDropPacket in SimulateTransport".to_string()));
        }
        x
    }
}

impl Filter for FilterDelay {
    fn before(&self, _: &RaftMessage) -> bool {
        thread::sleep(self.duration);
        false
    }
    fn after(&self, x: Result<()>) -> Result<()> {
        x
    }
}

pub struct SimulateTransport<T: Transport> {
    filters: Vec<Box<Filter>>,
    trans: Arc<RwLock<T>>,
}

impl<T: Transport> SimulateTransport<T> {
    pub fn new(trans: Arc<RwLock<T>>) -> SimulateTransport<T> {
        SimulateTransport {
            filters: vec![],
            trans: trans,
        }
    }

    pub fn set_filters(&mut self, filters: Vec<Box<Filter>>) {
        self.filters = filters;
    }
}

impl<T: Transport> Transport for SimulateTransport<T> {
    fn send(&self, msg: RaftMessage) -> Result<()> {
        let mut discard = false;
        for filter in &self.filters {
            if filter.before(&msg) {
                discard = true;
            }
        }

        let mut res = if !discard {
            self.trans.rl().send(msg)
        } else {
            Ok(())
        };

        for filter in self.filters.iter().rev() {
            res = filter.after(res);
        }

        res
    }
}

pub trait FilterFactory {
    fn generate(&self, node_id: u64) -> Vec<Box<Filter>>;
}

pub struct DropPacket {
    rate: u32,
}

impl DropPacket {
    pub fn new(rate: u32) -> DropPacket {
        DropPacket { rate: rate }
    }
}

impl FilterFactory for DropPacket {
    fn generate(&self, _: u64) -> Vec<Box<Filter>> {
        vec![box FilterDropPacket {
                 rate: self.rate,
                 drop: AtomicBool::new(false),
             }]
    }
}

pub struct Delay {
    duration: time::Duration,
}

impl Delay {
    pub fn new(duration: time::Duration) -> Delay {
        Delay { duration: duration }
    }
}

impl FilterFactory for Delay {
    fn generate(&self, _: u64) -> Vec<Box<Filter>> {
        vec![box FilterDelay { duration: self.duration }]
    }
}

struct PartitionFilter {
    node_ids: Vec<u64>,
    drop: AtomicBool,
}

impl Filter for PartitionFilter {
    fn before(&self, msg: &RaftMessage) -> bool {
        let drop = self.node_ids.contains(&msg.get_to_peer().get_store_id());
        self.drop.store(drop, Ordering::Relaxed);
        drop
    }
    fn after(&self, r: Result<()>) -> Result<()> {
        if self.drop.load(Ordering::Relaxed) {
            return Err(Error::Timeout("drop by PartitionPacket in SimulateTransport".to_string()));
        }
        r
    }
}

pub struct Partition {
    s1: Vec<u64>,
    s2: Vec<u64>,
}

impl Partition {
    pub fn new(s1: Vec<u64>, s2: Vec<u64>) -> Partition {
        Partition { s1: s1, s2: s2 }
    }
}

impl FilterFactory for Partition {
    fn generate(&self, node_id: u64) -> Vec<Box<Filter>> {
        if self.s1.contains(&node_id) {
            return vec![box PartitionFilter {
                            node_ids: self.s2.clone(),
                            drop: AtomicBool::new(false),
                        }];
        }
        return vec![box PartitionFilter {
                        node_ids: self.s1.clone(),
                        drop: AtomicBool::new(false),
                    }];
    }
}

pub struct Isolate {
    node_id: u64,
}

impl Isolate {
    pub fn new(node_id: u64) -> Isolate {
        Isolate { node_id: node_id }
    }
}

impl FilterFactory for Isolate {
    fn generate(&self, node_id: u64) -> Vec<Box<Filter>> {
        if node_id == self.node_id {
            return vec![box FilterDropPacket {
                            rate: 100,
                            drop: AtomicBool::new(false),
                        }];
        }
        vec![box PartitionFilter {
                 node_ids: vec![self.node_id],
                 drop: AtomicBool::new(false),
             }]
    }
}

struct MockForCheck {
    // <node_id, msg_count>
    routers: RwLock<HashMap<u64, u64>>,
}

impl MockForCheck {
    fn new(count: usize) -> MockForCheck {
        let mut hash: HashMap<u64, u64> = HashMap::new();
        for i in 0..count {
            hash.insert(i as u64, 0);
        }
        MockForCheck { routers: RwLock::new(hash) }
    }
    fn message_count(&self, node_id: u64) -> u64 {
        self.routers.rl()[&node_id]
    }
    fn reset(&mut self) {
        let mut hash = self.routers.wl();
        for (_, v) in hash.iter_mut() {
            *v = 0;
        }
    }
}

impl Transport for MockForCheck {
    fn send(&self, msg: RaftMessage) -> Result<()> {
        let to = msg.get_to_peer().get_store_id();
        if let Some(count) = self.routers.wl().get_mut(&to) {
            *count += 1
        }
        Ok(())
    }
}

struct Test {
    check: Arc<RwLock<MockForCheck>>,
    trans: Vec<SimulateTransport<MockForCheck>>,
}

impl Test {
    fn new(node_count: usize) -> Test {
        let t = Arc::new(RwLock::new(MockForCheck::new(node_count)));
        let mut trans = vec![];
        for _ in 0..node_count {
            let sim_trans = SimulateTransport::new(t.clone());
            trans.push(sim_trans);
        }
        Test {
            check: t.clone(),
            trans: trans,
        }
    }

    fn reset(&mut self) {
        self.check.wl().reset();
        for tran in &mut self.trans {
            tran.set_filters(vec![]);
        }
    }

    fn send_message(&self, from: u64, to: u64) -> Result<()> {
        let mut msg = RaftMessage::new();
        msg.set_from_peer(new_peer(from, 0));
        msg.set_to_peer(new_peer(to, 0));
        self.trans[from as usize].send(msg)
    }

    fn hook_transport<F: FilterFactory>(&mut self, factory: F) {
        for i in 0..self.trans.len() {
            let filters = factory.generate(i as u64);
            self.trans.get_mut(i).unwrap().set_filters(filters);
        }
    }

    fn message_count(&self, node_id: u64) -> u64 {
        self.check.rl().message_count(node_id)
    }

    fn assert_msg_count(&self, vec: Vec<u64>) {
        for (i, item) in vec.iter().enumerate() {
            assert_eq!(*item, self.message_count(i as u64));
        }
    }
}

#[test]
fn test_transport_filters() {
    let mut test = Test::new(5);

    // testcase for isolate
    test.hook_transport(Isolate::new(3));
    for _ in 0..1000 {
        let r = rand::random::<u64>() % 5;
        let _ = test.send_message(r, 3);
        let _ = test.send_message(3, r);
    }
    test.assert_msg_count(vec![0, 0, 0, 0, 0]);
    let _ = test.send_message(4, 0);
    let _ = test.send_message(2, 1);
    let _ = test.send_message(2, 4);
    test.assert_msg_count(vec![1, 1, 0, 0, 1]);

    // testcase for partition
    test.reset();
    test.hook_transport(Partition::new(vec![0, 1], vec![2, 3, 4]));
    for _ in 0..1000 {
        let (s1, s2) = (rand::random::<u64>() % 2, 2 + rand::random::<u64>() % 3);
        let _ = test.send_message(s1, s2);
        let _ = test.send_message(s2, s1);
    }
    test.assert_msg_count(vec![0, 0, 0, 0, 0]);
    let _ = test.send_message(4, 3);
    let _ = test.send_message(4, 2);
    let _ = test.send_message(3, 2);
    let _ = test.send_message(2, 4);
    let _ = test.send_message(0, 1);
    let _ = test.send_message(1, 0);
    test.assert_msg_count(vec![1, 1, 2, 1, 1]);

    // testcase for drop packet
    test.reset();
    test.hook_transport(DropPacket::new(20));
    for _ in 0..100000 {
        let _ = test.send_message(0, 3);
    }
    assert!(test.message_count(3) > 70000);

    // testcase for delay
    test.reset();
    test.hook_transport(Delay::new(Duration::from_millis(500)));
    let now = Instant::now();
    let _ = test.send_message(0, 3);
    assert!(now.elapsed() > Duration::from_millis(450));
}
