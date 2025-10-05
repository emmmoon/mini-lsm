// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    fn estimated_size(&self) -> usize {
        2 + self.offsets.len() * 2 + self.data.len()
    }

    fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
        let mut i = 0;
        loop {
            if i >= first_key.key_len() || i >= key.key_len() {
                break;
            }
            if first_key.key_ref()[i] != key.key_ref()[i] {
                break;
            }
            i += 1;
        }
        i
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.estimated_size() + key.key_len() + value.len() + 6 > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // compute overlap part length
        let overlap_len = Self::compute_overlap(self.first_key.as_key_slice(), key);
        // encode offset
        self.offsets.push(self.data.len() as u16);
        // encode overlap length
        self.data.put_u16(overlap_len as u16);
        // encode not-overlap length
        self.data.put_u16((key.key_len() - overlap_len) as u16);
        // encode not-overlap part
        self.data.put(&key.key_ref()[overlap_len..]);
        // encode ts
        self.data.put_u64(key.ts());
        // encode value len
        self.data.put_u16(value.len() as u16);
        // encode value
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
