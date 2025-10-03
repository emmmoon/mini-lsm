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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        let level_nums = _snapshot.levels.len();
        let last_level_size = _snapshot.levels[level_nums - 1].1.len();
        let mut engine_size = 0;
        for (index, level) in _snapshot.levels.iter().enumerate() {
            if index == level_nums - 1 {
                break;
            }
            engine_size += level.1.len();
        }
        println!(
            "level_nums: {}, last_level_size: {}, engine_size: {}",
            level_nums, last_level_size, engine_size
        );
        if engine_size as f64 / last_level_size as f64
            >= self.options.max_size_amplification_percent as f64 / 100.0f64
        {
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        let mut previous_tiers_size = 0;
        for (index, level) in _snapshot.levels.iter().enumerate() {
            if (index + 1) < self.options.min_merge_width {
                previous_tiers_size += level.1.len();
                continue;
            }
            let this_tier = level.1.len();
            if previous_tiers_size as f64 / this_tier as f64
                >= (self.options.size_ratio as f64 + 100.0f64) / 100.0f64
            {
                println!(
                    "previous_tiers_size: {}, this_tier: {}, n: {}",
                    previous_tiers_size,
                    this_tier,
                    index + 1
                );
                return Some(TieredCompactionTask {
                    tiers: _snapshot
                        .levels
                        .iter()
                        .take(index + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    bottom_tier_included: index == level_nums - 1,
                });
            }
            previous_tiers_size += level.1.len();
        }

        if _snapshot.levels.len() == self.options.num_tiers {
            return None;
        }

        let nums = _snapshot.levels.len() - self.options.num_tiers + 1;
        println!(
            "Reduce Sorted Runs, levels size: {}, num_tiers: {}, nums: {}",
            _snapshot.levels.len(),
            self.options.num_tiers,
            nums
        );
        return Some(TieredCompactionTask {
            tiers: _snapshot
                .levels
                .iter()
                .take(nums)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: false,
        });
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut tier_to_remove = _task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();
        for (tier_id, files) in &snapshot.levels {
            if let Some(ffiles) = tier_to_remove.remove(tier_id) {
                // the tier should be removed
                assert_eq!(ffiles, files, "file changed after issuing compaction task");
                files_to_remove.extend(ffiles.iter().copied());
            } else {
                // retain the tier
                levels.push((*tier_id, files.clone()));
            }
            if tier_to_remove.is_empty() && !new_tier_added {
                // add the compacted tier to the LSM tree
                new_tier_added = true;
                levels.push((_output[0], _output.to_vec()));
            }
        }
        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found??");
        }
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
