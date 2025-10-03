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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let begin_key = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();

        let mut overlap_ssts = Vec::new();
        for sst_id in &_snapshot.levels[_in_level - 1].1 {
            let sst = &_snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut target_size = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        let mut last_level_size = _snapshot.levels[self.options.max_levels - 1]
            .1
            .iter()
            .map(|x| _snapshot.sstables.get(x).unwrap().table_size())
            .sum::<u64>() as usize;

        for num in 0..self.options.max_levels {
            target_size.insert(0, last_level_size);
            if last_level_size < self.options.base_level_size_mb * 1024 * 1024 {
                last_level_size = 0;
            } else {
                last_level_size /= self.options.level_size_multiplier;
                base_level -= 1;
            }
        }

        if base_level == 0 {
            base_level = 1;
        }

        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush l0 sst to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &_snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priority = 0.0f64;

        let mut select_level = 0;
        for num in 0..(self.options.max_levels - 1) {
            if target_size[num] == 0 {
                continue;
            }
            let current_size = _snapshot.levels[num]
                .1
                .iter()
                .map(|x| _snapshot.sstables.get(x).unwrap().table_size())
                .sum::<u64>() as usize;

            let current_priority = current_size as f64 / target_size[num] as f64;
            if current_priority > 1.0f64 && current_priority > priority {
                select_level = num + 1;
                priority = current_priority;
            }
            println!(
                "l {}, current_size: {}, target_size: {}, priority: {}",
                (num + 1),
                current_size,
                target_size[num],
                current_priority,
            );
        }

        if select_level > 0 {
            let select_sst = _snapshot.levels[select_level - 1]
                .1
                .iter()
                .min()
                .copied()
                .unwrap();
            return Some(LeveledCompactionTask {
                upper_level: Some(select_level),
                upper_level_sst_ids: vec![select_sst],
                lower_level: select_level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &[select_sst],
                    select_level + 1,
                ),
                is_lower_level_bottom_level: (select_level + 1) == self.options.max_levels,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut upper_level_sst_ids_set = _task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_ids_set = _task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(upper_level) = _task.upper_level {
            let new_upper_level_ssts = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        } else {
            let new_l0_ssts = snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            snapshot.l0_sstables = new_l0_ssts;
        }
        files_to_remove.extend(&_task.upper_level_sst_ids);
        files_to_remove.extend(&_task.lower_level_sst_ids);

        let mut new_lower_level_ssts = snapshot.levels[_task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| {
                if lower_level_sst_ids_set.remove(x) {
                    return None;
                }
                Some(*x)
            })
            .collect::<Vec<_>>();
        new_lower_level_ssts.extend(_output);
        if !_in_recovery {
            new_lower_level_ssts.sort_by(|x, y| {
                snapshot
                    .sstables
                    .get(x)
                    .unwrap()
                    .first_key()
                    .cmp(snapshot.sstables.get(y).unwrap().first_key())
            });
        }
        snapshot.levels[_task.lower_level - 1].1 = new_lower_level_ssts;
        (snapshot, files_to_remove)
    }
}
