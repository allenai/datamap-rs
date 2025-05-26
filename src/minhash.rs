/*================================================================================
=                            MINHASH LSH
Mostly a Claude 4 Opus-generated implementation of MinHash LSH.

It performs in-memory deduplication of text lines.

Reviewed by Luca Soldaini (@soldni)


Example usage:

```rust
fn main() {
    // Example usage
    let text = r#"The quick brown fox jumps over the lazy dog
The quick brown fox jumps over the lazy cat
A completely different line here
The quick brown fox leaps over the lazy dog
Some other content that is unique
The quick brown fox jumps over a lazy dog
Another unique line with different content
The quick brown fox jumped over the lazy dog"#;

    println!("Original text ({} lines):", text.lines().count());
    println!("{}\n", text);

    // Create deduplicator with 80% similarity threshold
    let dedup = MinHashLSH::new(128, 0.8);

    // Deduplicate with 3-gram shingles
    let result = dedup.deduplicate(text, 3);

    println!("Deduplicated text ({} lines):", result.lines().count());
    println!("{}", result);

    // Show which lines were grouped together
    println!("\nSimilar line groups:");
    let groups = dedup.find_similar_groups(text, 3);
    for (i, group) in groups.iter().enumerate() {
        println!("Group {}:", i + 1);
        for line in group {
            println!("  - {}", line);
        }
    }

    // Show configuration
    println!("\nConfiguration:");
    println!("- Hash functions: {}", dedup.num_perm);
    println!("- Bands: {}", dedup.num_bands);
    println!("- Rows per band: {}", dedup.band_width);
    println!("- Similarity threshold: {:.0}%", dedup.threshold * 100.0);
}
```

================================================================================*/
use std::collections::{HashMap, HashSet};
use std::hash::{Hasher};
use fxhash::FxHasher;
use rayon::prelude::*;
use regex::Regex;

#[derive(Debug, Clone)]
pub struct MinHashLSH {
    num_perm: usize,
    num_bands: usize,
    band_width: usize,
    threshold: f32,
    seeds: Vec<u64>,
    line_filter_regex: Regex,
    line_min_length: usize,
}

struct LineInfo {
    index: usize,
    length: usize,
    signature: Vec<u64>,
}

impl MinHashLSH {
    pub fn new(num_perm: usize, threshold: f32, line_expr: Option<&str>, line_min_length: usize) -> Self {
        let (num_bands, band_width) = Self::optimal_bands(num_perm, threshold);

        // Pre-compute seeds
        let mut seeds = Vec::with_capacity(num_perm);
        let mut hasher = FxHasher::default();
        let line_filter_regex = match line_expr {
            Some(expr) => Regex::new(expr).unwrap(), // load from user-provided regex
            None => Regex::new(r".?").unwrap(), // at least one character
        };

        for i in 0..num_perm {
            hasher.write_u64(i as u64);
            hasher.write_u64(0x517cc1b727220a95);
            seeds.push(hasher.finish());
            hasher = FxHasher::default(); // Reset
        }

        Self {
            num_perm,
            num_bands,
            band_width,
            threshold,
            seeds,
            line_filter_regex,
            line_min_length,
        }
    }

    fn optimal_bands(num_perm: usize, threshold: f32) -> (usize, usize) {
        let mut best_bands = 1;
        let mut best_width = num_perm;
        let mut best_error = f32::MAX;

        for bands in 1..=num_perm {
            if num_perm % bands == 0 {
                let width = num_perm / bands;
                let prob = (1.0 / bands as f32).powf(1.0 / width as f32);
                let error = (prob - threshold).abs();

                if error < best_error {
                    best_error = error;
                    best_bands = bands;
                    best_width = width;
                }
            }
        }

        (best_bands, best_width)
    }

    #[inline]
    fn minhash_signature_fast(&self, text: &str, k: usize) -> Vec<u64> {
        let mut mins = vec![u64::MAX; self.num_perm];
        let bytes = text.as_bytes();

        if bytes.len() < k {
            // For short texts, hash the whole thing
            for (i, &seed) in self.seeds.iter().enumerate() {
                let mut hasher = FxHasher::default();
                hasher.write_u64(seed);
                hasher.write(bytes);
                mins[i] = hasher.finish();
            }
            return mins;
        }

        // Process shingles without storing them
        for window in bytes.windows(k) {
            // Hash window once
            let mut base_hasher = FxHasher::default();
            base_hasher.write(window);
            let base_hash = base_hasher.finish();

            // Mix with seeds
            for (i, &seed) in self.seeds.iter().enumerate() {
                // Fast mixing: XOR and multiply
                let hash = base_hash ^ seed;
                let hash = hash.wrapping_mul(0x9e3779b97f4a7c15);

                if hash < mins[i] {
                    mins[i] = hash;
                }
            }
        }

        mins
    }

    #[inline]
    fn hash_band_fast(&self, signature: &[u64], band_idx: usize) -> u64 {
        let start = band_idx * self.band_width;
        let end = start + self.band_width;

        // XOR folding
        let mut hash = band_idx as u64;
        for i in start..end {
            hash ^= signature[i].rotate_left(((i - start) * 8) as u32);
        }

        // Final mix
        hash ^= hash >> 33;
        hash = hash.wrapping_mul(0xff51afd7ed558ccd);
        hash ^= hash >> 33;
        hash
    }

    #[inline]
    fn estimate_similarity_fast(&self, sig1: &[u64], sig2: &[u64]) -> Option<f32> {
        let total = sig1.len();
        let min_matches = (total as f32 * self.threshold).ceil() as usize;
        let max_mismatches = total - min_matches;

        let mut matches = 0;
        let mut mismatches = 0;

        for (a, b) in sig1.iter().zip(sig2.iter()) {
            if a == b {
                matches += 1;
            } else {
                mismatches += 1;
                if mismatches > max_mismatches {
                    return None; // Early termination
                }
            }
        }

        Some(matches as f32 / total as f32)
    }

    pub fn deduplicate(&self, text: &str, shingle_size: usize) -> String {
        let lines: Vec<&str> = text.lines().collect();
        if lines.is_empty() {
            return String::new();
        }

        // Parallel signature generation with line info
        let line_infos: Vec<LineInfo> = lines
            .par_iter()
            .enumerate()
            .map(|(index, line)| {
                LineInfo {
                    index,
                    length: line.len(),
                    signature: self.minhash_signature_fast(line, shingle_size),
                }
            })
            .collect();

        // Build LSH index
        let mut lsh_index: HashMap<(usize, u64), Vec<usize>> = HashMap::new();

        for line_info in &line_infos {
            for band_idx in 0..self.num_bands {
                let band_hash = self.hash_band_fast(&line_info.signature, band_idx);
                lsh_index
                    .entry((band_idx, band_hash))
                    .or_insert_with(Vec::new)
                    .push(line_info.index);
            }
        }

        // Find candidates with length filtering
        let mut candidates: Vec<(usize, usize)> = Vec::new();

        for (_, indices) in lsh_index.iter() {
            if indices.len() > 1 {
                for i in 0..indices.len() {

                    // skip if the line is too short
                    if line_infos[indices[i]].length < self.line_min_length {
                        continue;
                    }

                    // skip if the line does not match the regex
                    if !self.line_filter_regex.is_match(lines[indices[i]]) {
                        continue;
                    }

                    for j in i + 1..indices.len() {
                        let idx1 = indices[i];
                        let idx2 = indices[j];

                        // Length-based pre-filtering
                        let len1 = line_infos[idx1].length;
                        let len2 = line_infos[idx2].length;
                        let len_ratio = len1 as f32 / len2 as f32;

                        if len_ratio >= 0.5 && len_ratio <= 2.0 {
                            candidates.push((idx1.min(idx2), idx1.max(idx2)));
                        }
                    }
                }
            }
        }

        // Deduplicate candidates
        candidates.sort_unstable();
        candidates.dedup();

        // Parallel verification of candidates
        let verified_pairs: Vec<(usize, usize)> = candidates
            .par_iter()
            .filter_map(|&(idx1, idx2)| {
                if let Some(sim) = self.estimate_similarity_fast(
                    &line_infos[idx1].signature,
                    &line_infos[idx2].signature,
                ) {
                    if sim >= self.threshold {
                        return Some((idx1, idx2));
                    }
                }
                None
            })
            .collect();

        // let mut already_seen = HashSet::new();
        // for (idx1, idx2) in &verified_pairs {
        //     if !already_seen.contains(&lines[*idx1]) || !already_seen.contains(&lines[*idx2]) {
        //         println!("Similarity ({}, {})\n\t{}\n\t{}\n\n", idx1, idx2, lines[*idx1], lines[*idx2]);
        //         already_seen.insert(lines[*idx1]);
        //         already_seen.insert(lines[*idx2]);
        //     }
        // }

        // Build groups (sequential - hard to parallelize efficiently)
        let mut groups: Vec<HashSet<usize>> = Vec::new();
        let mut line_to_group: HashMap<usize, usize> = HashMap::new();

        for (idx1, idx2) in verified_pairs {
            match (line_to_group.get(&idx1), line_to_group.get(&idx2)) {
                (Some(&g1), Some(&g2)) if g1 != g2 => {
                    // Merge groups
                    let (merge_from, merge_to) = (g1.min(g2), g1.max(g2));
                    let items = groups[merge_from].clone();
                    for item in items {
                        groups[merge_to].insert(item);
                        line_to_group.insert(item, merge_to);
                    }
                    groups[merge_from].clear();
                }
                (Some(&g), None) => {
                    groups[g].insert(idx2);
                    line_to_group.insert(idx2, g);
                }
                (None, Some(&g)) => {
                    groups[g].insert(idx1);
                    line_to_group.insert(idx1, g);
                }
                (None, None) => {
                    let group_idx = groups.len();
                    let mut group = HashSet::new();
                    group.insert(idx1);
                    group.insert(idx2);
                    groups.push(group);
                    line_to_group.insert(idx1, group_idx);
                    line_to_group.insert(idx2, group_idx);
                }
                _ => {}
            }
        }

        // Build result
        let mut keep_lines = HashSet::new();

        for idx in 0..lines.len() {
            if !line_to_group.contains_key(&idx) {
                keep_lines.insert(idx);
            }
        }

        for group in groups.iter() {
            if !group.is_empty() {
                keep_lines.insert(*group.iter().min().unwrap());
            }
        }

        let mut result = Vec::new();
        for (idx, line) in lines.iter().enumerate() {
            if keep_lines.contains(&idx) {
                result.push(*line);
            }
        }

        result.join("\n")
    }
}
