//! Chunked ordered index with O(log n) rank and by-rank access.
//!
//! Entries live in sorted chunks of roughly 512 items. A Fenwick
//! tree over the chunk lengths gives prefix counts, so:
//!
//! - `insert` / `remove`: O(log #chunks) to find the chunk, O(log chunk) to
//!   find the slot, O(chunk) to shift items, O(log #chunks) to update counts.
//!   Splitting, merging, or dropping a chunk (rare, about once per
//!   `CHUNK_TARGET` operations) costs O(#chunks) to rebuild the tree.
//! - `rank_of` / `get`: O(log n).
//! - iteration from a rank or a bound: O(log n) to seek, then O(1) per item.
//!
//! The state sits behind one `Box` so the index is a single pointer wide.

use std::ops::{Bound, RangeBounds};

/// Chunks are split in half once they exceed this many items.
const CHUNK_SPLIT: usize = 1024;
/// Neighbouring chunks are merged only when the result has at most this many items.
const CHUNK_TARGET: usize = 512;
/// A chunk smaller than this is a merge candidate after a removal.
const CHUNK_MIN: usize = 128;

#[derive(Debug, Clone)]
struct Inner<T> {
    chunks: Vec<Vec<T>>,
    /// Fenwick tree over chunk lengths, 1-based; `tree[0]` is unused.
    tree: Vec<usize>,
    len: usize,
}

/// Ordered set of unique items with O(log n) rank queries.
#[derive(Debug, Clone)]
pub struct OrderedIndex<T> {
    inner: Box<Inner<T>>,
}

impl<T> Default for OrderedIndex<T> {
    fn default() -> Self {
        Self {
            inner: Box::new(Inner {
                chunks: Vec::new(),
                tree: vec![0],
                len: 0,
            }),
        }
    }
}

impl<T> Inner<T> {
    fn rebuild_tree(&mut self) {
        let n = self.chunks.len();
        self.tree.clear();
        self.tree.push(0);
        self.tree.extend(self.chunks.iter().map(Vec::len));
        for i in 1..=n {
            let parent = i + (i & i.wrapping_neg());
            if parent <= n {
                let value = self.tree[i];
                self.tree[parent] += value;
            }
        }
    }

    fn tree_add(&mut self, chunk: usize, delta: isize) {
        let n = self.chunks.len();
        let mut i = chunk + 1;
        while i <= n {
            self.tree[i] = self.tree[i].wrapping_add_signed(delta);
            i += i & i.wrapping_neg();
        }
    }

    /// Number of items in chunks `0..chunk`.
    fn prefix(&self, chunk: usize) -> usize {
        let mut i = chunk;
        let mut sum = 0;
        while i > 0 {
            sum += self.tree[i];
            i &= i - 1;
        }
        sum
    }

    /// Position `(chunk, index)` of the item at `rank`. `rank == len` maps to
    /// `(chunks.len(), 0)`.
    fn locate(&self, rank: usize) -> (usize, usize) {
        let n = self.chunks.len();
        let mut pos = 0;
        let mut rem = rank;
        let mut step = if n == 0 { 0 } else { 1 << n.ilog2() };
        while step > 0 {
            let next = pos + step;
            if next <= n && self.tree[next] <= rem {
                pos = next;
                rem -= self.tree[next];
            }
            step >>= 1;
        }
        (pos, rem)
    }

    /// Rank of the first item for which `before` is false. `before` must be
    /// true for a prefix of the order and false afterwards.
    fn partition_rank(&self, before: impl Fn(&T) -> bool) -> usize {
        let c = self
            .chunks
            .partition_point(|chunk| chunk.last().is_some_and(&before));
        if c == self.chunks.len() {
            return self.len;
        }
        self.prefix(c) + self.chunks[c].partition_point(before)
    }

    /// Merge the small chunk `chunk` into a neighbour when the result stays
    /// within `CHUNK_TARGET`. Rebuilds the tree and returns `true` if merged.
    fn try_merge(&mut self, chunk: usize) -> bool {
        let len = self.chunks[chunk].len();
        let next_fits =
            chunk + 1 < self.chunks.len() && len + self.chunks[chunk + 1].len() <= CHUNK_TARGET;
        let prev_fits = chunk > 0 && len + self.chunks[chunk - 1].len() <= CHUNK_TARGET;
        let (lo, hi) = if next_fits {
            (chunk, chunk + 1)
        } else if prev_fits {
            (chunk - 1, chunk)
        } else {
            return false;
        };
        let moved = self.chunks.remove(hi);
        self.chunks[lo].extend(moved);
        self.rebuild_tree();
        true
    }
}

impl<T: Ord> OrderedIndex<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.inner.len
    }

    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }

    /// Insert `item`. Returns `false` (and keeps the existing item) if an
    /// equal item is already present.
    pub fn insert(&mut self, item: T) -> bool {
        let inner = &mut *self.inner;
        if inner.chunks.is_empty() {
            inner.chunks.push(vec![item]);
            inner.len = 1;
            inner.rebuild_tree();
            return true;
        }
        let c = inner
            .chunks
            .partition_point(|chunk| chunk.last().is_some_and(|last| *last < item))
            .min(inner.chunks.len() - 1);
        let chunk = &mut inner.chunks[c];
        let idx = match chunk.binary_search(&item) {
            Ok(_) => return false,
            Err(idx) => idx,
        };
        chunk.insert(idx, item);
        inner.len += 1;
        if chunk.len() > CHUNK_SPLIT {
            let tail = chunk.split_off(chunk.len() / 2);
            inner.chunks.insert(c + 1, tail);
            inner.rebuild_tree();
        } else {
            inner.tree_add(c, 1);
        }
        true
    }

    /// Remove `item`. Returns whether it was present.
    pub fn remove(&mut self, item: &T) -> bool {
        let inner = &mut *self.inner;
        let c = inner
            .chunks
            .partition_point(|chunk| chunk.last().is_some_and(|last| last < item));
        if c == inner.chunks.len() {
            return false;
        }
        let Ok(idx) = inner.chunks[c].binary_search(item) else {
            return false;
        };
        inner.chunks[c].remove(idx);
        inner.len -= 1;
        if inner.chunks[c].is_empty() {
            inner.chunks.remove(c);
            inner.rebuild_tree();
        } else if inner.chunks[c].len() >= CHUNK_MIN || !inner.try_merge(c) {
            inner.tree_add(c, -1);
        }
        true
    }

    /// Zero-based rank of `item`, or `None` if absent.
    pub fn rank_of(&self, item: &T) -> Option<usize> {
        let rank = self.inner.partition_rank(|e| e < item);
        (self.get(rank)? == item).then_some(rank)
    }

    pub fn contains(&self, item: &T) -> bool {
        self.rank_of(item).is_some()
    }

    /// Item at zero-based `rank`.
    pub fn get(&self, rank: usize) -> Option<&T> {
        if rank >= self.inner.len {
            return None;
        }
        let (c, i) = self.inner.locate(rank);
        self.inner.chunks.get(c)?.get(i)
    }

    pub fn first(&self) -> Option<&T> {
        self.inner.chunks.first()?.first()
    }

    pub fn last(&self) -> Option<&T> {
        self.inner.chunks.last()?.last()
    }

    /// All items in ascending order.
    pub fn iter(&self) -> Iter<'_, T> {
        self.iter_ranks(0, self.inner.len)
    }

    /// Items with rank in `start..end` (clamped to the length).
    pub fn iter_ranks(&self, start: usize, end: usize) -> Iter<'_, T> {
        let end = end.min(self.inner.len);
        let start = start.min(end);
        Iter {
            chunks: &self.inner.chunks,
            front: self.inner.locate(start),
            back: self.inner.locate(end),
            remaining: end - start,
        }
    }

    /// Items with rank `start..`.
    pub fn iter_from_rank(&self, start: usize) -> Iter<'_, T> {
        self.iter_ranks(start, self.inner.len)
    }

    /// Items inside `bounds`. An inverted range is empty.
    pub fn range<R: RangeBounds<T>>(&self, bounds: R) -> Iter<'_, T> {
        let start = match bounds.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(b) => self.inner.partition_rank(|e| e < b),
            Bound::Excluded(b) => self.inner.partition_rank(|e| e <= b),
        };
        let end = match bounds.end_bound() {
            Bound::Unbounded => self.inner.len,
            Bound::Included(b) => self.inner.partition_rank(|e| e <= b),
            Bound::Excluded(b) => self.inner.partition_rank(|e| e < b),
        };
        self.iter_ranks(start, end)
    }

    /// Number of items strictly before `item` in the order, whether or not
    /// `item` is present.
    pub fn count_before(&self, item: &T) -> usize {
        self.inner.partition_rank(|e| e < item)
    }
}

impl<'a, T: Ord> IntoIterator for &'a OrderedIndex<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

/// Double-ended iterator over a contiguous rank range.
#[derive(Debug)]
pub struct Iter<'a, T> {
    chunks: &'a [Vec<T>],
    front: (usize, usize),
    /// Exclusive end position.
    back: (usize, usize),
    remaining: usize,
}

impl<T> Clone for Iter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            chunks: self.chunks,
            front: self.front,
            back: self.back,
            remaining: self.remaining,
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<&'a T> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let (c, i) = self.front;
        let chunk = &self.chunks[c];
        let item = &chunk[i];
        self.front = if i + 1 == chunk.len() {
            (c + 1, 0)
        } else {
            (c, i + 1)
        };
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl<'a, T> DoubleEndedIterator for Iter<'a, T> {
    fn next_back(&mut self) -> Option<&'a T> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let (mut c, mut i) = self.back;
        if i == 0 {
            c -= 1;
            i = self.chunks[c].len();
        }
        i -= 1;
        self.back = (c, i);
        Some(&self.chunks[c][i])
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    fn check(index: &OrderedIndex<(i64, u32)>, reference: &BTreeSet<(i64, u32)>) {
        assert_eq!(index.len(), reference.len());
        assert_eq!(index.is_empty(), reference.is_empty());
        assert!(index.iter().eq(reference.iter()));
        assert!(index.iter().rev().eq(reference.iter().rev()));
        assert_eq!(index.first(), reference.first());
        assert_eq!(index.last(), reference.last());
        for (rank, item) in reference.iter().enumerate() {
            assert_eq!(index.rank_of(item), Some(rank));
            assert_eq!(index.get(rank), Some(item));
        }
        assert_eq!(index.get(reference.len()), None);
    }

    #[test]
    fn empty_index() {
        let index = OrderedIndex::<u32>::new();
        assert_eq!(index.len(), 0);
        assert_eq!(index.first(), None);
        assert_eq!(index.get(0), None);
        assert_eq!(index.rank_of(&1), None);
        assert_eq!(index.iter().next(), None);
        assert_eq!(index.range(1..5).count(), 0);
    }

    #[test]
    fn duplicate_insert_is_rejected() {
        let mut index = OrderedIndex::new();
        assert!(index.insert(5));
        assert!(!index.insert(5));
        assert_eq!(index.len(), 1);
        assert!(!index.remove(&6));
        assert!(index.remove(&5));
        assert!(index.is_empty());
    }

    #[test]
    fn sequential_and_reverse_fill_split_chunks() {
        for reverse in [false, true] {
            let mut index = OrderedIndex::new();
            let mut reference = BTreeSet::new();
            for n in 0..6000u32 {
                let n = if reverse { 6000 - n } else { n };
                index.insert((i64::from(n), 0));
                reference.insert((i64::from(n), 0));
            }
            check(&index, &reference);
            for n in (0..6000u32).step_by(2) {
                let item = (i64::from(n), 0);
                assert_eq!(index.remove(&item), reference.remove(&item));
            }
            check(&index, &reference);
            // Shrink far enough to exercise merges and chunk drops.
            for n in 0..5900u32 {
                let item = (i64::from(n), 0);
                assert_eq!(index.remove(&item), reference.remove(&item));
            }
            check(&index, &reference);
        }
    }

    #[test]
    fn random_ops_match_btreeset() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut index = OrderedIndex::new();
        let mut reference = BTreeSet::new();
        for step in 0..20_000 {
            let item = ((rng.next() % 600) as i64 - 300, (rng.next() % 8) as u32);
            if rng.next() % 3 == 0 {
                assert_eq!(index.remove(&item), reference.remove(&item));
            } else {
                assert_eq!(index.insert(item), reference.insert(item));
            }
            if step % 2_000 == 0 {
                check(&index, &reference);
            }
        }
        check(&index, &reference);

        for _ in 0..500 {
            let a = ((rng.next() % 700) as i64 - 350, (rng.next() % 9) as u32);
            let b = ((rng.next() % 700) as i64 - 350, (rng.next() % 9) as u32);
            assert!(index.range(a..b).eq(if a <= b {
                reference.range(a..b)
            } else {
                reference.range(a..a)
            }));
            if a <= b {
                assert!(index.range(a..=b).eq(reference.range(a..=b)));
                assert!(index.range(a..=b).rev().eq(reference.range(a..=b).rev()));
                assert!(index.range(..=b).eq(reference.range(..=b)));
                assert!(index.range(a..).eq(reference.range(a..)));
            }
            assert!(
                index
                    .range((Bound::Excluded(a), Bound::Unbounded))
                    .eq(reference.range((Bound::Excluded(a), Bound::Unbounded)))
            );
            assert_eq!(index.count_before(&a), reference.range(..a).count());
        }

        let mid = reference.len() / 2;
        for start in [0, 1, 17, mid, reference.len(), reference.len() + 5] {
            assert!(index.iter_from_rank(start).eq(reference.iter().skip(start)));
            assert!(
                index
                    .iter_from_rank(start)
                    .rev()
                    .eq(reference.iter().skip(start).rev())
            );
        }

        let all: Vec<_> = reference.iter().copied().collect();
        for item in all {
            assert!(index.remove(&item));
        }
        assert!(index.is_empty());
        assert_eq!(index.iter().next(), None);
    }

    #[test]
    fn mixed_direction_iteration_meets_in_the_middle() {
        let mut index = OrderedIndex::new();
        for n in 0..3000u32 {
            index.insert(n);
        }
        let mut it = index.iter_ranks(10, 2990);
        let mut seen = 0;
        while let (Some(a), Some(b)) = (it.next(), it.next_back()) {
            assert!(a < b);
            seen += 2;
        }
        assert_eq!(seen, 2980);
        assert_eq!(it.next(), None);
        assert_eq!(it.next_back(), None);
    }
}
