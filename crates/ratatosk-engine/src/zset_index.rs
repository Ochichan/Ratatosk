//! Order-statistic B+ tree with O(log n) rank and by-rank access.
//!
//! Items live in sorted leaves of up to 32 entries. Internal nodes
//! hold, for each child, the child's largest item (`keys`), the number of
//! items below it, and its arena id. Leaves are chained in order, so
//! iteration walks them without touching the tree.
//!
//! Every leaf and node also keeps a dense `leads` array: the `u64` prefix of
//! each key (see [`OrderLead`]). Searches binary-search `leads` first, which
//! touches a few cache lines instead of one line per 40-byte key, and fall
//! back to full comparisons only inside a run of equal prefixes.
//!
//! Cost per operation, with `n` items (leaf size 32, node fan-out 128):
//!
//! - `insert`: one root-to-leaf descent (a binary search per level over the
//!   dense `leads` arrays), one memmove inside one leaf, one counter update
//!   per level. A full leaf shifts items into a sibling or splits; a full
//!   node splits. A split touches one node and its parent. O(log n).
//! - `remove`: the same descent and memmove. A leaf left under `LEAF_MIN`
//!   items merges with or borrows from an adjacent sibling, and a node left
//!   under `NODE_MIN` children does the same one level up, to the root,
//!   which collapses when it has one child. Each step touches at most two
//!   nodes per level, so a removal is amortized O(log n). One removal can pay
//!   O(peak / 16) extra when it frees the top of an arena: it pops the run of
//!   trailing dead slots, shrinks the arena and prunes the free list (under
//!   1 ms at 4M members).
//! - `rank_of` / `get`: one descent, summing child counts along the way.
//!   O(log n).
//! - `range` / `iter_ranks`: one or two descents, then O(1) per item.
//!
//! Every non-root node keeps at least `NODE_MIN` children, and every leaf
//! except the first and last keeps at least `LEAF_MIN` items. The first and
//! last leaf may be sparse because sequential fills pack the leaves behind
//! them. Vectors start small and grow on demand, so a one-member set is a
//! few hundred bytes. Freed arena slots drop their buffers at once, trailing
//! free slots are popped, and the arenas shrink when mostly empty. A freed
//! slot below a live one still costs its slot struct (56 or 72 bytes), its
//! share of the arena's rounded-up capacity and a free-list entry until it
//! is reused. That is typically 4 to 7 bytes per member of the peak size and
//! at most about 15 in an adversarial shape (a peak of leaves with 8 items
//! each, then removing all but the member in the highest-numbered leaf).
//!
//! The nodes sit in two arenas of `Vec`s addressed by `u32` ids, so the code
//! is safe Rust. The state is behind one `Box` to keep the index a single
//! pointer wide.

use std::ops::{Bound, RangeBounds};

/// Maximum items in a leaf. A full leaf splits in two before an insert.
const LEAF_MAX: usize = 32;
/// A leaf that shrinks below this merges with or borrows from a sibling.
const LEAF_MIN: usize = 8;
/// Siblings merge only when the result stays at or below this size.
const LEAF_MERGE_MAX: usize = 24;
/// A full leaf shifts items into its next sibling only if that sibling has
/// at least this many free slots.
const LEAF_SHIFT_SLACK: usize = 8;
/// Maximum children of an internal node.
const NODE_MAX: usize = 128;
/// A non-root node that drops below this many children is merged into or
/// rebalanced with a sibling.
const NODE_MIN: usize = 32;
/// Sibling nodes merge only when the result stays at or below this size.
const NODE_MERGE_MAX: usize = 96;
/// Deeper than any tree this index can build (32^10 items).
const MAX_DEPTH: usize = 10;
const NIL: u32 = u32::MAX;
/// Marks a freed leaf slot in `Leaf::prev` (a freed node has no children).
const DEAD: u32 = u32::MAX - 1;

/// Where an insert lands relative to the whole index.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Edge {
    Below,
    Above,
    Inside,
}

/// Items the index can order. `lead` is an order-preserving prefix:
/// `a <= b` must imply `a.lead() <= b.lead()`. Items with equal leads are
/// ordered by the full `Ord`.
pub trait OrderLead: Ord + Clone {
    fn lead(&self) -> u64;
}

#[derive(Debug, Clone)]
struct Leaf<T> {
    items: Vec<T>,
    /// `leads[i] == items[i].lead()`.
    leads: Vec<u64>,
    prev: u32,
    next: u32,
}

#[derive(Debug, Clone, Copy)]
struct Kid {
    /// Number of items below the child.
    count: usize,
    id: u32,
}

#[derive(Debug, Clone)]
struct Node<T> {
    /// `keys[i]` is the largest item below `kids[i]`.
    keys: Vec<T>,
    /// `leads[i] == keys[i].lead()`.
    leads: Vec<u64>,
    kids: Vec<Kid>,
}

#[derive(Debug, Clone)]
struct Inner<T> {
    leaves: Vec<Leaf<T>>,
    nodes: Vec<Node<T>>,
    free_leaves: Vec<u32>,
    free_nodes: Vec<u32>,
    /// A leaf id when `height == 0`, otherwise a node id. `NIL` when empty.
    root: u32,
    /// Number of internal levels above the leaves.
    height: usize,
    len: usize,
    head: u32,
    tail: u32,
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
                leaves: Vec::new(),
                nodes: Vec::new(),
                free_leaves: Vec::new(),
                free_nodes: Vec::new(),
                root: NIL,
                height: 0,
                len: 0,
                head: NIL,
                tail: NIL,
            }),
        }
    }
}

/// Make room for `new_len` entries without letting `Vec` double past `max`:
/// capacity grows 4, 8, 16, ... up to `max`, so small sets stay small and a
/// full node is never larger than `max` entries.
fn grow_to<U>(v: &mut Vec<U>, new_len: usize, max: usize) {
    if new_len > v.capacity() {
        let target = (v.capacity() * 2).clamp(4, max).max(new_len);
        v.reserve_exact(target - v.len());
    }
}

/// Return arena memory once the arena holds a quarter of its capacity.
fn shrink_arena<U>(arena: &mut Vec<U>) {
    if arena.capacity() > 4 * arena.len() + 64 {
        arena.shrink_to(arena.len() * 2 + 16);
    }
}

/// Index of the first entry that is not before `target`, or of the first
/// entry after `target` when `after` is set. `leads` mirrors `items`.
fn cut<T: OrderLead>(items: &[T], leads: &[u64], target: &T, lead: u64, after: bool) -> usize {
    let lo = leads.partition_point(|&l| l < lead);
    if lo == leads.len() || leads[lo] != lead {
        return lo;
    }
    let run = leads[lo..].partition_point(|&l| l == lead);
    lo + items[lo..lo + run].partition_point(|e| if after { e <= target } else { e < target })
}

/// Position of `target` among `items`, or where it would be inserted.
fn find<T: OrderLead>(items: &[T], leads: &[u64], target: &T, lead: u64) -> Result<usize, usize> {
    let lo = cut(items, leads, target, lead, false);
    // Comparing leads first spares a cold read of `items` for absent targets.
    if lo < items.len() && leads[lo] == lead && items[lo] == *target {
        Ok(lo)
    } else {
        Err(lo)
    }
}

impl<T: OrderLead> Inner<T> {
    fn leaf(&self, id: u32) -> &Leaf<T> {
        &self.leaves[id as usize]
    }

    fn node(&self, id: u32) -> &Node<T> {
        &self.nodes[id as usize]
    }

    fn alloc_leaf(&mut self, items: Vec<T>, leads: Vec<u64>, prev: u32, next: u32) -> u32 {
        let leaf = Leaf {
            items,
            leads,
            prev,
            next,
        };
        // Free lists may hold stale ids (a slot truncated, or reused through
        // a duplicate entry); only a slot that is still dead can be reused.
        while let Some(id) = self.free_leaves.pop() {
            if self
                .leaves
                .get(id as usize)
                .is_some_and(|slot| slot.prev == DEAD)
            {
                self.leaves[id as usize] = leaf;
                return id;
            }
        }
        self.leaves.push(leaf);
        (self.leaves.len() - 1) as u32
    }

    fn alloc_node(&mut self, node: Node<T>) -> u32 {
        while let Some(id) = self.free_nodes.pop() {
            if self
                .nodes
                .get(id as usize)
                .is_some_and(|slot| slot.kids.is_empty())
            {
                self.nodes[id as usize] = node;
                return id;
            }
        }
        self.nodes.push(node);
        (self.nodes.len() - 1) as u32
    }

    /// Free a leaf slot: its buffers are dropped at once, so a dead slot
    /// costs only the slot struct. Trailing dead slots are popped and the
    /// arena shrinks once it is mostly empty.
    fn free_leaf(&mut self, id: u32) {
        self.leaves[id as usize] = Leaf {
            items: Vec::new(),
            leads: Vec::new(),
            prev: DEAD,
            next: NIL,
        };
        if id as usize + 1 == self.leaves.len() {
            while self.leaves.last().is_some_and(|slot| slot.prev == DEAD) {
                self.leaves.pop();
            }
        } else {
            self.free_leaves.push(id);
        }
        shrink_arena(&mut self.leaves);
        if self.free_leaves.len() > 2 * self.leaves.len() + 64 {
            let leaves = &self.leaves;
            self.free_leaves.retain(|&id| {
                leaves
                    .get(id as usize)
                    .is_some_and(|slot| slot.prev == DEAD)
            });
            self.free_leaves.shrink_to_fit();
        }
    }

    fn free_node(&mut self, id: u32) {
        self.nodes[id as usize] = Node {
            keys: Vec::new(),
            leads: Vec::new(),
            kids: Vec::new(),
        };
        if id as usize + 1 == self.nodes.len() {
            while self.nodes.last().is_some_and(|slot| slot.kids.is_empty()) {
                self.nodes.pop();
            }
        } else {
            self.free_nodes.push(id);
        }
        shrink_arena(&mut self.nodes);
        if self.free_nodes.len() > 2 * self.nodes.len() + 64 {
            let nodes = &self.nodes;
            self.free_nodes.retain(|&id| {
                nodes
                    .get(id as usize)
                    .is_some_and(|slot| slot.kids.is_empty())
            });
            self.free_nodes.shrink_to_fit();
        }
    }

    fn first_item(&self) -> Option<&T> {
        self.leaves.get(self.head as usize)?.items.first()
    }

    fn last_item(&self) -> Option<&T> {
        self.leaves.get(self.tail as usize)?.items.last()
    }

    /// Position `(leaf, index)` of the item at `rank`, which must be `< len`.
    fn locate(&self, rank: usize) -> (u32, usize) {
        let mut cur = self.root;
        let mut rem = rank;
        for _ in 0..self.height {
            let node = self.node(cur);
            let mut ci = 0;
            while rem >= node.kids[ci].count {
                rem -= node.kids[ci].count;
                ci += 1;
            }
            cur = node.kids[ci].id;
        }
        (cur, rem)
    }

    /// Number of items before `target`, or not after it when `after` is set.
    fn cut_rank(&self, target: &T, after: bool) -> usize {
        if self.root == NIL {
            return 0;
        }
        let lead = target.lead();
        let mut cur = self.root;
        let mut rank = 0;
        for _ in 0..self.height {
            let node = self.node(cur);
            let ci = cut(&node.keys, &node.leads, target, lead, after).min(node.keys.len() - 1);
            rank += node.kids[..ci].iter().map(|k| k.count).sum::<usize>();
            cur = node.kids[ci].id;
        }
        let leaf = self.leaf(cur);
        rank + cut(&leaf.items, &leaf.leads, target, lead, after)
    }

    fn is_full(&self, id: u32, is_leaf: bool) -> bool {
        if is_leaf {
            self.leaf(id).items.len() >= LEAF_MAX
        } else {
            self.node(id).kids.len() >= NODE_MAX
        }
    }

    /// Split the full leaf `id`. An insert beyond the whole index's maximum
    /// keeps the old leaf nearly full and a prepend gives nearly all items to
    /// the new leaf, so sequential fills pack leaves instead of leaving them
    /// half empty. Every other insert splits evenly, including one that
    /// lands in a gap between leaves.
    fn split_leaf(&mut self, id: u32, edge: Edge) -> u32 {
        let len = self.leaf(id).items.len();
        let mid = match edge {
            Edge::Above => len - 1,
            Edge::Below => 1,
            Edge::Inside => len / 2,
        };
        let leaf = &mut self.leaves[id as usize];
        // In a small set, memory per member matters more than the realloc a
        // growing leaf would cost, so halves are sized to fit. Otherwise a
        // sequential fill goes on appending to the new leaf and gets full
        // capacity up front, and other halves grow on demand.
        let small = self.len <= 4 * LEAF_MAX;
        let (mut items, mut leads) = if edge == Edge::Above && !small {
            (Vec::with_capacity(LEAF_MAX), Vec::with_capacity(LEAF_MAX))
        } else {
            (Vec::new(), Vec::new())
        };
        items.extend(leaf.items.drain(mid..));
        leads.extend(leaf.leads.drain(mid..));
        if small {
            leaf.items.shrink_to_fit();
            leaf.leads.shrink_to_fit();
        }
        let next = leaf.next;
        let new = self.alloc_leaf(items, leads, id, next);
        self.leaves[id as usize].next = new;
        if next == NIL {
            self.tail = new;
        } else {
            self.leaves[next as usize].prev = new;
        }
        new
    }

    fn split_node(&mut self, id: u32) -> u32 {
        let mid = self.node(id).kids.len() / 2;
        let node = &mut self.nodes[id as usize];
        let mut upper = Node {
            keys: Vec::new(),
            leads: Vec::new(),
            kids: Vec::new(),
        };
        upper.keys.extend(node.keys.drain(mid..));
        upper.leads.extend(node.leads.drain(mid..));
        upper.kids.extend(node.kids.drain(mid..));
        self.alloc_node(upper)
    }

    /// Make room in the full leaf child `ci` of `parent` by moving its upper
    /// items into the next sibling, when that sibling has slack and the
    /// insert is not a sequential append or prepend (those split instead,
    /// which packs sequential fills). Returns whether it shifted. Sharing
    /// load this way raises the fill of randomly built leaves, which saves
    /// memory.
    fn try_shift_right(&mut self, parent: u32, ci: usize, edge: Edge) -> bool {
        let node = self.node(parent);
        if ci + 1 >= node.kids.len() {
            return false;
        }
        let (src, dst) = (node.kids[ci].id, node.kids[ci + 1].id);
        let items = &self.leaf(src).items;
        let dst_len = self.leaf(dst).items.len();
        if edge != Edge::Inside || dst_len + LEAF_SHIFT_SLACK > LEAF_MAX {
            return false;
        }
        let moved = (items.len() - dst_len) / 2;
        let at = items.len() - moved;
        let src_leaf = &mut self.leaves[src as usize];
        let moved_items = src_leaf.items.split_off(at);
        let moved_leads = src_leaf.leads.split_off(at);
        let new_max = src_leaf.items.last().cloned().expect("shift keeps items");
        let dst_leaf = &mut self.leaves[dst as usize];
        let new_len = dst_leaf.items.len() + moved_items.len();
        grow_to(&mut dst_leaf.items, new_len, LEAF_MAX);
        grow_to(&mut dst_leaf.leads, new_len, LEAF_MAX);
        dst_leaf.items.splice(0..0, moved_items);
        dst_leaf.leads.splice(0..0, moved_leads);
        let node = &mut self.nodes[parent as usize];
        node.leads[ci] = new_max.lead();
        node.keys[ci] = new_max;
        node.kids[ci].count -= moved;
        node.kids[ci + 1].count += moved;
        true
    }

    /// Split the full child `ci` of `parent` and record both halves.
    fn split_child(&mut self, parent: u32, ci: usize, child_is_leaf: bool, edge: Edge) {
        let child = self.node(parent).kids[ci].id;
        let (new_id, lk, lc, rk, rc) = if child_is_leaf {
            let new = self.split_leaf(child, edge);
            let l = &self.leaf(child).items;
            let r = &self.leaf(new).items;
            (
                new,
                l.last().cloned().expect("split halves are non-empty"),
                l.len(),
                r.last().cloned().expect("split halves are non-empty"),
                r.len(),
            )
        } else {
            let new = self.split_node(child);
            let l = self.node(child);
            let r = self.node(new);
            (
                new,
                l.keys.last().cloned().expect("split halves are non-empty"),
                l.kids.iter().map(|k| k.count).sum(),
                r.keys.last().cloned().expect("split halves are non-empty"),
                r.kids.iter().map(|k| k.count).sum(),
            )
        };
        let (ll, rl) = (lk.lead(), rk.lead());
        let p = &mut self.nodes[parent as usize];
        p.keys[ci] = lk;
        p.leads[ci] = ll;
        p.kids[ci].count = lc;
        let new_len = p.keys.len() + 1;
        grow_to(&mut p.keys, new_len, NODE_MAX);
        grow_to(&mut p.leads, new_len, NODE_MAX);
        grow_to(&mut p.kids, new_len, NODE_MAX);
        p.keys.insert(ci + 1, rk);
        p.leads.insert(ci + 1, rl);
        p.kids.insert(
            ci + 1,
            Kid {
                count: rc,
                id: new_id,
            },
        );
    }

    fn split_root(&mut self, edge: Edge) {
        let root_is_leaf = self.height == 0;
        let max = if root_is_leaf {
            self.leaf(self.root).items.last().cloned()
        } else {
            self.node(self.root).keys.last().cloned()
        }
        .expect("a full root is non-empty");
        let mut node = Node {
            keys: Vec::new(),
            leads: Vec::new(),
            kids: Vec::new(),
        };
        node.leads.push(max.lead());
        node.keys.push(max);
        node.kids.push(Kid {
            count: self.len,
            id: self.root,
        });
        let new_root = self.alloc_node(node);
        self.root = new_root;
        self.height += 1;
        self.split_child(new_root, 0, root_is_leaf, edge);
    }

    /// Record `key` as the maximum of the subtree at `path[upto]` and of
    /// every ancestor for which that subtree is the last child.
    fn fix_max(&mut self, path: &[(u32, usize)], upto: usize, key: &T) {
        let lead = key.lead();
        for &(nid, ci) in path[..=upto].iter().rev() {
            let node = &mut self.nodes[nid as usize];
            node.keys[ci] = key.clone();
            node.leads[ci] = lead;
            if ci + 1 != node.keys.len() {
                break;
            }
        }
    }

    fn collapse_root(&mut self) {
        while self.height > 0 && self.node(self.root).kids.len() == 1 {
            let old = self.root;
            self.root = self.node(old).kids[0].id;
            self.free_node(old);
            self.height -= 1;
        }
    }

    /// Rebalance the leaf that is child `path[lvl].1` of node `path[lvl].0`
    /// after a removal left it below `LEAF_MIN` items (possibly empty). It
    /// merges with an adjacent sibling when the result fits `LEAF_MERGE_MAX`,
    /// and otherwise evens the two out. Every non-root node has at least two
    /// children, so a sibling always exists.
    fn rebalance_leaf(&mut self, path: &[(u32, usize)], lvl: usize) {
        let (parent, ci) = path[lvl];
        let n = self.node(parent).kids.len();
        let left = if ci + 1 < n { ci } else { ci - 1 };
        let node = self.node(parent);
        let (l, r) = (node.kids[left].id, node.kids[left + 1].id);
        let (ll, rl) = (self.leaf(l).items.len(), self.leaf(r).items.len());
        if ll + rl <= LEAF_MERGE_MAX {
            let items = std::mem::take(&mut self.leaves[r as usize].items);
            let leads = std::mem::take(&mut self.leaves[r as usize].leads);
            let next = self.leaf(r).next;
            let target = &mut self.leaves[l as usize];
            grow_to(&mut target.items, ll + rl, LEAF_MAX);
            grow_to(&mut target.leads, ll + rl, LEAF_MAX);
            target.items.extend(items);
            target.leads.extend(leads);
            target.next = next;
            let max = target.items.last().cloned().expect("a merge keeps items");
            if next == NIL {
                self.tail = l;
            } else {
                self.leaves[next as usize].prev = l;
            }
            self.free_leaf(r);
            let node = &mut self.nodes[parent as usize];
            node.keys.remove(left + 1);
            node.leads.remove(left + 1);
            let merged = node.kids.remove(left + 1);
            node.leads[left] = max.lead();
            node.keys[left] = max;
            node.kids[left].count += merged.count;
        } else {
            let total = ll + rl;
            let new_left = total / 2;
            if ll < new_left {
                let k = new_left - ll;
                let src = &mut self.leaves[r as usize];
                let items: Vec<T> = src.items.drain(..k).collect();
                let leads: Vec<u64> = src.leads.drain(..k).collect();
                let dst = &mut self.leaves[l as usize];
                grow_to(&mut dst.items, new_left, LEAF_MAX);
                grow_to(&mut dst.leads, new_left, LEAF_MAX);
                dst.items.extend(items);
                dst.leads.extend(leads);
            } else {
                let k = ll - new_left;
                let src = &mut self.leaves[l as usize];
                let items = src.items.split_off(new_left);
                let leads = src.leads.split_off(new_left);
                let dst = &mut self.leaves[r as usize];
                grow_to(&mut dst.items, rl + k, LEAF_MAX);
                grow_to(&mut dst.leads, rl + k, LEAF_MAX);
                dst.items.splice(0..0, items);
                dst.leads.splice(0..0, leads);
            }
            let lmax = self.leaf(l).items.last().cloned().expect("balanced");
            let rmax = self.leaf(r).items.last().cloned().expect("balanced");
            let node = &mut self.nodes[parent as usize];
            node.leads[left] = lmax.lead();
            node.keys[left] = lmax;
            node.leads[left + 1] = rmax.lead();
            node.keys[left + 1] = rmax;
            node.kids[left].count = new_left;
            node.kids[left + 1].count = total - new_left;
        }
    }

    /// Rebalance the internal node that is child `path[lvl].1` of
    /// `path[lvl].0` after it fell below `NODE_MIN` children: merge with a
    /// sibling when the result fits `NODE_MERGE_MAX`, otherwise even out.
    fn rebalance_node(&mut self, path: &[(u32, usize)], lvl: usize) {
        let (parent, ci) = path[lvl];
        let n = self.node(parent).kids.len();
        let left = if ci + 1 < n { ci } else { ci - 1 };
        let (l, r) = {
            let node = self.node(parent);
            (node.kids[left].id, node.kids[left + 1].id)
        };
        let (ll, rl) = (self.node(l).kids.len(), self.node(r).kids.len());
        if ll + rl <= NODE_MERGE_MAX {
            let right = &mut self.nodes[r as usize];
            let keys = std::mem::take(&mut right.keys);
            let leads = std::mem::take(&mut right.leads);
            let kids = std::mem::take(&mut right.kids);
            let target = &mut self.nodes[l as usize];
            grow_to(&mut target.keys, ll + rl, NODE_MAX);
            grow_to(&mut target.leads, ll + rl, NODE_MAX);
            grow_to(&mut target.kids, ll + rl, NODE_MAX);
            target.keys.extend(keys);
            target.leads.extend(leads);
            target.kids.extend(kids);
            let max = target.keys.last().cloned().expect("a merge keeps kids");
            self.free_node(r);
            let node = &mut self.nodes[parent as usize];
            node.keys.remove(left + 1);
            node.leads.remove(left + 1);
            let merged = node.kids.remove(left + 1);
            node.leads[left] = max.lead();
            node.keys[left] = max;
            node.kids[left].count += merged.count;
        } else {
            let total = ll + rl;
            let new_left = total / 2;
            if ll < new_left {
                let k = new_left - ll;
                let src = &mut self.nodes[r as usize];
                let keys: Vec<T> = src.keys.drain(..k).collect();
                let leads: Vec<u64> = src.leads.drain(..k).collect();
                let kids: Vec<Kid> = src.kids.drain(..k).collect();
                let dst = &mut self.nodes[l as usize];
                grow_to(&mut dst.keys, new_left, NODE_MAX);
                grow_to(&mut dst.leads, new_left, NODE_MAX);
                grow_to(&mut dst.kids, new_left, NODE_MAX);
                dst.keys.extend(keys);
                dst.leads.extend(leads);
                dst.kids.extend(kids);
            } else {
                let k = ll - new_left;
                let src = &mut self.nodes[l as usize];
                let keys = src.keys.split_off(new_left);
                let leads = src.leads.split_off(new_left);
                let kids = src.kids.split_off(new_left);
                let dst = &mut self.nodes[r as usize];
                grow_to(&mut dst.keys, rl + k, NODE_MAX);
                grow_to(&mut dst.leads, rl + k, NODE_MAX);
                grow_to(&mut dst.kids, rl + k, NODE_MAX);
                dst.keys.splice(0..0, keys);
                dst.leads.splice(0..0, leads);
                dst.kids.splice(0..0, kids);
            }
            let left_count: usize = self.node(l).kids.iter().map(|k| k.count).sum();
            let right_count: usize = self.node(r).kids.iter().map(|k| k.count).sum();
            let lmax = self.node(l).keys.last().cloned().expect("balanced");
            let rmax = self.node(r).keys.last().cloned().expect("balanced");
            let node = &mut self.nodes[parent as usize];
            node.leads[left] = lmax.lead();
            node.keys[left] = lmax;
            node.leads[left + 1] = rmax.lead();
            node.keys[left + 1] = rmax;
            node.kids[left].count = left_count;
            node.kids[left + 1].count = right_count;
        }
    }
}

impl<T: OrderLead> OrderedIndex<T> {
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
        let lead = item.lead();
        if inner.root == NIL {
            let mut items = Vec::new();
            let mut leads = Vec::new();
            items.push(item);
            leads.push(lead);
            let id = inner.alloc_leaf(items, leads, NIL, NIL);
            inner.root = id;
            inner.head = id;
            inner.tail = id;
            inner.len = 1;
            return true;
        }
        // Sequential fills (monotonic scores) insert below the minimum or above
        // the maximum; those skip every search on the way down.
        let below_all = inner.first_item().is_some_and(|first| item < *first);
        let above_all = !below_all && inner.last_item().is_some_and(|last| item > *last);
        let edge = if below_all {
            Edge::Below
        } else if above_all {
            Edge::Above
        } else {
            Edge::Inside
        };
        if inner.is_full(inner.root, inner.height == 0) {
            inner.split_root(edge);
        }

        // Descend, splitting any full child first so its parent always has
        // room for the new sibling.
        let mut path = [(NIL, 0usize); MAX_DEPTH];
        let mut cur = inner.root;
        let height = inner.height;
        for (lvl, step) in path.iter_mut().enumerate().take(height) {
            let node = inner.node(cur);
            let n = node.keys.len();
            let mut ci = if below_all {
                0
            } else if above_all {
                n - 1
            } else {
                cut(&node.keys, &node.leads, &item, lead, false).min(n - 1)
            };
            let child_is_leaf = lvl + 1 == inner.height;
            if inner.is_full(node.kids[ci].id, child_is_leaf) {
                if !(child_is_leaf && inner.try_shift_right(cur, ci, edge)) {
                    inner.split_child(cur, ci, child_is_leaf, edge);
                }
                if inner.node(cur).keys[ci] < item {
                    ci += 1;
                }
            }
            *step = (cur, ci);
            cur = inner.node(cur).kids[ci].id;
        }

        let leaf = &mut inner.leaves[cur as usize];
        let idx = if below_all {
            0
        } else if above_all {
            leaf.items.len()
        } else {
            match find(&leaf.items, &leaf.leads, &item, lead) {
                Ok(_) => return false,
                Err(idx) => idx,
            }
        };
        let appended = idx == leaf.items.len();
        if appended && inner.height > 0 {
            inner.fix_max(&path, inner.height - 1, &item);
        }
        let leaf = &mut inner.leaves[cur as usize];
        let new_len = leaf.items.len() + 1;
        grow_to(&mut leaf.items, new_len, LEAF_MAX);
        grow_to(&mut leaf.leads, new_len, LEAF_MAX);
        leaf.items.insert(idx, item);
        leaf.leads.insert(idx, lead);
        for &(nid, ci) in &path[..inner.height] {
            inner.nodes[nid as usize].kids[ci].count += 1;
        }
        inner.len += 1;
        true
    }

    /// Remove `item`. Returns whether it was present.
    pub fn remove(&mut self, item: &T) -> bool {
        let inner = &mut *self.inner;
        if inner.root == NIL {
            return false;
        }
        let lead = item.lead();
        let mut path = [(NIL, 0usize); MAX_DEPTH];
        let mut cur = inner.root;
        for step in path.iter_mut().take(inner.height) {
            let node = inner.node(cur);
            let ci = cut(&node.keys, &node.leads, item, lead, false);
            if ci == node.keys.len() {
                return false;
            }
            *step = (cur, ci);
            cur = node.kids[ci].id;
        }
        let leaf = &mut inner.leaves[cur as usize];
        let Ok(idx) = find(&leaf.items, &leaf.leads, item, lead) else {
            return false;
        };
        leaf.items.remove(idx);
        leaf.leads.remove(idx);
        inner.len -= 1;
        let height = inner.height;
        for &(nid, ci) in &path[..height] {
            inner.nodes[nid as usize].kids[ci].count -= 1;
        }
        let leaf_len = inner.leaf(cur).items.len();
        if height == 0 {
            if leaf_len == 0 {
                inner.free_leaf(cur);
                inner.root = NIL;
                inner.head = NIL;
                inner.tail = NIL;
            }
            return true;
        }
        // Every leaf of a tree with internal nodes holds at least two items
        // (sparse end leaves start with two and rebalance below `LEAF_MIN`),
        // so a removal never empties one.
        debug_assert!(leaf_len > 0, "a removal emptied a leaf");
        if idx == leaf_len && leaf_len > 0 {
            let key = inner.leaf(cur).items.last().cloned();
            if let Some(key) = key {
                inner.fix_max(&path, height - 1, &key);
            }
        }
        if leaf_len < LEAF_MIN {
            // Fix the leaf, then each ancestor that lost children, bottom up.
            let mut lvl = height - 1;
            inner.rebalance_leaf(&path, lvl);
            while lvl > 0 && inner.node(path[lvl].0).kids.len() < NODE_MIN {
                inner.rebalance_node(&path, lvl - 1);
                lvl -= 1;
            }
            inner.collapse_root();
        }
        true
    }

    /// Zero-based rank of `item`, or `None` if absent.
    pub fn rank_of(&self, item: &T) -> Option<usize> {
        let inner = &*self.inner;
        if inner.root == NIL {
            return None;
        }
        let lead = item.lead();
        let mut cur = inner.root;
        let mut rank = 0;
        for _ in 0..inner.height {
            let node = inner.node(cur);
            let ci = cut(&node.keys, &node.leads, item, lead, false);
            if ci == node.keys.len() {
                return None;
            }
            rank += node.kids[..ci].iter().map(|k| k.count).sum::<usize>();
            cur = node.kids[ci].id;
        }
        let leaf = inner.leaf(cur);
        let idx = find(&leaf.items, &leaf.leads, item, lead).ok()?;
        Some(rank + idx)
    }

    pub fn contains(&self, item: &T) -> bool {
        self.rank_of(item).is_some()
    }

    /// Item at zero-based `rank`.
    pub fn get(&self, rank: usize) -> Option<&T> {
        if rank >= self.inner.len {
            return None;
        }
        let (leaf, idx) = self.inner.locate(rank);
        self.inner.leaf(leaf).items.get(idx)
    }

    pub fn first(&self) -> Option<&T> {
        self.inner.first_item()
    }

    pub fn last(&self) -> Option<&T> {
        self.inner.last_item()
    }

    /// All items in ascending order.
    pub fn iter(&self) -> Iter<'_, T> {
        self.iter_ranks(0, self.inner.len)
    }

    /// Items with rank in `start..end` (clamped to the length).
    pub fn iter_ranks(&self, start: usize, end: usize) -> Iter<'_, T> {
        let end = end.min(self.inner.len);
        let start = start.min(end);
        if start == end {
            return Iter {
                leaves: &self.inner.leaves,
                front: (NIL, 0),
                back: (NIL, 0),
                remaining: 0,
            };
        }
        Iter {
            leaves: &self.inner.leaves,
            front: self.inner.locate(start),
            back: self.inner.locate(end - 1),
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
            Bound::Included(b) => self.inner.cut_rank(b, false),
            Bound::Excluded(b) => self.inner.cut_rank(b, true),
        };
        let end = match bounds.end_bound() {
            Bound::Unbounded => self.inner.len,
            Bound::Included(b) => self.inner.cut_rank(b, true),
            Bound::Excluded(b) => self.inner.cut_rank(b, false),
        };
        self.iter_ranks(start, end)
    }

    /// Number of items strictly before `item` in the order, whether or not
    /// `item` is present.
    pub fn count_before(&self, item: &T) -> usize {
        self.inner.cut_rank(item, false)
    }

    /// Heap bytes held by the arenas and every node vector, by capacity.
    /// Test-only.
    #[cfg(test)]
    pub(crate) fn heap_bytes(&self) -> usize {
        use std::mem::size_of;
        let i = &*self.inner;
        let leaves: usize = i
            .leaves
            .iter()
            .map(|l| l.items.capacity() * size_of::<T>() + l.leads.capacity() * 8)
            .sum();
        let nodes: usize = i
            .nodes
            .iter()
            .map(|n| {
                n.keys.capacity() * size_of::<T>()
                    + n.leads.capacity() * 8
                    + n.kids.capacity() * size_of::<Kid>()
            })
            .sum();
        size_of::<Inner<T>>()
            + i.leaves.capacity() * size_of::<Leaf<T>>()
            + i.nodes.capacity() * size_of::<Node<T>>()
            + (i.free_leaves.capacity() + i.free_nodes.capacity()) * 4
            + leaves
            + nodes
    }

    /// Check every structural invariant. Test-only.
    #[cfg(test)]
    pub(crate) fn assert_invariants(&self) {
        let inner = &*self.inner;
        if inner.root == NIL {
            assert_eq!(inner.len, 0);
            assert_eq!(inner.head, NIL);
            assert_eq!(inner.tail, NIL);
            assert!(
                inner.leaves.is_empty(),
                "an empty index holds no leaf slots"
            );
            assert!(inner.nodes.is_empty(), "an empty index holds no node slots");
            return;
        }
        let mut census = Census::default();
        let (count, _) = Self::check_subtree(inner, inner.root, inner.height, &mut census);
        assert_eq!(count, inner.len);
        let order = census.order;
        let dead_leaves = inner.leaves.iter().filter(|l| l.prev == DEAD).count();
        let dead_nodes = inner.nodes.iter().filter(|n| n.kids.is_empty()).count();
        assert_eq!(census.leaves + dead_leaves, inner.leaves.len());
        assert_eq!(census.nodes + dead_nodes, inner.nodes.len());
        // Trailing dead slots are popped, and dead slots hold no buffers.
        assert!(inner.leaves.last().is_some_and(|l| l.prev != DEAD));
        assert!(inner.nodes.is_empty() || inner.nodes.last().is_some_and(|n| !n.kids.is_empty()));
        for leaf in inner.leaves.iter().filter(|l| l.prev == DEAD) {
            assert_eq!(leaf.items.capacity() + leaf.leads.capacity(), 0);
        }
        // Leaves reachable through the tree match the chain.
        assert_eq!(order.first().copied(), Some(inner.head));
        assert_eq!(order.last().copied(), Some(inner.tail));
        let mut prev_item: Option<&T> = None;
        for (pos, &id) in order.iter().enumerate() {
            let leaf = inner.leaf(id);
            let want_prev = if pos == 0 { NIL } else { order[pos - 1] };
            let want_next = order.get(pos + 1).copied().unwrap_or(NIL);
            assert_eq!(leaf.prev, want_prev);
            assert_eq!(leaf.next, want_next);
            for item in &leaf.items {
                if let Some(prev) = prev_item {
                    assert!(prev < item, "items must be strictly ascending");
                }
                prev_item = Some(item);
            }
        }
    }

    #[cfg(test)]
    fn check_subtree(inner: &Inner<T>, id: u32, level: usize, census: &mut Census) -> (usize, T) {
        if level == 0 {
            census.leaves += 1;
            census.order.push(id);
            let leaf = inner.leaf(id);
            assert!(!leaf.items.is_empty(), "leaves are never empty");
            assert!(leaf.items.len() <= LEAF_MAX);
            // Only the first and last leaf may be sparse (sequential fills
            // leave one there); deletions rebalance every other leaf.
            if id != inner.head && id != inner.tail {
                assert!(leaf.items.len() >= LEAF_MIN, "sparse inner leaf");
            }
            assert_eq!(leaf.leads.len(), leaf.items.len());
            for (item, &lead) in leaf.items.iter().zip(&leaf.leads) {
                assert_eq!(item.lead(), lead, "leaf lead mirrors its item");
            }
            return (leaf.items.len(), leaf.items.last().unwrap().clone());
        }
        census.nodes += 1;
        let node = inner.node(id);
        let n = node.kids.len();
        assert!(n > 0 && n <= NODE_MAX);
        if id == inner.root {
            assert!(n >= 2, "an internal root has at least two children");
        } else {
            assert!(n >= NODE_MIN, "underfull internal node");
        }
        assert_eq!(node.keys.len(), n);
        assert_eq!(node.leads.len(), n);
        let mut total = 0;
        let mut last = None;
        for i in 0..n {
            let (count, max) = Self::check_subtree(inner, node.kids[i].id, level - 1, census);
            assert_eq!(node.kids[i].count, count, "count of child {i}");
            assert!(node.keys[i] == max, "key of child {i} is its maximum");
            assert_eq!(node.leads[i], max.lead(), "node lead mirrors its key");
            total += count;
            last = Some(max);
        }
        (total, last.unwrap())
    }
}

/// Counts gathered while walking the tree in `assert_invariants`.
#[cfg(test)]
#[derive(Default)]
struct Census {
    leaves: usize,
    nodes: usize,
    /// Leaf ids in tree order.
    order: Vec<u32>,
}

impl<'a, T: OrderLead> IntoIterator for &'a OrderedIndex<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Iter<'a, T> {
        self.iter()
    }
}

/// Double-ended iterator over a contiguous rank range.
#[derive(Debug)]
pub struct Iter<'a, T> {
    leaves: &'a [Leaf<T>],
    /// Position of the next item from the front.
    front: (u32, usize),
    /// Position of the next item from the back.
    back: (u32, usize),
    remaining: usize,
}

impl<T> Clone for Iter<'_, T> {
    fn clone(&self) -> Self {
        Self {
            leaves: self.leaves,
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
        let (id, i) = self.front;
        let leaf = &self.leaves[id as usize];
        let item = &leaf.items[i];
        self.front = if i + 1 == leaf.items.len() {
            (leaf.next, 0)
        } else {
            (id, i + 1)
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
        let (id, i) = self.back;
        let leaf = &self.leaves[id as usize];
        let item = &leaf.items[i];
        self.back = if i == 0 {
            if leaf.prev == NIL {
                (NIL, 0)
            } else {
                (leaf.prev, self.leaves[leaf.prev as usize].items.len() - 1)
            }
        } else {
            (id, i - 1)
        };
        Some(item)
    }
}

impl<T> ExactSizeIterator for Iter<'_, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    impl OrderLead for u32 {
        fn lead(&self) -> u64 {
            u64::from(*self)
        }
    }

    impl OrderLead for (i64, u32) {
        fn lead(&self) -> u64 {
            (self.0 as u64) ^ (1 << 63)
        }
    }

    /// Every item has the same lead, so all searches take the tie path.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct Tied(u32);

    impl OrderLead for Tied {
        fn lead(&self) -> u64 {
            0
        }
    }

    /// Leads collide in runs of 16 consecutive values.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct Coarse(u32);

    impl OrderLead for Coarse {
        fn lead(&self) -> u64 {
            u64::from(self.0 / 16)
        }
    }

    fn churn_with_ties<T: OrderLead + std::fmt::Debug>(make: impl Fn(u32) -> T) {
        let mut rng = Rng(0xA5A5_1234_9999_0001);
        let mut index = OrderedIndex::new();
        let mut reference = BTreeSet::new();
        for step in 0..30_000 {
            let item = make((rng.next() % 3_000) as u32);
            if rng.next() % 3 == 0 {
                assert_eq!(index.remove(&item), reference.remove(&item));
            } else {
                assert_eq!(index.insert(item.clone()), reference.insert(item));
            }
            if step % 3_000 == 0 {
                index.assert_invariants();
                assert!(index.iter().eq(reference.iter()));
            }
        }
        index.assert_invariants();
        for (rank, item) in reference.iter().enumerate() {
            assert_eq!(index.rank_of(item), Some(rank));
            assert_eq!(index.get(rank), Some(item));
        }
        for probe in 0..3_100u32 {
            let item = make(probe);
            assert_eq!(index.count_before(&item), reference.range(..&item).count());
            assert_eq!(index.rank_of(&item).is_some(), reference.contains(&item));
            assert!(index.range(&item..).eq(reference.range(&item..)));
            assert!(index.range(..=&item).eq(reference.range(..=&item)));
        }
    }

    #[test]
    fn equal_leads_fall_back_to_full_comparison() {
        churn_with_ties(Tied);
        churn_with_ties(Coarse);
    }

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
        index.assert_invariants();
        assert_eq!(index.len(), reference.len());
        assert_eq!(index.is_empty(), reference.is_empty());
        assert!(index.iter().eq(reference.iter()));
        assert!(index.iter().rev().eq(reference.iter().rev()));
        assert_eq!(index.first(), reference.first());
        assert_eq!(index.last(), reference.last());
        for (rank, item) in reference.iter().enumerate() {
            assert_eq!(index.rank_of(item), Some(rank));
            assert_eq!(index.get(rank), Some(item));
            assert!(index.contains(item));
        }
        assert_eq!(index.get(reference.len()), None);
    }

    #[test]
    fn empty_index() {
        let index = OrderedIndex::<u32>::new();
        index.assert_invariants();
        assert_eq!(index.len(), 0);
        assert_eq!(index.first(), None);
        assert_eq!(index.last(), None);
        assert_eq!(index.get(0), None);
        assert_eq!(index.rank_of(&1), None);
        assert_eq!(index.iter().next(), None);
        assert_eq!(index.iter().next_back(), None);
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
        index.assert_invariants();
        // The structure is reusable after emptying.
        assert!(index.insert(7));
        assert_eq!(index.rank_of(&7), Some(0));
    }

    #[test]
    fn sequential_and_reverse_fill_grow_and_shrink() {
        for reverse in [false, true] {
            let mut index = OrderedIndex::new();
            let mut reference = BTreeSet::new();
            // Large enough for three internal levels over 128-way nodes
            // would need 64^3 items, so this covers two.
            for n in 0..30_000u32 {
                let n = if reverse { 30_000 - n } else { n };
                index.insert((i64::from(n), 0));
                reference.insert((i64::from(n), 0));
            }
            check(&index, &reference);
            for n in (0..30_000u32).step_by(2) {
                let item = (i64::from(n), 0);
                assert_eq!(index.remove(&item), reference.remove(&item));
            }
            check(&index, &reference);
            // Shrink far enough to exercise merges, empty leaves and root collapse.
            for n in 0..29_900u32 {
                let item = (i64::from(n), 0);
                assert_eq!(index.remove(&item), reference.remove(&item));
            }
            check(&index, &reference);
            for n in 29_900..=30_000u32 {
                let item = (i64::from(n), 0);
                assert_eq!(index.remove(&item), reference.remove(&item));
            }
            check(&index, &reference);
            assert!(index.is_empty());
        }
    }

    #[test]
    fn removing_from_the_back_and_middle_updates_maxima() {
        let mut index = OrderedIndex::new();
        let mut reference = BTreeSet::new();
        for n in 0..5_000u32 {
            index.insert((i64::from(n), 0));
            reference.insert((i64::from(n), 0));
        }
        for n in (0..5_000u32).rev().step_by(3) {
            let item = (i64::from(n), 0);
            assert_eq!(index.remove(&item), reference.remove(&item));
            if n % 300 == 0 {
                check(&index, &reference);
            }
        }
        check(&index, &reference);
    }

    #[test]
    fn random_ops_match_btreeset() {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut index = OrderedIndex::new();
        let mut reference = BTreeSet::new();
        for step in 0..60_000 {
            let item = ((rng.next() % 4_000) as i64 - 2_000, (rng.next() % 8) as u32);
            if rng.next() % 3 == 0 {
                assert_eq!(index.remove(&item), reference.remove(&item));
            } else {
                assert_eq!(index.insert(item), reference.insert(item));
            }
            if step % 5_000 == 0 {
                check(&index, &reference);
            }
        }
        check(&index, &reference);

        for _ in 0..500 {
            let a = ((rng.next() % 4_400) as i64 - 2_200, (rng.next() % 9) as u32);
            let b = ((rng.next() % 4_400) as i64 - 2_200, (rng.next() % 9) as u32);
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
            assert_eq!(index.rank_of(&a).is_some(), reference.contains(&a));
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
        for _ in 0..200 {
            let a = (rng.next() % (reference.len() as u64 + 3)) as usize;
            let b = (rng.next() % (reference.len() as u64 + 3)) as usize;
            let want: Vec<_> = reference.iter().skip(a).take(b.saturating_sub(a)).collect();
            let got: Vec<_> = index.iter_ranks(a, b).collect();
            assert_eq!(got, want);
        }

        let cloned = index.clone();
        check(&cloned, &reference);

        let all: Vec<_> = reference.iter().copied().collect();
        for (n, item) in all.into_iter().enumerate() {
            assert!(index.remove(&item));
            if n % 700 == 0 {
                index.assert_invariants();
            }
        }
        assert!(index.is_empty());
        index.assert_invariants();
        assert_eq!(index.iter().next(), None);
        // The clone is independent of the drained original.
        check(&cloned, &reference);
    }

    #[test]
    fn three_internal_levels_stay_consistent() {
        // A multiplicative permutation visits 0..N once each in scattered
        // order, so splits, shifts and merges happen at every level.
        const N: u32 = 700_001;
        let scatter = |i: u32| (u64::from(i) * 7_919 % u64::from(N)) as u32;
        let mut index = OrderedIndex::new();
        let mut reference = BTreeSet::new();
        for i in 0..N {
            let n = scatter(i);
            assert!(index.insert(n));
            reference.insert(n);
            if i % 150_000 == 0 {
                index.assert_invariants();
            }
        }
        assert!(index.inner.height >= 3, "height {}", index.inner.height);
        index.assert_invariants();
        assert_eq!(index.len(), reference.len());
        assert!(index.iter().eq(reference.iter()));
        for probe in (0..N).step_by(997) {
            assert_eq!(index.rank_of(&probe), Some(probe as usize));
            assert_eq!(index.get(probe as usize), Some(&probe));
        }
        // Remove two thirds in scattered order, then the rest from the front.
        for i in 0..N {
            if i % 3 != 0 {
                let n = scatter(i);
                assert!(index.remove(&n));
                reference.remove(&n);
            }
            if i % 200_000 == 0 {
                index.assert_invariants();
            }
        }
        index.assert_invariants();
        assert!(index.iter().eq(reference.iter()));
        assert!(index.iter().rev().eq(reference.iter().rev()));
        for (rank, n) in reference.iter().enumerate().step_by(101) {
            assert_eq!(index.rank_of(n), Some(rank));
        }
        while let Some(&first) = index.first() {
            assert!(index.remove(&first));
            if index.len() % 100_000 == 0 {
                index.assert_invariants();
            }
        }
        index.assert_invariants();
        assert_eq!(index.inner.height, 0);
    }

    /// Memory follows the current size: a few multiples of the bytes the items
    /// need, plus a small constant, plus up to 16 bytes per member of the peak
    /// size. The peak term is a regression bound for freed arena slots that
    /// cannot be popped (slot struct, rounded-up arena capacity and a
    /// free-list entry); typical shapes use 4 to 7.
    fn assert_memory_bounded(index: &OrderedIndex<(i64, u32)>, peak: usize, context: &str) {
        let per_item = std::mem::size_of::<(i64, u32)>() + 8;
        let bound = 6 * per_item * index.len() + 16 * peak + 4_096;
        assert!(
            index.heap_bytes() <= bound,
            "{context}: {} bytes for {} items (bound {bound})",
            index.heap_bytes(),
            index.len()
        );
    }

    fn pair(n: u32) -> (i64, u32) {
        (i64::from(n), 0)
    }

    #[test]
    fn small_sets_stay_small() {
        for count in [0u32, 1, 2, 4, 8, 33, 100, 128, 300] {
            let mut index = OrderedIndex::new();
            for n in 0..count {
                index.insert(pair(n * 3));
            }
            index.assert_invariants();
            assert_memory_bounded(&index, count as usize, &format!("{count} items"));
        }
        let mut one = OrderedIndex::new();
        one.insert(pair(1));
        assert!(one.heap_bytes() < 1_024, "{}", one.heap_bytes());
    }

    #[test]
    fn memory_after_scattered_mass_removal() {
        const N: u32 = 1_000_003;
        let scatter = |i: u32| (u64::from(i) * 7_919 % u64::from(N)) as u32;
        let mut index = OrderedIndex::new();
        for i in 0..N {
            index.insert(pair(scatter(i)));
        }
        assert_memory_bounded(&index, N as usize, "full");
        for i in 0..N {
            if i % 1_000 != 0 {
                assert!(index.remove(&pair(scatter(i))));
            }
        }
        index.assert_invariants();
        assert_eq!(index.len(), 1_001);
        assert_memory_bounded(&index, N as usize, "1M scattered down to 1k");
    }

    #[test]
    fn memory_after_sequential_fill_and_sparse_keep() {
        const N: u32 = 1_000_000;
        let mut index = OrderedIndex::new();
        for n in 0..N {
            index.insert(pair(n));
        }
        for n in 0..N {
            if n % 1_984 != 0 {
                assert!(index.remove(&pair(n)));
            }
            if n % 100_000 == 0 {
                assert_memory_bounded(&index, N as usize, "sequential shrink");
            }
        }
        index.assert_invariants();
        assert_memory_bounded(&index, N as usize, "sequential fill, keep every 1984th");
    }

    #[test]
    fn memory_across_repeated_fill_and_purge_rounds() {
        const ROUNDS: u32 = 40;
        const FILL: u32 = 200_000;
        let mut index = OrderedIndex::new();
        let mut peak = 0;
        for round in 0..ROUNDS {
            let base = round * FILL;
            let scatter = |i: u32| base + (u64::from(i) * 7_919 % u64::from(FILL)) as u32;
            for i in 0..FILL {
                index.insert(pair(scatter(i)));
            }
            for i in 0..FILL {
                let n = scatter(i);
                if n % 4_000 != 0 {
                    assert!(index.remove(&pair(n)));
                }
            }
            peak = peak.max(index.heap_bytes());
            assert_memory_bounded(
                &index,
                FILL as usize + index.len(),
                &format!("round {round}"),
            );
        }
        index.assert_invariants();
        assert_eq!(index.len(), (ROUNDS * FILL / 4_000) as usize);
        assert!(peak < 3_000_000, "peak {peak}");
    }

    fn assert_same(index: &OrderedIndex<(i64, u32)>, reference: &BTreeSet<(i64, u32)>) {
        index.assert_invariants();
        assert_eq!(index.len(), reference.len());
        assert!(index.iter().eq(reference.iter()));
        for (rank, item) in reference.iter().enumerate().step_by(7) {
            assert_eq!(index.rank_of(item), Some(rank));
            assert_eq!(index.get(rank), Some(item));
        }
    }

    /// Removal-heavy shapes that force leaf and node rebalancing at every
    /// level, checked against a reference with the full invariant checker.
    #[test]
    fn rebalancing_phases_match_reference() {
        let mut rng = Rng(0x5EED_0000_1234_5678);
        let mut index = OrderedIndex::new();
        let mut reference = BTreeSet::new();

        // Sawtooth: grow to 60k, shrink to 2k by removing random members.
        for _ in 0..4 {
            while reference.len() < 60_000 {
                let item = ((rng.next() % 400_000) as i64, (rng.next() % 4) as u32);
                assert_eq!(index.insert(item), reference.insert(item));
            }
            assert_same(&index, &reference);
            while reference.len() > 2_000 {
                let rank = (rng.next() % reference.len() as u64) as usize;
                let item = *index.get(rank).unwrap();
                assert!(index.remove(&item));
                assert!(reference.remove(&item));
                if reference.len() % 9_000 == 0 {
                    index.assert_invariants();
                }
            }
            assert_same(&index, &reference);
        }

        // Keep every k-th member of a sequential fill, removing in random order.
        for k in [2u32, 3, 7, 50, 1_984] {
            let mut index = OrderedIndex::new();
            let mut reference = BTreeSet::new();
            let mut doomed: Vec<u32> = (0..30_000).filter(|n| n % k != 0).collect();
            for n in 0..30_000u32 {
                index.insert(pair(n));
                reference.insert(pair(n));
            }
            for i in (1..doomed.len()).rev() {
                doomed.swap(i, (rng.next() % (i as u64 + 1)) as usize);
            }
            for (done, n) in doomed.into_iter().enumerate() {
                assert!(index.remove(&pair(n)));
                reference.remove(&pair(n));
                if done % 5_000 == 0 {
                    index.assert_invariants();
                }
            }
            assert_same(&index, &reference);
        }

        // Rank-window removal: repeatedly delete a window of 200 consecutive
        // ranks (the pattern of ZREMRANGEBYRANK), then refill a little.
        let mut index = OrderedIndex::new();
        let mut reference = BTreeSet::new();
        for n in 0..50_000u32 {
            index.insert(pair(n * 2));
            reference.insert(pair(n * 2));
        }
        let mut window = 0;
        while reference.len() > 400 {
            let start = (rng.next() % reference.len() as u64) as usize;
            for _ in 0..200.min(reference.len() - start) {
                let item = *index.get(start).unwrap();
                assert!(index.remove(&item));
                assert!(reference.remove(&item));
            }
            for _ in 0..20 {
                let item = pair((rng.next() % 200_000) as u32 * 2 + 1);
                assert_eq!(index.insert(item), reference.insert(item));
            }
            window += 1;
            if window % 40 == 0 {
                index.assert_invariants();
            }
        }
        assert_same(&index, &reference);

        // Drain from the front and from the back, as ZPOPMIN and ZPOPMAX do.
        for from_front in [true, false] {
            let mut index = OrderedIndex::new();
            for n in 0..40_000u32 {
                index.insert(pair(n));
            }
            let mut step = 0;
            while let Some(&item) = if from_front {
                index.first()
            } else {
                index.last()
            } {
                assert!(index.remove(&item));
                step += 1;
                if step % 8_000 == 0 {
                    index.assert_invariants();
                }
            }
            index.assert_invariants();
        }
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
