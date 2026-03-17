use std::sync::LazyLock;

use bytes::Bytes;
use hashbrown::HashMap as HashBrownMap;

use super::{COMMAND_SPECS, CommandSpec, EXTRA_COMMAND_SPECS, to_uppercase_stack};

pub(super) fn all_command_specs() -> impl Iterator<Item = CommandSpec> {
    COMMAND_SPECS
        .iter()
        .copied()
        .chain(EXTRA_COMMAND_SPECS.iter().copied())
}

pub(super) fn command_spec_count() -> usize {
    COMMAND_SPECS.len() + EXTRA_COMMAND_SPECS.len()
}

static COMMAND_SPEC_MAP: LazyLock<HashBrownMap<&'static [u8], CommandSpec>> = LazyLock::new(|| {
    let mut map = HashBrownMap::with_capacity(COMMAND_SPECS.len() + EXTRA_COMMAND_SPECS.len());
    for spec in COMMAND_SPECS.iter().chain(EXTRA_COMMAND_SPECS.iter()) {
        map.insert(spec.name.as_bytes(), *spec);
    }
    map
});

pub(super) fn find_command_spec(name: &Bytes) -> Option<CommandSpec> {
    let upper = to_uppercase_stack(name);
    COMMAND_SPEC_MAP.get(upper.as_slice()).copied()
}

pub(super) fn find_command_spec_upper(name: &[u8]) -> Option<CommandSpec> {
    COMMAND_SPEC_MAP.get(name).copied()
}

pub(super) fn find_command_spec_parts(parts: &[Bytes]) -> Option<CommandSpec> {
    let mut name = Vec::new();
    for (idx, part) in parts.iter().enumerate() {
        if idx > 0 {
            name.push(b' ');
        }
        let upper = to_uppercase_stack(part);
        name.extend_from_slice(upper.as_slice());
    }
    COMMAND_SPEC_MAP.get(name.as_slice()).copied()
}

pub(super) fn is_write_command_name(name: &[u8]) -> bool {
    COMMAND_SPEC_MAP
        .get(name)
        .is_some_and(|spec| spec.flags.contains(&"write"))
}
