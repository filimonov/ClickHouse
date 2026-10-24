"""Capability gate of the `s3_plain_rewritable` disk (model variant `s3plain`, R3) at the pinned source.

One entry per cost operation: whether the variant can perform it, and the source lines that decide it. Each ref
names a token its line must contain, so a moved line fails `test_every_gate_cites_a_source_line...` instead of
citing the wrong code. Cards return `reason(op)` from `supports` for a refused operation.
"""
from __future__ import annotations

SRC = 'tmp/src-26.8/src/'
PLAIN_POLICIES = ('s3plain', 's3plain_cached')


def _ref(path: str, line: int, token: str, claim: str) -> dict:
    return {'path': SRC + path, 'line': line, 'token': token, 'claim': claim}


GATES = {
    'storage': {'supported': True, 'refs': [
        _ref('Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.cpp', 34,
             'object_storage_common_key_prefix / directory_remote_path / file_name',
             'a file is stored as is under its directory\'s random prefix')]},
    'inserts': {'supported': True, 'refs': [
        _ref('Storages/MergeTree/MergeTreeDataWriter.cpp', 974, 'createDirectories', 'an insert creates the part directory'),
        _ref('Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp',
             111, 'writeObject', 'a new directory writes its `prefix.path` object')]},
    'merges': {'supported': True, 'refs': [
        _ref('Storages/MergeTree/DataPartStorageOnDiskBase.cpp', 983, 'moveDirectory',
             'a merge result is renamed from its temporary directory'),
        _ref('Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp',
             239, 'createWriteBuf', 'a rename rewrites `prefix.path` of every directory of the subtree')]},
    'mutations': {'supported': False, 'refs': [
        _ref('Disks/DiskObjectStorage/DiskObjectStorage.cpp', 805, 'isPlain()',
             'a plain metadata storage reports no hard links'),
        _ref('Storages/MergeTree/MergeTreeData.cpp', 6240, 'supportsHardLinks',
             'checkMutationIsPossible refuses every mutation on such a disk'),
        _ref('Interpreters/InterpreterDeleteQuery.cpp', 200, 'UPDATE `_row_exists`',
             'a lightweight DELETE runs as ALTER UPDATE _row_exists, the same check'),
        _ref('Core/Settings.cpp', 5070, 'LightweightDeleteMode::ALTER_UPDATE',
             'that is the default lightweight_delete_mode'),
        _ref('Storages/MergeTree/MergeTreeData.cpp', 12630, 'enable_block_number_column',
             'a lightweight UPDATE (patch parts) needs enable_block_number_column'),
        _ref('Storages/MergeTree/MergeTreeSettings.cpp', 2013, 'enable_block_number_column, false',
             'which is off by default')]},
    'deletes': {'supported': True, 'refs': [
        _ref('Storages/MergeTree/MergeTreeData.cpp', 8249, 'PartitionCommand::DROP_PARTITION',
             'DROP PARTITION is one of the partition commands a plain-rewritable disk allows'),
        _ref('Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.cpp',
             390, 'copyObject', 'unlinking a file copies it aside before deleting it')]},
    'reads': {'supported': True, 'refs': [
        _ref('Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.cpp',
             359, 'constructFileObjectKey', 'a read resolves the object key from the in-memory layout')]},
    'recovery': {'supported': True, 'refs': [
        _ref('Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.cpp',
             138, 'constructMetadataDirectoryKey', 'startup lists `__meta/` to rebuild the layout'),
        _ref('Storages/MergeTree/DataPartsExchange.cpp', 1254, 'createDirectories',
             'a replicated fetch writes the part through the disk like an insert')]},
    'local_disk': {'supported': True, 'refs': [
        _ref('Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h',
             87, 'FsMetadata fs', 'the layout is held in memory; no local metadata file per object')]},
}

# The operation a card exercises, for the cards whose operation the gate can refuse.
CARD_OPERATION = {'mutation': 'mutations'}


def reason(op: str) -> str:
    gate = GATES[op]
    if gate['supported']:
        raise ValueError(f'{op} is supported on s3plain')
    cites = '; '.join(f'{r["claim"]} ({r["path"]}:{r["line"]})' for r in gate['refs'])
    return f'UNSUPPORTED: s3_plain_rewritable cannot perform {op}: {cites}'


def supports(card_name: str, policy: str) -> tuple | None:
    """The gate's answer for a plain policy, or None when the gate does not decide (another policy, or an operation
    the variant performs)."""
    op = CARD_OPERATION.get(card_name)
    if policy not in PLAIN_POLICIES or op is None or GATES[op]['supported']:
        return None
    return False, reason(op)
