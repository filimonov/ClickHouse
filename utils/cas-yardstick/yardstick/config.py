"""Server configuration for one disk under test per process."""

import hashlib
import os
import shutil
import xml.etree.ElementTree as ET

POLICIES = ['cas', 's3', 's3plain', 'cas_cached', 's3_cached', 's3plain_cached']

ETC_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'etc')

ACCESS = '''<access_key_id>clickhouse</access_key_id>
        <secret_access_key>clickhouse</secret_access_key>
        <http_keep_alive_timeout>30</http_keep_alive_timeout>
        <http_keep_alive_max_requests>10000</http_keep_alive_max_requests>'''

# {endpoint}, {gc}, {meta} are filled in by write_config.
POLICY_DISK = {
    'cas': '''<type>object_storage</type>
        <object_storage_type>s3</object_storage_type>
        <metadata_type>cas</metadata_type>
        <endpoint>{endpoint}</endpoint>
        ''' + ACCESS + '''
        <cas_server_root_id>yardstick</cas_server_root_id>
        <cas_scratch_path>cas_scratch/</cas_scratch_path>
        <cas_gc_enabled>{gc}</cas_gc_enabled>
        <cas_gc_interval_sec>20</cas_gc_interval_sec>''',
    's3': '''<type>s3</type>
        <endpoint>{endpoint}</endpoint>
        ''' + ACCESS + '''
        <metadata_path>{meta}</metadata_path>''',
    's3plain': '''<type>object_storage</type>
        <object_storage_type>s3</object_storage_type>
        <metadata_type>plain_rewritable</metadata_type>
        <endpoint>{endpoint}</endpoint>
        ''' + ACCESS,
}

CACHE_DISK = '''<type>cache</type>
        <disk>{disk}</disk>
        <path>{disk}_cache/</path>
        <max_size>1Gi</max_size>
        <cache_on_write_operations>1</cache_on_write_operations>
        <background_download_threads>0</background_download_threads>
        <background_download_queue_size_limit>0</background_download_queue_size_limit>
        <load_metadata_asynchronously>0</load_metadata_asynchronously>'''

TEMPLATE = '''<clickhouse>
  <path>{run_dir}/data/</path>
  <tmp_path>{run_dir}/tmp/</tmp_path>
  <user_files_path>{run_dir}/uf/</user_files_path>
  <format_schema_path>{run_dir}/fs/</format_schema_path>
  <storage_configuration>
    <disks>
      <default/>
      <{disk}>
        {disk_body}
      </{disk}>{cache_block}
    </disks>
    <policies>
      <{policy}>
        <volumes>
          <main>
            <disk>{outer}</disk>
          </main>
        </volumes>
      </{policy}>
    </policies>
  </storage_configuration>
</clickhouse>
'''


def write_config(run_dir: str, policy: str, endpoint: str, prefix: str, gc_enabled: bool) -> str:
    """Write the server config under run_dir/etc; return the config hash.

    The hash covers the config files with run_dir and the bucket prefix masked,
    so passes of the same configuration hash equal.
    """
    if policy not in POLICIES:
        raise ValueError(f'unknown policy {policy!r}')
    run_dir = os.path.abspath(run_dir)
    disk = policy.replace('_cached', '')
    cached = policy.endswith('_cached')
    etc = os.path.join(run_dir, 'etc')
    os.makedirs(os.path.join(etc, 'config.d'), exist_ok=True)
    for sub in ('data', 'tmp', 'uf', 'caches'):
        os.makedirs(os.path.join(run_dir, sub), exist_ok=True)
    for name in ('config.xml', 'users.xml'):
        shutil.copyfile(os.path.join(ETC_SRC, name), os.path.join(etc, name))

    base = endpoint.rstrip('/')
    full_endpoint = f"{base}/{prefix.strip('/')}/"
    body = POLICY_DISK[disk].format(
        endpoint=full_endpoint,
        gc='1' if gc_enabled else '0',
        meta=os.path.join(run_dir, 's3_meta') + '/',
    )
    cache_block = ''
    if cached:
        cache_block = f'\n      <{policy}>\n        ' + CACHE_DISK.format(disk=disk) + f'\n      </{policy}>'
    text = TEMPLATE.format(run_dir=run_dir, disk=disk, disk_body=body, cache_block=cache_block,
                           policy=policy, outer=policy if cached else disk)
    with open(os.path.join(etc, 'config.d', 'yardstick.xml'), 'w') as f:
        f.write(text)

    # The prefixed endpoint is masked before run_dir: either may contain the other's text.
    masked = text.replace(full_endpoint, f'{base}/<PREFIX>/').replace(run_dir, '<RUN_DIR>')
    digest = hashlib.sha256()
    for name in ('config.xml', 'users.xml'):
        with open(os.path.join(etc, name), 'rb') as f:
            digest.update(f.read())
    digest.update(masked.encode())
    return digest.hexdigest()


COST_POLICIES = ['s3', 's3plain', 'cas', 'local', 's3_cached', 's3plain_cached', 'cas_cached']

LOCAL_DISK = '''<type>local</type>
        <path>{path}</path>'''

# Server-level blocks of one rig replica; ports are passed on the command line by server.sh, the rest must come from XML.
COST_SERVER_BLOCKS = '''
  <listen_host>127.0.0.1</listen_host>
  <interserver_http_host>127.0.1.{replica}</interserver_http_host>
  <zookeeper>
    <node><host>127.0.0.1</host><port>{keeper_client}</port></node>
    <session_timeout_ms>10000</session_timeout_ms>
  </zookeeper>
  <macros><shard>1</shard><replica>r{replica}</replica></macros>{keeper}'''

COST_KEEPER_BLOCK = '''
  <keeper_server>
    <tcp_port>{keeper_client}</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>{replica_dir}/keeper/log</log_storage_path>
    <snapshot_storage_path>{replica_dir}/keeper/snapshots</snapshot_storage_path>
    <coordination_settings>
      <operation_timeout_ms>10000</operation_timeout_ms>
      <session_timeout_ms>30000</session_timeout_ms>
    </coordination_settings>
    <raft_configuration>
      <server><id>1</id><hostname>127.0.0.1</hostname><port>{keeper_raft}</port></server>
    </raft_configuration>
  </keeper_server>'''


def _set_xml_path(root: ET.Element, dotted: str, value) -> None:
    """Set the element at a dotted path, reusing existing elements so a setting overrides instead of duplicating."""
    node = root
    for part in dotted.split('.'):
        child = node.find(part)
        if child is None:
            child = ET.SubElement(node, part)
        node = child
    node.text = str(value)


def write_cost_config(replica_dir: str, replica: int, policy: str, topology, key_prefix: str,
                      local_dir: str | None, gc_enabled: bool, cas_server_root_id: str,
                      server_settings: dict) -> str:
    """Write replica_dir/etc/{config.xml,users.xml,config.d/cost.xml}; return the config hash.

    Non-shared disks write under `<prefix>/r<replica>/`, CAS under `<prefix>/shared/`.
    The hash masks replica_dir, local_dir and the key prefix, so passes of one configuration hash equal.
    """
    if policy not in COST_POLICIES:
        raise ValueError(f'unknown policy {policy!r}')
    replica_dir = os.path.abspath(replica_dir)
    disk = policy.replace('_cached', '')
    cached = policy.endswith('_cached')
    etc = os.path.join(replica_dir, 'etc')
    os.makedirs(os.path.join(etc, 'config.d'), exist_ok=True)
    for sub in ('data', 'tmp', 'uf', 'fs', 'caches'):
        os.makedirs(os.path.join(replica_dir, sub), exist_ok=True)
    for name in ('config.xml', 'users.xml'):
        shutil.copyfile(os.path.join(ETC_SRC, name), os.path.join(etc, name))

    if disk == 'local':
        if not local_dir:
            raise ValueError('local policy needs local_dir')
        body = LOCAL_DISK.format(path=local_dir.rstrip('/') + '/')
        full_endpoint = None
    else:
        suffix = 'shared/' if disk == 'cas' else f'r{replica}/'
        full_endpoint = topology.endpoint(replica, key_prefix) + suffix
        body = POLICY_DISK[disk].format(endpoint=full_endpoint, gc='1' if gc_enabled else '0',
                                        meta=os.path.join(replica_dir, 's3_meta') + '/')
        body = body.replace('<cas_server_root_id>yardstick</cas_server_root_id>',
                            f'<cas_server_root_id>{cas_server_root_id}</cas_server_root_id>')
    cache_block = ''
    if cached:
        cache_block = f'\n      <{policy}>\n        ' + CACHE_DISK.format(disk=disk) + f'\n      </{policy}>'
    text = TEMPLATE.format(run_dir=replica_dir, disk=disk, disk_body=body, cache_block=cache_block,
                           policy=policy, outer=policy if cached else disk)
    keeper = ''
    if replica == 1:
        keeper = COST_KEEPER_BLOCK.format(keeper_client=topology.keeper_client(), replica_dir=replica_dir,
                                          keeper_raft=topology.keeper_raft())
    server = COST_SERVER_BLOCKS.format(replica=replica, keeper_client=topology.keeper_client(), keeper=keeper)
    root = ET.fromstring(text.replace('</clickhouse>', server + '\n</clickhouse>'))
    for dotted, value in server_settings.items():
        _set_xml_path(root, dotted, value)
    ET.indent(root)
    out = ET.tostring(root, encoding='unicode') + '\n'
    with open(os.path.join(etc, 'config.d', 'cost.xml'), 'w') as f:
        f.write(out)

    # Mask the longer, prefixed strings first: either path may contain the other's text.
    masked = out
    if full_endpoint is not None:
        masked = masked.replace(topology.endpoint(replica, key_prefix),
                                topology.endpoint(replica, '<PREFIX>'))
    if local_dir:
        masked = masked.replace(local_dir.rstrip('/'), '<LOCAL_DIR>')
    masked = masked.replace(replica_dir, '<REPLICA_DIR>')
    digest = hashlib.sha256()
    for name in ('config.xml', 'users.xml'):
        with open(os.path.join(etc, name), 'rb') as f:
            digest.update(f.read())
    digest.update(masked.encode())
    return digest.hexdigest()
