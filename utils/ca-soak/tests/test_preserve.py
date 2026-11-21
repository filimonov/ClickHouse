from soak import preserve
from soak.stand import Stand

RAW = ('ref_catalog v1\n'
       '{"kind":"entry","ns":"srv1/ca_a","state":"live","life":"0a1b"}\n'
       '{"kind":"entry","ns":"srv1/ca_b","state":"removing","life":"0a1c","removal_started_round":7}\n'
       '{"kind":"other","x":1}\n')


def test_catalog_rows_keep_namespace_state_and_extras():
    rows = preserve.catalog_rows(RAW)
    assert [r["ns"] for r in rows] == ["srv1/ca_a", "srv1/ca_b"]
    tsv = preserve.catalog_tsv(rows).splitlines()
    assert tsv[0] == "namespace\tstate\tlife\tother"
    assert tsv[2].startswith("srv1/ca_b\tremoving\t0a1c\t") and "removal_started_round" in tsv[2]


def test_system_log_query_is_bounded_and_has_names():
    q = preserve.system_log_query("cas_log")
    assert f"LIMIT {preserve.SYSTEM_LOG_ROWS}" in q and f"INTERVAL {preserve.SYSTEM_LOG_HOURS} HOUR" in q
    assert q.endswith("FORMAT TSVWithNames")


def test_removal_command_names_the_project_and_the_binary(tmp_path):
    s = Stand(str(tmp_path), "soakfix", "/bin/true", 23350, ["docker-compose.yml"], [], print)
    cmd = s.removal_command()
    assert "-p soakfix" in cmd and "down -v --remove-orphans" in cmd and "CA_SOAK_CLICKHOUSE_BINARY=/bin/true" in cmd
