"""Generation 4 (deletion intents) harness contracts: pool classification, fsck parsing, hard findings,
dry-run `standing` entries. Pure; no cluster, no docker."""

from scenarios.framework import assertions, observe
from scenarios.framework.report import ScenarioResult, PASS, FAIL, INCONCLUSIVE
from soak.fsck import parse_dryrun, parse_fsck_summary


def _result():
    return ScenarioResult(scenario="T", title="t", priority="P0", seed=1, params={})


def test_pool_classification_generation_4():
    c = observe.classify_pool_path
    assert c("soak_pool/blob-intents/xxh3/a1/a1b2c3/etag/613162") == "blob_intents"
    assert c("blob-intents/ch128/ab/abcd/generation/00ff") == "blob_intents"
    assert c("soak_pool/blobs/xxh3/a1/a1b2c3d4e5f60708b1c2d3e4f5061728.meta") == "legacy_meta"
    assert c("soak_pool/blobs/xxh3/a1/a1b2c3d4e5f60708b1c2d3e4f5061728") == "blobs"
    # RustFS data-directory view: one directory per object holding `xl.meta` (and `part.1` when large).
    assert c("soak_pool/blobs/xxh3/a1/a1b2c3d4e5f60708b1c2d3e4f5061728/xl.meta") == "blobs"
    assert c("soak_pool/blobs/xxh3/a1/a1b2c3d4e5f60708b1c2d3e4f5061728/9f3e/part.1") == "blobs"
    assert c("soak_pool/blobs/xxh3/a1/a1b2c3d4e5f60708b1c2d3e4f5061728.meta/xl.meta") == "legacy_meta"
    # RustFS multipart part metadata is not a marker.
    assert c("soak_pool/blobs/ch128/0a/0a4322c7103ae0322c932ba755b788a2/8673ec8c/part.1.meta") == "blobs"
    assert c("soak_pool/blob-intents/xxh3/a1/a1b2c3d4e5f60708b1c2d3e4f5061728/etag/613162/xl.meta") == "blob_intents"
    assert c("soak_pool/cas/ns/stream/0123456789abcdef0123456789abcdef/_log/0000000000000001-0000000000000003.zst") == "refs"
    assert c("soak_pool/cas/ns/state/0123456789abcdef0123456789abcdef/_ckpt") == "refs"
    assert c("soak_pool/cas/ref_catalog") == "refs"
    assert c("soak_pool/cas/ns/state/0123456789abcdef0123456789abcdef/_files/columns.txt") == "_files"
    assert c("soak_pool/cas/manifests/srv1/store/3f2/uuid@cas@/0000000000000001-0000000000000003/000001.zst") == "_manifests"


def test_fsck_summary_with_intent_fields_parses():
    line = ("reachable=3 dangling=0 unreachable=1 pending_gc=1 awaiting_gc=0 unaccounted=0 stale_edge=0 "
            "corrupted_runs=0 chain_broken=0 lifeless_keys=0 intents_listed=2 intent_current=0 intent_replaced=0 "
            "intent_body_gone=0 intent_malformed=0 intent_unread=0 pending_without_intent=0 "
            "pending_intent_absent=0 pending_token_rejected=0 standing_present=0 intents_verified=0 "
            "legacy_marker=0 legacy_markers_deleted=0 legacy_markers_changed=0 janitor_pending=0 "
            "janitor_pending_bytes=0 janitor_pending_lives=0 unchecked=0 ref_records_walked=9 "
            "physical_bytes=100 referenced_logical_bytes=50 distinct_blobs=3 total_blob_refs=4 dedup_ratio=1.5")
    s = parse_fsck_summary(line)
    assert s["intents_listed"] == 2 and s["dedup_ratio"] == 1.5 and s["legacy_marker"] == 0


def test_fsck_intents_listed_reads_new_and_old_spelling():
    from scenarios.framework.lifecycle import fsck_intents_listed
    assert fsck_intents_listed({"intents_listed": 3}) == 3
    assert fsck_intents_listed({"intent_listed": 4}) == 4
    assert fsck_intents_listed({"intents_listed": 1, "intent_listed": 9}) == 1
    try:
        fsck_intents_listed({"unreachable": 0})
    except RuntimeError:
        pass
    else:
        raise AssertionError("absent field must raise")


def test_hard_findings_pass_fail_and_missing():
    base = {k: 0 for k in assertions._FSCK_HARD_FIELDS}
    r = _result()
    assert assertions.assert_fsck_hard_findings(r, base).status == PASS
    bad = dict(base, pending_without_intent=2)
    assert assertions.assert_fsck_hard_findings(_result(), bad).status == FAIL
    lacking = {k: 0 for k in assertions._FSCK_HARD_FIELDS if k != "intent_unread"}
    assert assertions.assert_fsck_hard_findings(_result(), lacking).status == INCONCLUSIVE


def test_legacy_marker_check():
    ok = {"_ok": True, "legacy_meta": {"objects": 0, "bytes": 0}}
    bad = {"_ok": True, "legacy_meta": {"objects": 3, "bytes": 9}}
    assert assertions.assert_no_legacy_markers(_result(), ok)[0].status == PASS
    assert assertions.assert_no_legacy_markers(_result(), bad)[0].status == FAIL
    assert assertions.assert_no_legacy_markers(_result(), {"_ok": False})[0].status == INCONCLUSIVE


def test_dryrun_standing_entries_are_not_body_deletes():
    dry = parse_dryrun("preview_deletes=3\nunreachable\tp/blobs/xxh3/aa/aa1\t10\n"
                       "pending\tp/blobs/xxh3/bb/bb1\t10\nstanding\tp/blobs/xxh3/cc/cc1\t10\n")
    detail = {"detail": [{"class": "unreachable", "key": "p/blobs/xxh3/aa/aa1", "size": 10},
                         {"class": "pending-gc", "key": "p/blobs/xxh3/bb/bb1", "size": 10},
                         {"class": "reachable", "key": "p/blobs/xxh3/cc/cc1", "size": 10}]}
    r = _result()
    v = assertions.assert_dryrun_subset(r, detail, dry)[0]
    assert v.status == PASS
    assert r.observations["dryrun_standing_entries_excluded"] == 1
    # a `pending` entry whose key fsck calls reachable is still a real over-proposal
    dry2 = parse_dryrun("preview_deletes=1\npending\tp/blobs/xxh3/cc/cc1\t10\n")
    assert assertions.assert_dryrun_subset(_result(), detail, dry2)[0].status == FAIL
