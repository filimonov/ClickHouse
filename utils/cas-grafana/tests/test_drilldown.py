import os
import unittest

from casgrafana import drilldown, checks

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def panel(rows, title):
    return next(p for r in rows for p in r["panels"] if p["title"] == title)


class GcRows(unittest.TestCase):
    def test_row_titles(self):
        self.assertEqual([r["title"] for r in drilldown.gc_rows()],
                         ["GC: rounds", "GC: stage flow", "GC: phases", "GC: round cost", "GC: ref-log cleanup", "GC: anomalies"])
        self.assertTrue(all(r["collapsed"] for r in drilldown.gc_rows()))

    def test_rounds_panel_groups_by_outcome(self):
        sql = panel(drilldown.gc_rows(), "Rounds per interval, by outcome and host")["targets"][0]["query"]
        self.assertIn("concat(hostName(), ' ', toString(outcome))", sql)

    def test_leader_timeline_uses_async_metric(self):
        sql = panel(drilldown.gc_rows(), "GC leader (1 = this host leads)")["targets"][0]["query"]
        self.assertIn("metric LIKE 'CASGCIsLeader_%'", sql)
        self.assertIn("replaceOne(metric, 'CASGCIsLeader_', '') IN ($disk)", sql)

    def test_phase_table_reads_profile_events_map(self):
        sql = panel(drilldown.gc_rows(), "Phases: duration and S3 requests per phase")["targets"][0]["query"]
        for k in ("ProfileEvents['S3ListObjects']", "ProfileEvents['S3GetObject']", "quantile(0.99)(phase_duration_microseconds)"):
            self.assertIn(k, sql)

    def test_build_passes_checks_with_repo_names(self):
        known, async_prefixes = checks.load_known_names(ROOT)
        probs = [p for p in checks.check_dashboards([drilldown.build()], known, async_prefixes) if "variables" not in p]
        self.assertEqual(probs, [])


class WritesRows(unittest.TestCase):
    def test_row_titles(self):
        self.assertEqual([r["title"] for r in drilldown.writes_rows()],
                         ["Writes: mounts now", "Writes: lease", "Writes: conditional writes", "Writes: ref lane",
                          "Writes: blobs and dedup", "Writes: caches", "Writes: replication"])

    def test_mounts_now_refreshes_slowly_and_has_no_time_filter(self):
        p = panel(drilldown.writes_rows(), "Mounts now")
        self.assertEqual(p["interval"], "5m")
        sql = p["targets"][0]["query"]
        self.assertIn("system.cas_mounts", sql)
        self.assertNotIn("$timeFilter", sql)
        self.assertIn("dateDiff('second', now(), expires_at)", sql)

    def test_dedup_ratio(self):
        sql = panel(drilldown.writes_rows(), "Dedup ratio (%)")["targets"][0]["query"]
        self.assertIn("100 * sum(ProfileEvent_CASBlobPutDeduplicated) / greatest(sum(ProfileEvent_CASBlobPutDeduplicated) + sum(ProfileEvent_CASBlobHeadMiss), 1)", sql)

    def test_decode_cache_vs_cap(self):
        sql = panel(drilldown.writes_rows(), "Manifest decode cache bytes vs 128 MiB default cap")["targets"][0]["query"]
        self.assertIn("max(CurrentMetric_CASManifestDecodeCacheBytes)", sql)
        self.assertIn("134217728", sql)


if __name__ == "__main__":
    unittest.main()
