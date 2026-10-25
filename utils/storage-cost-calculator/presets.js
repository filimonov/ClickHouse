// Scenario and price presets for the calculator page (spec §7 "Presets").
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const SCC = root.SCC || (root.SCC = {});
  const pipeline = () => root.SCC.pipeline || require('./pipeline.js');

  // Scenario values are a product manager's round numbers for a plausible ClickHouse workload. Sizes are compressed
  // on-disk bytes (`bytes_on_disk`); `ingest_rate` is per day, `insert_rate` is per second. Only the locked members of the
  // data = ingest × retention group carry a value: the third member is derived, and a value for it would be ignored.
  // Every value lies inside every validity domain of the contract; `coverageProblems` checks it. Each scenario sets
  // `columns` itself: the table width is a workload fact, not left to the `shape` preset.
  // Real tables often merge into larger parts than the measured `merge_size_ratio` range; such a workload is
  // entered by the user and labelled extrapolated, not shipped as a preset.
  const SCENARIOS = {
    // Log shipping (Vector, Fluent Bit): 500 GiB/day compressed kept 30 days (15,000 GiB derived), two replicas.
    // One 100,000-row batch per second: 500 GiB/day over 86,400 inserts is ~5.9 MiB compressed per insert, ~62 B
    // per row; 60 MiB uncompressed is ~10x compression. Shippers batch themselves, so async inserts are off. Log tables carry
    // many columns: wide shape.
    logs: {
      label: 'Logs',
      description: '500 GiB/day of logs kept 30 days on 2 replicas, one 100,000-row batch per second; append-only, no ALTER UPDATE/DELETE',
      values: {
        ingest_rate: 500, retention_days: 30, replicas: 2,
        insert_rows: 100000, insert_rate: 1, insert_bytes_uncompressed: 62914560, insert_clients: 4,
        // Ratio 256 merges ~5.9 MiB inserts into ~1.5 GiB parts.
        async_insert: '0', shape: 'wide', columns: 40, merge_size_ratio: 256,
        // Append-only: logs are never updated or deleted in place, so no mutation of any kind runs.
        mutations_per_month: 0, mutated_compact_parts_per_month: 0, mutated_wide_parts_per_month: 0, cas_mutations_per_s: 0,
        // Searches over recent logs: 2,000 parts read a day that miss the cache. One rolling restart a month.
        select_part_reads_per_day: 2000, restarts_per_month: 1,
      },
      locks: ['ingest_rate', 'retention_days'],
    },
    // Metrics from many agents: 20 GiB/day compressed kept 90 days (1,800 GiB derived), two replicas.
    // 50 agents send 1,000-row inserts, 50 per second in total: 20 GiB/day over 4,320,000 inserts is ~4.9 KiB
    // compressed per insert, ~5 B per row (Delta/Gorilla codecs); 100 KiB uncompressed is ~20x compression. Small inserts go through the async queue. Few columns: narrow shape.
    metrics: {
      label: 'Metrics',
      description: '20 GiB/day of metrics kept 90 days on 2 replicas, 50 small async inserts per second; append-only, no ALTER UPDATE/DELETE',
      values: {
        ingest_rate: 20, retention_days: 90, replicas: 2,
        insert_rows: 1000, insert_rate: 50, insert_bytes_uncompressed: 102400, insert_clients: 50,
        async_insert: '1', shape: 'narrow', columns: 5, merge_size_ratio: 256,
        // Append-only: samples are never rewritten; expiry is by TTL or partition drop, not by mutation.
        mutations_per_month: 0, mutated_compact_parts_per_month: 0, mutated_wide_parts_per_month: 0, cas_mutations_per_s: 0,
        // Dashboards re-read history often: 5,000 parts read a day that miss the cache. One rolling restart a month.
        select_part_reads_per_day: 5000, restarts_per_month: 1,
      },
      locks: ['ingest_rate', 'retention_days'],
      // 1,000-row inserts lie below the measured mutation part sizes (op_write_bytes) and below the part sizes the
      // replica fetch transfer was measured at (acquire_bytes, cas_relink_bytes_per_part); the page shows the
      // dependent numbers as extrapolated.
      expected_extrapolated: { ebs: ['op_write_bytes', 'acquire_bytes'], s3: ['acquire_bytes'], s3plain: ['acquire_bytes'], cas: ['cas_relink_bytes_per_part'] },
    },
    // Analytics warehouse loaded in batches: 10 TiB kept one year, so ingest is derived (~28 GiB/day), two replicas.
    // A loader sends 100,000-row native inserts, one every 20 s: 10,240 GiB / 365 d = 28 GiB/day over 4,320 inserts
    // a day is ~6.6 MiB compressed per insert, ~70 B per row; 100 MiB uncompressed is ~15x compression.
    analytics_batch: {
      label: 'Analytics batch',
      description: '10 TiB kept one year on 2 replicas, batch loads of 100,000 rows every 20 seconds, 30 ALTER UPDATE corrections a month',
      values: {
        data_bytes: 10240, retention_days: 365, replicas: 2,
        insert_rows: 100000, insert_rate: 0.05, insert_bytes_uncompressed: 104857600, insert_clients: 1,
        // Ratio 400 merges ~6.6 MiB inserts into ~2.6 GiB parts.
        async_insert: '0', shape: 'wide', columns: 40, merge_size_ratio: 400,
        // Periodic corrections and backfills: 30 ALTER UPDATE statements a month, each rewriting ~10 wide parts of the
        // affected partitions (300 parts a month). The CAS rate is the same 30 a month per second (30 / 2,628,000 s).
        mutations_per_month: 30, mutation_kind: 'update_column', mutated_compact_parts_per_month: 0, mutated_wide_parts_per_month: 300,
        cas_mutations_per_s: 30 / 2628000, cas_parts_per_mutation: 10,
        // Reports scan wide date ranges: 20,000 parts read a day that miss the cache. One rolling restart a month.
        select_part_reads_per_day: 20000, restarts_per_month: 1,
      },
      locks: ['data_bytes', 'retention_days'],
    },
    // Cold archive without replication: 50 TiB kept two years (~70 GiB/day derived) on one replica.
    // One 100,000-row insert every 10 s: 51,200 GiB / 730 d = 70 GiB/day over 8,640 inserts a day is ~8.3 MiB
    // compressed per insert; 100 MiB uncompressed is ~12x compression.
    single_replica_archive: {
      label: 'Single replica archive',
      description: '50 TiB kept two years on 1 replica, one 100,000-row insert every 10 seconds; append-only, no ALTER UPDATE/DELETE',
      values: {
        data_bytes: 51200, retention_days: 730, replicas: 1,
        insert_rows: 100000, insert_rate: 0.1, insert_bytes_uncompressed: 104857600, insert_clients: 1,
        // Ratio 450 merges ~8.3 MiB inserts into ~3.7 GiB parts, the largest a measured ratio allows.
        async_insert: '0', shape: 'wide', columns: 40, merge_size_ratio: 450,
        // Append-only: an archive is written once and expires by partition drop.
        mutations_per_month: 0, mutated_compact_parts_per_month: 0, mutated_wide_parts_per_month: 0, cas_mutations_per_s: 0,
        // Rare lookups: 200 parts read a day. One restart a month.
        select_part_reads_per_day: 200, restarts_per_month: 1,
      },
      locks: ['data_bytes', 'retention_days'],
    },
    // The standard deployment: what the workload knobs alone describe. Wide table, async inserts on (the async buffer
    // flushes ~1 MiB, the registry default), default merges, a modest number of cold part reads, one restart a month,
    // no mutations, 3 replicas in one zone, filesystem cache on. Everything else stays at its registry default.
    custom: {
      label: 'Custom',
      description: 'A typical deployment: wide table, async inserts, 3 replicas in one zone, filesystem cache on, 1,000 cold part reads a day, one restart a month, no ALTER UPDATE/DELETE; edit any input',
      values: {
        replicas: 3, topology: 'same_az', shape: 'wide', columns: 40, async_insert: '1', cache_enabled: '1', select_part_reads_per_day: 1000, restarts_per_month: 1,
        mutations_per_month: 0, mutated_compact_parts_per_month: 0, mutated_wide_parts_per_month: 0, cas_mutations_per_s: 0,
      },
      locks: [],
    },
  };

  // Inputs the chart and tools/sweeps.mjs sweep. Both call ladderFor, so page and document report the same intervals.
  const SWEEP_INPUTS = { data_bytes: { integer: false }, ingest_rate: { integer: false }, replicas: { integer: true } };
  // ×¼ … ×8 of the current value: the workload sits near the left third of the chart instead of at one end of a
  // fixed 64 GiB … 4 PiB span. Geometric, so a crossing is bracketed at any scale.
  const LADDER_FACTORS = [0.25, 0.5, 1, 2, 4, 8];

  // The x values to sweep `inputId` over around `current`, clamped to `validity` ([lo, hi] or null). The current value
  // itself is always a point, even outside the validity, so the chart marks where the workload is. Counts are whole
  // numbers. Returns null when there is no positive current value to scale.
  function ladderFor(inputId, current, validity) {
    const spec = SWEEP_INPUTS[inputId];
    if (!spec) throw new Error(`${inputId} is not a sweep input; expected one of ${Object.keys(SWEEP_INPUTS).join(', ')}`);
    if (typeof current !== 'number' || !Number.isFinite(current) || current <= 0) return null;
    let xs = LADDER_FACTORS.map((f) => current * f);
    if (validity) xs = xs.map((x) => Math.min(validity[1], Math.max(validity[0], x)));
    if (spec.integer) xs = xs.map((x) => Math.max(1, Math.round(x)));
    return [...new Set([...xs, current])].sort((a, b) => a - b);
  }

  // Words for a billing rule id of a preset; an unknown rule throws.
  // AWS names its request classes by their leading operations; the full op list stays in the billing contract.
  const REQUEST_LABELS = { put_copy_post_list: 'PUT, COPY, POST and LIST requests', get_and_other: 'GET and other requests', delete_cancel: 'DELETE and cancel requests' };
  const BLOCK_LABELS = { block_capacity: 'gp3 capacity', block_iops: 'gp3 IOPS above baseline', block_mibps: 'gp3 throughput above baseline' };
  function ruleLabel(ruleId, preset) {
    if (ruleId === 'storage') return 'S3 storage';
    if (BLOCK_LABELS[ruleId]) return BLOCK_LABELS[ruleId];
    if ((preset.transfer || []).some((t) => t.id === ruleId)) {
      if (!TRANSFER_LABELS[ruleId]) throw new Error(`transfer rule ${ruleId} has no label in presets.js`);
      return TRANSFER_LABELS[ruleId];
    }
    if ((preset.request_classes || []).some((c) => c.id === ruleId)) {
      if (!REQUEST_LABELS[ruleId]) throw new Error(`request class ${ruleId} has no label in presets.js`);
      return REQUEST_LABELS[ruleId];
    }
    throw new Error(`billing rule ${ruleId} is not a rule of price preset ${preset.id}`);
  }

  // Price preset ids are the billing contract's. `contract` presets are taken from the contract as they are;
  // `custom` is an editable copy of `base`.
  const PRICES = {
    'aws-us-east-1': { label: 'AWS us-east-1', source: 'contract', structure_only: false },
    gcs: { label: 'GCS', source: 'contract', structure_only: true, note: 'values not provided' },
    azure: { label: 'Azure', source: 'contract', structure_only: true, note: 'values not provided' },
    custom: { label: 'Custom', source: 'custom', base: 'aws-us-east-1', structure_only: false },
  };

  function contractPreset(contract, id) {
    const p = ((contract.billing && contract.billing.presets) || []).find((x) => x.id === id);
    if (!p) throw new Error(`price preset ${id} is not present in billing version ${contract.manifest.billing_version}`);
    return p;
  }

  // Display label: a priced preset names the date its prices were checked.
  function priceLabel(contract, id) {
    const d = PRICES[id];
    if (!d) throw new Error(`unknown price preset ${JSON.stringify(id)}; expected one of ${Object.keys(PRICES).join(', ')}`);
    if (d.structure_only) return `${d.label} (${d.note})`;
    const p = contractPreset(contract, d.source === 'custom' ? d.base : id);
    return d.source === 'custom' ? `${d.label} (from ${PRICES[d.base].label}, ${p.as_of})` : `${d.label} (${p.as_of})`;
  }

  // The billing contract names transfer rules by id only; the page shows words. An unknown rule throws.
  const TRANSFER_LABELS = {
    ec2_s3_same_region: 'Transfer EC2 to S3 in the same region',
    ec2_ec2_cross_az: 'Transfer EC2 to EC2 across availability zones',
    internet: 'Transfer to the internet',
  };

  // Editable price fields: {path, label, value, per, unit}. A path is `section.<id>.field` for lists keyed by id,
  // `storage.tiers.<index>.price_per_gb_month` for tiers and `block.gp3.field` for gp3.
  function priceFields(preset) {
    if (preset.structure_only) return [];
    const out = [];
    const add = (f) => out.push(Object.assign(f, { unit: `${preset.currency} ${f.per}` }));
    for (const c of preset.request_classes) add({ path: `request_classes.${c.id}.price_per_1000`, label: ruleLabel(c.id, preset), per: 'per 1,000 requests', value: c.price_per_1000, source_url: c.source_url });
    preset.storage.tiers.forEach((t, i) => add({ path: `storage.tiers.${i}.price_per_gb_month`, label: t.up_to_gb === null ? 'Storage above the last tier' : `Storage up to ${t.up_to_gb.toLocaleString('en-US')} GB`, per: 'per GB-month', value: t.price_per_gb_month, source_url: preset.storage.source_url }));
    for (const t of preset.transfer) {
      if (!Object.prototype.hasOwnProperty.call(TRANSFER_LABELS, t.id)) throw new Error(`transfer rule ${t.id} has no label in presets.js`);
      add({ path: `transfer.${t.id}.price_per_gb`, label: TRANSFER_LABELS[t.id], per: 'per GB', value: t.price_per_gb, source_url: t.source_url });
    }
    const g = preset.block.gp3;
    add({ path: 'block.gp3.price_per_gib_month', label: 'gp3 capacity', per: 'per GiB-month', value: g.price_per_gib_month, source_url: g.source_url });
    add({ path: 'block.gp3.iops_price', label: 'gp3 IOPS above baseline', per: 'per IOPS-month', value: g.iops_price, source_url: g.source_url });
    add({ path: 'block.gp3.mibps_price', label: 'gp3 throughput above baseline', per: 'per MiB/s-month', value: g.mibps_price, source_url: g.source_url });
    return out;
  }

  function setPath(preset, path, value) {
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) throw new Error(`price ${path}: expected a finite number >= 0, got ${JSON.stringify(value)}`);
    const [section, key, field] = path.split('.');
    let target = null;
    if (section === 'request_classes' || section === 'transfer') target = (preset[section] || []).find((x) => x.id === key);
    else if (section === 'storage' && key === 'tiers') {
      // storage.tiers.<i>.price_per_gb_month has four parts.
      const [, , i, f] = path.split('.');
      const tier = preset.storage.tiers[Number(i)];
      if (!tier || f !== 'price_per_gb_month' || !/^\d+$/.test(i)) throw new Error(`price ${path}: no such price field`);
      tier[f] = value;
      return;
    } else if (section === 'block' && key === 'gp3') target = preset.block.gp3;
    if (!target || !['price_per_1000', 'price_per_gb', 'price_per_gib_month', 'iops_price', 'mibps_price'].includes(field) || !(field in target)) {
      throw new Error(`price ${path}: no such price field`);
    }
    target[field] = value;
  }

  // Returns the preset object `SCC.pipeline.run` takes. `custom` is a copy of its base with `edits` ({path: value})
  // applied; an edit of a path that is not a price field throws.
  function resolvePrice(contract, id, edits) {
    const d = PRICES[id];
    if (!d) throw new Error(`unknown price preset ${JSON.stringify(id)}; expected one of ${Object.keys(PRICES).join(', ')}`);
    if (d.source === 'contract') {
      const p = contractPreset(contract, id);
      if (!!p.structure_only !== d.structure_only) throw new Error(`price preset ${id}: structure_only is ${!!p.structure_only} in the billing contract, ${d.structure_only} in presets.js`);
      if (edits && Object.keys(edits).length) throw new Error(`price preset ${id} is not editable; edit the custom preset`);
      return p;
    }
    const copy = JSON.parse(JSON.stringify(contractPreset(contract, d.base)));
    copy.id = 'custom';
    copy.base = d.base;
    for (const [path, v] of Object.entries(edits || {})) setPath(copy, path, v);
    return copy;
  }

  // Every validity domain in the contract: coefficients and registry rows. Returns [{id, owner, param, range}].
  function validityDomains(contract) {
    const out = [];
    const M = root.SCC.model || require('./model.js');
    for (const c of contract.coefficients) for (const [param, range] of Object.entries(M.rangeOf(c) || {})) out.push({ id: c.id, owner: `coefficient ${c.id}`, param, range, basis: M.basisOf(c) });
    // A registry row is `derived` when it is a formula from source, or an input only such formulas read.
    const P = pipeline();
    const derivedOnly = P.derivedOnlyInputs(contract);
    const basisOf = (q) => (q.kind === 'derivation' ? P.derivationBasis(q) : derivedOnly.has(q.id) ? 'derived' : 'fitted');
    for (const q of contract.registry) for (const [param, range] of Object.entries(M.rangeOf(q) || {})) out.push({ id: q.id, owner: `registry ${q.id}`, param, range, basis: basisOf(q) });
    return out;
  }

  // Ids of the coefficients and tables the page can read for this contract: every `coef`/`lookup` node in a cell of
  // any variant (or of `variant` alone), followed through the registry derivations of the inputs those cells read.
  function reachedCoefficients(contract, variant) {
    const ids = new Set();
    const seenInputs = new Set();
    const rules = new Map(contract.registry.filter((q) => q.kind === 'derivation' && q.rule).map((q) => [q.id, q.rule]));
    const visit = (node) => {
      if (!node || typeof node !== 'object') return;
      if (Array.isArray(node)) { node.forEach(visit); return; }
      if (node.kind === 'coef') ids.add(node.id);
      else if (node.kind === 'lookup') ids.add(node.table_id);
      else if (node.kind === 'input' && !seenInputs.has(node.id)) {
        seenInputs.add(node.id);
        if (rules.has(node.id)) visit(rules.get(node.id));
      }
      for (const v of Object.values(node)) visit(v);
    };
    for (const v of variant ? [variant] : contract.variants) for (const cell of Object.values((contract.formulas || {})[v] || {})) visit(cell);
    return ids;
  }

  // Coverage of a scenario: `problems` keep it from being "within measured coverage" (an id that is not a registry
  // input, a lock outside a precedence group, a value for a derived member, a value the pipeline rejects, or an
  // effective value (scenario over registry defaults, derived members included) outside the validity domain of a
  // coefficient the scenario reaches (see `reachedCoefficients`; `reported_only` coefficients are skipped) or of a
  // registry row). A validity parameter the pipeline does not resolve cannot be checked and is a problem too.
  // `scenario.expected_extrapolated` ({variant: [coefficient ids]}) declares coefficients that lie out of range on
  // purpose: such a coefficient is listed in `declared` instead of `problems`. A declaration must stay true, so a
  // declared coefficient the variant does not reach, or one that is in range, is a problem. A table whose lookup
  // the scenario answers with the nearest measured key (`ui.nearest`) must be declared the same way.
  function coverageReport(contract, scenario) {
    const problems = [];
    const declared = [];
    const info = [];
    const res = pipeline().resolveInputs(contract, scenario.values, scenario.locks, {});
    // Validity may sit on a derived quantity (part_rows): check against what the pipeline evaluates, not raw inputs.
    const evaluated = pipeline().evaluationInputs(contract, res.inputs);
    problems.push(...res.problems);
    const byId = new Map(contract.registry.map((q) => [q.id, q]));
    for (const id of scenario.locks) if (!byId.has(id)) problems.push(`lock ${id} is not a registry id`);
    for (const id of Object.keys(scenario.values)) {
      if (!byId.has(id)) problems.push(`value ${id} is not a registry id`);
      else if (res.fields[id] && res.fields[id].derived) problems.push(`value ${id} is set but derived from its locked partners`);
    }
    const reached = reachedCoefficients(contract);
    const reachedBy = new Map(contract.variants.map((v) => [v, reachedCoefficients(contract, v)]));
    const coefIds = new Set(contract.coefficients.map((c) => c.id));
    const skipped = new Set(contract.coefficients.filter((c) => c.reported_only || !reached.has(c.id)).map((c) => c.id));
    const expected = scenario.expected_extrapolated || {};
    const seenOut = new Set(); // `${variant}:${coefficient}` pairs found out of range
    for (const d of validityDomains(contract)) {
      const id = d.id;
      // A registry row that mirrors a coefficient follows that coefficient's reach.
      if (coefIds.has(id) && skipped.has(id)) continue;
      if (!evaluated.has(d.param)) { problems.push(`${d.owner}: validity parameter ${d.param} has no value to check`); continue; }
      const v = evaluated.get(d.param);
      if (typeof v === 'number' && v >= d.range[0] && v <= d.range[1]) continue;
      const text = `${d.owner}: ${d.param} = ${JSON.stringify(v)} is outside [${d.range[0]}, ${d.range[1]}]`;
      // A formula derived from the source is exact outside the range it was verified at: information, not a gap.
      if (d.basis === 'derived') { info.push(`${text} (derived formula, verified range only)`); continue; }
      const variants = coefIds.has(id) ? contract.variants.filter((x) => reachedBy.get(x).has(id)) : [];
      if (variants.length && variants.every((x) => (expected[x] || []).includes(id))) {
        for (const x of variants) seenOut.add(`${x}:${id}`);
        declared.push(`${text} (declared)`);
      } else problems.push(text);
    }
    for (const sub of substitutionsReached(contract, scenario)) {
      const text = `${sub.variant}.${sub.operation}: ${sub.table_id} measured for ${sub.measured}, applied to ${sub.requested}`;
      if ((expected[sub.variant] || []).includes(sub.table_id)) {
        seenOut.add(`${sub.variant}:${sub.table_id}`);
        declared.push(`${text} (declared)`);
      } else problems.push(`${text}: nearest-key substitution not declared in expected_extrapolated`);
    }
    for (const [x, ids] of Object.entries(expected)) {
      for (const id of ids) if (!seenOut.has(`${x}:${id}`)) problems.push(`declared extrapolation ${x}:${id} is not out of range for a coefficient that variant reaches`);
    }
    // A column the scenario cannot price because a table lacks its key is outside coverage, unless declared in
    // `scenario.expected_lookup_misses` (["<variant>.<operation>: <key>"]); a declaration must stay true.
    const misses = lookupMisses(contract, scenario);
    const allowed = scenario.expected_lookup_misses || [];
    for (const m of misses) {
      if (allowed.includes(m)) declared.push(`lookup miss ${m} (declared)`);
      else problems.push(`lookup miss ${m}: the table has no entry for this key`);
    }
    for (const m of allowed) if (!misses.includes(m)) problems.push(`declared lookup miss ${m} does not occur`);
    return { problems, declared, info };
  }

  // Lookups the scenario answers with the nearest measured key: [{variant, operation, table_id, requested, measured}].
  function substitutionsReached(contract, scenario) {
    const res = pipeline().resolveInputs(contract, scenario.values, scenario.locks, {});
    const preset = ((contract.billing && contract.billing.presets) || [])[0];
    if (!preset) return [];
    const run = pipeline().run(contract, res.inputs, preset, {});
    const out = [];
    for (const [v, vr] of Object.entries(run.variants || {})) {
      for (const [op, cell] of Object.entries(vr.cells || {})) {
        for (const s of pipeline().substitutions(cell.trace)) out.push(Object.assign({ variant: v, operation: op }, s));
      }
    }
    return out;
  }

  // Lookup keys the scenario reaches that a table does not hold: "<variant>.<operation>: <key>" per miss.
  function lookupMisses(contract, scenario) {
    const res = pipeline().resolveInputs(contract, scenario.values, scenario.locks, {});
    const preset = ((contract.billing && contract.billing.presets) || [])[0];
    if (!preset) return [];
    const out = new Set();
    const walk = (t, where) => {
      if (!t || typeof t !== 'object') return;
      // A miss is an unavailable lookup with its key and no other reason (arity and bad values carry a reason).
      if (t.kind === 'lookup' && t.state === 'unavailable' && typeof t.lookup_key === 'string' && !t.reason
        && (t.children || []).every((k) => k && k.state === 'ok')) out.add(`${where}: ${t.lookup_key}`);
      for (const k of t.children || []) walk(k, where);
    };
    const run = pipeline().run(contract, res.inputs, preset, {});
    for (const [v, vr] of Object.entries(run.variants || {})) {
      if (vr.state === 'ok') continue;
      for (const [op, cell] of Object.entries(vr.cells || {})) for (const t of cell.trace || []) walk(t, `${v}.${op}`);
    }
    return [...out].sort();
  }

  function coverageProblems(contract, scenario) { return coverageReport(contract, scenario).problems; }

  SCC.presets = { lookupMisses, substitutionsReached, SCENARIOS, SWEEP_INPUTS, LADDER_FACTORS, ladderFor, ruleLabel, PRICES, priceLabel, priceFields, TRANSFER_LABELS, resolvePrice, coverageProblems, coverageReport, reachedCoefficients, validityDomains };
  if (typeof module !== 'undefined' && module.exports) module.exports = SCC.presets;
})(typeof window !== 'undefined' ? window : globalThis);
