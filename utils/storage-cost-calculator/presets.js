// Scenario and price presets for the calculator page (spec §7 "Presets").
// Classic script: attaches to window.SCC in the browser and exports the same object under Node.
(function (root) {
  'use strict';

  const SCC = root.SCC || (root.SCC = {});
  const pipeline = () => root.SCC.pipeline || require('./pipeline.js');

  // Scenario values are a product manager's round numbers for a plausible ClickHouse workload. Sizes are compressed
  // on-disk bytes (`bytes_on_disk`); `ingest_rate` is per day, `insert_rate` is per second. Only the locked members of the
  // data = ingest × retention group carry a value: the third member is derived, and a value for it would be ignored.
  // Every value lies inside every validity domain of the contract; `coverageProblems` checks it.
  // Real tables often merge into larger parts than the measured `merge_size_ratio` range; such a workload is
  // entered by the user and labelled extrapolated, not shipped as a preset.
  const SCENARIOS = {
    // Log shipping (Vector, Fluent Bit): 500 GiB/day compressed kept 30 days (15,000 GiB derived), two replicas.
    // One 100,000-row batch per second: 500 GiB/day over 86,400 inserts is ~5.9 MiB compressed per insert, ~62 B
    // per row; 60 MiB uncompressed is ~10x compression. Shippers batch themselves, so async inserts are off. Log tables carry
    // many columns: wide shape.
    logs: {
      label: 'Logs',
      description: '500 GiB/day of logs kept 30 days on 2 replicas, one 100,000-row batch per second',
      values: {
        ingest_rate: 500, retention_days: 30, replicas: 2,
        insert_rows: 100000, insert_rate: 1, insert_bytes_uncompressed: 62914560, insert_clients: 4,
        async_insert: '0', shape: 'wide', merge_size_ratio: 512,
      },
      locks: ['ingest_rate', 'retention_days'],
    },
    // Metrics from many agents: 20 GiB/day compressed kept 90 days (1,800 GiB derived), two replicas.
    // 50 agents send 1,000-row inserts, 50 per second in total: 20 GiB/day over 4,320,000 inserts is ~4.9 KiB
    // compressed per insert, ~5 B per row (Delta/Gorilla codecs); 100 KiB uncompressed is ~20x compression. Small inserts go through the async queue. Few columns: narrow shape.
    metrics: {
      label: 'Metrics',
      description: '20 GiB/day of metrics kept 90 days on 2 replicas, 50 small async inserts per second',
      values: {
        ingest_rate: 20, retention_days: 90, replicas: 2,
        insert_rows: 1000, insert_rate: 50, insert_bytes_uncompressed: 102400, insert_clients: 50,
        async_insert: '1', shape: 'narrow', merge_size_ratio: 256,
      },
      locks: ['ingest_rate', 'retention_days'],
    },
    // Analytics warehouse loaded in batches: 10 TiB kept one year, so ingest is derived (~28 GiB/day), two replicas.
    // A loader sends 100,000-row native inserts, one every 20 s: 10,240 GiB / 365 d = 28 GiB/day over 4,320 inserts
    // a day is ~6.6 MiB compressed per insert, ~70 B per row; 100 MiB uncompressed is ~15x compression.
    analytics_batch: {
      label: 'Analytics batch',
      description: '10 TiB kept one year on 2 replicas, batch loads of 100,000 rows every 20 seconds',
      values: {
        data_bytes: 10240, retention_days: 365, replicas: 2,
        insert_rows: 100000, insert_rate: 0.05, insert_bytes_uncompressed: 104857600, insert_clients: 1,
        async_insert: '0', shape: 'wide', merge_size_ratio: 640,
      },
      locks: ['data_bytes', 'retention_days'],
    },
    // Cold archive without replication: 50 TiB kept two years (~70 GiB/day derived) on one replica.
    // One 100,000-row insert every 10 s: 51,200 GiB / 730 d = 70 GiB/day over 8,640 inserts a day is ~8.3 MiB
    // compressed per insert; 100 MiB uncompressed is ~12x compression.
    single_replica_archive: {
      label: 'Single replica archive',
      description: '50 TiB kept two years on 1 replica, one 100,000-row insert every 10 seconds',
      values: {
        data_bytes: 51200, retention_days: 730, replicas: 1,
        insert_rows: 100000, insert_rate: 0.1, insert_bytes_uncompressed: 104857600, insert_clients: 1,
        async_insert: '0', shape: 'wide', merge_size_ratio: 700,
      },
      locks: ['data_bytes', 'retention_days'],
    },
    // The user's own workload: registry defaults until edited.
    custom: {
      label: 'Custom',
      description: 'Registry defaults; edit any input',
      values: {},
      locks: [],
    },
  };

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
    for (const c of preset.request_classes) add({ path: `request_classes.${c.id}.price_per_1000`, label: `Requests: ${c.ops.join(', ')}`, per: 'per 1,000 requests', value: c.price_per_1000, source_url: c.source_url });
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

  // Every validity domain in the contract: coefficients and registry rows. Returns [{owner, param, range}].
  function validityDomains(contract) {
    const out = [];
    for (const c of contract.coefficients) for (const [param, range] of Object.entries(c.validity || {})) out.push({ owner: `coefficient ${c.id}`, param, range });
    for (const q of contract.registry) for (const [param, range] of Object.entries(q.validity || {})) out.push({ owner: `registry ${q.id}`, param, range });
    return out;
  }

  // Problems that keep a scenario from being "within measured coverage": an id that is not a registry input, a lock
  // outside a precedence group, a value for a derived member, a value the pipeline rejects, or an effective value
  // (scenario over registry defaults, derived members included) outside a validity domain. A validity parameter
  // the pipeline does not resolve cannot be checked and is a problem too.
  function coverageProblems(contract, scenario) {
    const problems = [];
    const res = pipeline().resolveInputs(contract, scenario.values, scenario.locks, {});
    problems.push(...res.problems);
    const byId = new Map(contract.registry.map((q) => [q.id, q]));
    for (const id of scenario.locks) if (!byId.has(id)) problems.push(`lock ${id} is not a registry id`);
    for (const id of Object.keys(scenario.values)) {
      if (!byId.has(id)) problems.push(`value ${id} is not a registry id`);
      else if (res.fields[id] && res.fields[id].derived) problems.push(`value ${id} is set but derived from its locked partners`);
    }
    for (const d of validityDomains(contract)) {
      if (!res.inputs.has(d.param)) { problems.push(`${d.owner}: validity parameter ${d.param} has no value to check`); continue; }
      const v = res.inputs.get(d.param);
      if (typeof v !== 'number' || v < d.range[0] || v > d.range[1]) problems.push(`${d.owner}: ${d.param} = ${JSON.stringify(v)} is outside [${d.range[0]}, ${d.range[1]}]`);
    }
    return problems;
  }

  SCC.presets = { SCENARIOS, PRICES, priceLabel, priceFields, TRANSFER_LABELS, resolvePrice, coverageProblems, validityDomains };
  if (typeof module !== 'undefined' && module.exports) module.exports = SCC.presets;
})(typeof window !== 'undefined' ? window : globalThis);
