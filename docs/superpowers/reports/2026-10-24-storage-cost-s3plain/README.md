---
description: 'Operation graphs and capability matrix of the s3_plain_rewritable disk (R3) for the storage cost calculator.'
sidebar_label: 'Storage cost s3plain graphs (R3)'
sidebar_position: 56
slug: /superpowers/reports/storage-cost-s3plain
title: 'Storage cost calculator — s3_plain_rewritable graphs and capability matrix (R3)'
doc_type: 'reference'
---

# Storage cost calculator — s3_plain_rewritable graphs and capability matrix (R3) {#s3plain}

In progress: manifests and fit predictions are committed; measurements, coefficients and formulas follow.

## Capability matrix {#capability-matrix}

`s3_plain_rewritable` refuses mutations: a plain metadata storage reports no hard links
(`tmp/src-26.8/src/Disks/DiskObjectStorage/DiskObjectStorage.cpp:805`), so `checkMutationIsPossible` rejects
`ALTER UPDATE`, `ALTER DELETE` and lightweight `DELETE` (`tmp/src-26.8/src/Storages/MergeTree/MergeTreeData.cpp:6240`).
Every other cost operation is supported. The gate and its citations live in
`utils/cas-yardstick/yardstick/cost/cards/s3plain.py`.
