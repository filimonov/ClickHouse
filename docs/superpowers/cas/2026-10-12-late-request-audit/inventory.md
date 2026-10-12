# Номенклатура объектов CAS и риск опоздавшей записи {#cas-object-inventory}

База: `cas-janitor-budget` @ `d76b59f9295` (`altinity/antalya-26.6-20261008`). Серия аренды: `3b4037ae265`.
`CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`; пути в таблице — от `CA/`.
Метки: **V** — прослежено в коде базы; **I** — вывод (код не исполнялся, тестов нет).
Зомби = пауза процесса после истечения аренды, сетевой разрыв, позднее применение запроса хранилищем,
повтор SDK того же запроса, второй экземпляр того же `srid`, устаревшее кешированное чтение.

Обозначения защиты:

| Код | Что значит |
|---|---|
| `INM` | условный create `If-None-Match: *` |
| `IM(e)` | условная замена/удаление `If-Match` прочитанного токена `e` |
| `RMW` | `CasOperation::readModifyWrite`: чтение → `decide` → `INM`/`IM`; на конфликте `decide` заново по свежему телу |
| `RMWP` | `readModifyWriteOnPresence`: то же, но по `HEAD`, `decide` не видит тела |
| `RC` | `removeCurrent`: `HEAD` → `IM`-удаление → повтор на mismatch (по смыслу слепое удаление) |
| `WOD` | `removeManyWriteOnce`: пакетное удаление без условия (`DeleteObjects`), ключи типа `WriteOnceKey` |
| `blind PUT` | безусловная запись (`IObjectStorage::writeObject` / `copyObject` с `WriteSettings{}`) |
| `blind DEL` | `removeObjectIfExists` без условия |
| `T1` | допуск по остатку аренды: `CasMountRuntime::admit` (`Pool/CasMountRuntime.cpp:139`), резерв `reservedFor(0,2)` на запись (`Backend/CasRequests.cpp:914`), `reservedFor(0,1)` на чтение/удаление |
| `T3` | фенс GC по стабильности токена `L + L/20 + period` (`computeHeartbeatFloor`, `mountObservationThresholdMs`) |
| `T4` | ожидание рестарта `claimMountAwaitingExpiry` по стабильности токена |
| `T5` | `cas_unsafe_remount_no_delay` |
| `GC-int` | интервал GC между condemn и delete (раунды) |
| open | GC-плоскость `gc_requests`: отказ только при teardown (`Pool/CasPool.cpp:197`) |
| `SDK` | профиль повторов S3 по умолчанию: `Client::doRequest` повторяет тот же запрос до `max_retries+1` раз (`src/IO/S3/Client.cpp:808-890`); `WriteBufferFromS3` и `copyS3File` — ещё свой цикл `max_unexpected_write_error_retries` (`src/IO/WriteBufferFromS3.cpp:677,778`, `src/IO/S3/copyS3File.cpp:201,485,721`) |

Профили запросов (V): условные записи Native идут `SingleAttempt` с `s3_max_unexpected_write_error_retries_override = 1`
(`Backend/CasObjectStorageBackend.cpp:conditionalWriteSettings`); `read/head/list/remove/removeManyWriteOnce` —
`SingleAttempt` на записываемом Native-монтировании (`ContentAddressedMetadataStorage.cpp:822`). **`publish` блоба
(PUT и copy) идёт с `WriteSettings{}` — профиль по умолчанию, SDK повторяет тот же конверт**
(`Backend/CasObjectStorageBackend.cpp:ObjectStorageBackend::publish`). Движок `CasRequests` publish не повторяет
(`publication_policy = policy.asSingleAttempt()`, `Pool/CasPartWriteTxn.cpp:285`). Staging пишется и чистится мимо
`CasRequests` напрямую через `IObjectStorage` (`ContentAddressedTransaction.cpp:902,199`, `Pool/CasServerRoot.cpp:1812`).

## Сводка {#summary}

- Видов объектов: **22** (блоб, `.meta`, манифест, ref-log, `EpochSeal` как вид записи ref-log, снимок, `_ckpt`,
  `_files/`, `roots/`, `cas/ref_catalog`, `gc/state`, `gc/hb`, `gc/maintenance_state`, `fold_seal`, `blob_target`-runs,
  `outcomes`, `mount`, `epoch`, `owner`, `staging/`, `_pool_meta`, `_probe/`). Плюс 6 строк без объекта (протокол/чтения).
- Строк в таблице: **101**. Защита store-enforced или вред отсутствует: 77. Защита — время или ничего: **24 строки** (выделены жирным), сведены в 15 тем U1–U15.
- Непокрытых мест вызова: 0.

### Небезопасное множество (защита = время или ничего) {#unsafe-set}

Каждый пункт — отдельная тема исследования.

| Тема | Строки | Объект и опасность |
|---|---|---|
| **U1** | B9 | Блоб `blobs/<algo>/S/<hex>`: повтор SDK того же конверта (PUT/multipart/copy) восстанавливает старый content-ETag; поздний `IM`-delete GC с этим токеном удаляет живой блоб → висячая ссылка. Только ETag-диалект; GCS generation не повторяется |
| **U2** | M3 | `.meta`: писатель `reconcileMetaClean` через `RMW` пишет `Clean` поверх любого `Condemned` (Z6) → новый писатель принимает осуждённую инкарнацию, GC удаляет её → висячая ссылка. Защита: T1 + GC-int |
| **U3** | P1, P2 | `cas/ns/state/<life>/_files/<name>`: ключ общий у зомби и преемника того же `srid` (жизнь та же), запись `RMWP` по смыслу слепая, удаление `RC` слепое (Z5) → потерянное обновление / воскрешение файла таблицы (`mutation_*.txt`, `format_version.txt`). Защита: только T1 |
| **U4** | R1, R2 | `roots/<srid>/<path>`: то же для loose-файлов диска (Z5) |
| **U5** | C6 | `cas/ref_catalog`, переход `beginRemoving` `Live→Removing`: `decide` сравнивает строку по значению, строка не меняется, когда преемник пишет в таблицу (Z7) → поздний `DROP` зомби над живой таблицей преемника, строка застревает в `Removing`. Защита: T1 внутри `mutate` |
| **U6** | C4, C5 | `cas/ref_catalog`, строка `Creating`: `reconcileStaleCreator` и `cancelStalledCreating` решают по `isCreatorFenceTerminal` (gc_fenced / clean farewell) — сертификат смерти по часам T3 → перехват или удаление строки живого создателя |
| **U7** | MT3, MT4, MT8 | `gc/server-roots/<srid>/mount`: сертификат смерти держателя по часам (фенс-аут GC T3, reclaim после T4, T5). Сам объект защищён `IM`, но вывод «держатель не пишет» — по времени (G5) |
| **U8** | MT2 | `mount` на базе: `claimMount` при `existing.writer_epoch == our_epoch` переписывает слот (`seq+1`) — второй экземпляр с тем же `(uuid, epoch)` не фенсится (Z16). На `3b4037ae265` отказ |
| **U9** | ST4 | `staging/<srid>/`: `sweepOwnMountStaging` после claim — LIST + `blind DEL` мимо `CasRequests`, без фенса и без времени → зомби-старт удаляет staging преемника, его вставки падают (Z13) |
| **U10** | X1 | relink-confirm `Yes` из памяти, без I/O, по `mayMutate` (T1) → получатель промоутит по устаревшему ответу (Z8) |
| **U11** | X2, B7 | Чтения монтирования: манифесты (T1), ref-таблица в памяти, тела блобов через `readObject` без фенса (Z9) → устаревшие `SELECT`, ошибки чтения удалённых блобов |
| **U12** | L8 | Recovery зомби переходит через печать на/выше своего живого epoch (Z15, `runRecoveryWalkOnce`): применяет историю преемника к своему виду. Защита: T1 |
| **U13** | MT10, E2, E3, O3 (+ MT1, MT5, MF5, R4, ST5 как следствия) | Декомиссия: удаление `mount` и `epoch` по `IM`, затем tombstone `owner`; окно, в котором тот же `uuid` перевыпускает epoch 1; `DecommissionRecovery` решает по `expires_at_ms > now_ms`; задержанный `MountLeaseRenewer::claim` перевыпускает удалённый слот. Возможное следствие: `WOD` старого манифеста GC попадает в ключ, переиспользованный новым epoch 1 (I) |
| **U14** | X3 | Разрыв «допуск → отправка» для всех T1-записей: `gate` в `writeLoop` (`Backend/CasRequests.cpp:914`) до `backend->write` (`:948`) без перепроверки; плюс SDK-повторы `publish`. Остаток T1 не ограничивает момент применения |
| **U15** | X4 | Вне CAS: ReplicatedMergeTree-зомби с той же репликой в Keeper (Z17), фенс — таймаут сессии |

## Таблица номенклатуры {#nomenclature}

Колонки: id / объект и шаблон ключа / актор и функция / тип запроса / защита сегодня / риск опоздавшей записи
(сценарий → вред, V|I) / TLA+ (модель:оператор; «поздн.» = моделируется поздняя/зомби-запись) / отличие в серии.

### Блоб `POOL/blobs/<algo>/<S>/<hex>` {#blob}

Тело = конверт (`incarnation_tag` = случайные 128 бит на каждую физическую публикацию, `buildHeader` в
`Pool/CasPartWriteTxn.cpp:ensureBlobPresent`) + payload. ETag на S3 = MD5 тела, значит уникален на публикацию.

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| B1 | блоб | писатель (insert/merge/mutation/ALTER/relink-получатель): `Pool/CasPartWriteTxn.cpp:ensureBlobPresent` — обязательный `HEAD` до публикации | `HEAD` свежий, mount-план | T1; precommit `Durable` до HEAD (V) | зомби видит present+`Clean` → `Observed`, доказательство зависимости; его promote проигрывает печати → вреда нет (V порядок, I вред) | CaBlobPublishCore:ObserveBody, ReadyWithoutReobserve; CaEdgeBeforeObserve:Adopt | — |
| B2 | блоб | писатель: `ensureBlobPresent` → `op.publish(StreamingBlobPublication)` | `blind PUT` (single/multipart), `SDK` | свежий конверт на каждую итерацию цикла; GC удаляет только `IM(t)` (Z14) | поздний PUT зомби со свежим `t_Z` ложится поверх живого `t_B` → GC видит mismatch, `Replaced`, пере-осуждает текущий токен (`Gc::runRegularRound`, ~`CasGc.cpp:1030`) → утечка/ничего (V код GC, I сценарий) | CaBlobPublishCore:BackendLand, LateBackendLand (поздн.), CondemnedNeedsFreshPublication; CaIncarnationCore:WOverwrite | — |
| B3 | блоб | писатель: `ensureBlobPresent` → `VerbatimStagedBlobPublication` (только первая публикация после `Absent`, `beginPublication`) | `copyObject` NativeOnly, `blind`, `SDK` | конверт свежий на staging-объект, копия разрешена один раз | см. B9: повтор copy восстанавливает тот же ETag (V профиль, I сценарий) | CaBlobPublishCore:VerbatimCopyOnlyFirstAbsent, StagedRetagWitnessAction | — |
| B4 | блоб | GC fold: `Gc::fold` (`CasGc.cpp:2081`) `HEAD` для токена осуждения | `HEAD`, open | раунды; токен фиксируется в retired-записи | поздний HEAD не пишет | CaIncarnationCore:GFold, GRetire; CaGcShardIncarnationCore:GRetire | — |
| B5 | блоб | GC: `Gc::performRedeleteIo` (`CasGc.cpp:435`) после публикации `delete_pending` прошлым раундом | `HEAD` + `IM(t_retired)` | exact-token; токен меняется при каждой публикации | поздний delete свергнутого лидера: токен совпадает только с той же инкарнацией → безвредно, кроме ABA B9 (V) | CaBlobPublishCore:GCDelete, DeleteLands, ExactDeleteCannotRemoveFreshIncarnation; CaIncarnationCore:GRecheckDelete; CaRetiredInRun:Settle | — |
| B6 | блоб | инструменты: `Tools/CasFsck.cpp` (HEAD `:756,961,1060`), `Gc::previewDeletes` (`CasGc.cpp:4855`), `Gc::rebuildBaseline` (`:4344`) | `HEAD` | — (чтение) | нет записи | нет | — |
| **B7** | блоб | чтение данных: `ContentAddressedMetadataStorage.cpp:2172`, `ContentAddressedTransaction.cpp:644` | `readObject` мимо `CasRequests` | нет фенса | зомби читает блоб, удалённый GC → ошибка чтения (Z9, I) → U11 | нет | — |
| B8 | блоб | ABA при `IM`-delete из-за повторной публикации тех же байтов писателем | — | `incarnation_tag` случайный → ETag не повторяется (V) | не возникает, кроме B9 | CaBlobPublishCore:ETagToken, MintToken | — |
| **B9** | блоб | слой S3 под `publish`: `Client::doRequest`, `WriteBufferFromS3::completeMultipartUpload`, `copyS3File` | повтор того же PUT/Complete/Copy | **нет**: свежий конверт только на внешний вызов | upload A применён, ответ потерян → GC раунды: condemn A, graduate, `delete_pending(t_A)`; delete задержан у лидера 1; лидер 2 удаляет A; писатель публикует B; SDK повторяет A (или поздно применённый A) → тело снова `t_A`, поверх B; задержанный `IM(t_A)` лидера 1 удаляет живой блоб → висячая ссылка (V профиль, I сценарий; codex) | CaBlobPublishCore:LateBackendLand с `reproducesQueuedToken` — частично; повтор того же конверта после GC-удаления — нет | — |

### Маркер свежести `.meta` `POOL/blobs/<algo>/<S>/<hex>.meta` {#meta}

Тело `{state, condemn_round, size}` без инкарнации. `Clean{0,size}` — одни и те же байты для любой инкарнации блоба
(ABA по ETag — норма). `Condemned{r}` уникален по раунду, раунды монотонны.

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| M1 | `.meta` | писатель: `reconcileMetaClean` после публикации из `Absent` (`Pool/CasPartWriteTxn.cpp:354`) | `INM` `Clean` | store | поздний create ложится только над отсутствием → `Clean` без тела или над удалённым маркером; следующий писатель сначала делает HEAD тела → безвредно (I) | CaGcCondemnMarkerGate:WPublish, WReconcile | — |
| M2 | `.meta` | писатель: `reconcileMetaClean` после воскрешения (`:359`) | `IM(e_Condemned{r})` | store: байты с раундом | поздний replace ложится, только если маркер всё ещё `Condemned{r}` той же инкарнации, которую писатель уже перепубликовал → корректно (V механизм) | CaGcCondemnMarkerGate:WReconcile | — |
| **M3** | `.meta` | писатель: fallback `RMW` в `reconcileMetaClean` (`:370`) | `RMW`: `decide` пишет `Clean` поверх **любого** не-`Clean` | **T1 + GC-int** | писатель-зомби (precommit снят sweep-ом преемника) или aborted-писатель: GC осуждает новую инкарнацию `Condemned{r2}`, читает его для graduation, затем `Clean` писателя затирает маркер; новый писатель B видит `Clean`, принимает; GC удаляет `IM(t)` → висячая ссылка (Z6; V код, I сценарий для зомби) | CaGcCondemnMarkerGate:WLateLand (поздн.), WReconcile; CaRefWriterCleanupCore:SuccessorCleanupStep; спека 2026-09-30 §10 | — |
| M4 | `.meta` | писатель: adopt-backfill `putMetaIfAbsent` при present-теле и отсутствующем маркере (`ensureBlobPresent`) | `INM` `Clean` | store | поздний create после `deleteConfirmedMeta` → `Clean` над отсутствующим телом; писатель идёт от HEAD тела → безвредно (I) | CaGcCondemnMarkerGate:WAdopt | — |
| M5 | `.meta` | писатель: `loadMeta` (`Pool/CasBlobMeta.cpp:25`) | `GET` свежий | — | устаревшее чтение даёт `Clean` → адопция; см. M3/CaRetiredInRun | CaRetiredInRun:WriterStaleReuse | — |
| M6 | `.meta` | GC: `writeCondemnedMeta` (`Gc/CasGcMetaWriter.cpp`), пул `GcMetaWriter`, open | `GET` + `INM` или `IM(e)` поверх `Clean`/старшего `Condemned` | store; раунд в теле | поздний `Condemned` свергнутого лидера: `IM(e_Clean)` совпадает с **любым** `Clean` (ABA) → лишняя перепубликация, не потеря (G4; V) | CaGcCondemnMarkerGate:GMarkerJob, GStaleMarkerWrite (поздн.) | — |
| M7 | `.meta` | GC: `deleteConfirmedMeta` (`Gc/CasGcMetaWriter.cpp`) | `GET` + `IM(e)`, только `Condemned{≤r}` | store: раунд + токен | поздний delete по `Condemned{r}` той же инкарнации; новая инкарнация осуждается большим раундом → не трогается (V; codex) | CaGcCondemnMarkerGate:GMetaDeleteJob | — |
| M8 | `.meta` | GC fold: чтение маркера для graduation (`Gc/CasGcReadAhead.cpp`) | `GET`/`HEAD` | раунд чтения | устаревшее чтение → graduation по старому `Condemned` → M3 | CaGcCondemnMarkerGate:GObserveMeta, GSettleGraduate | — |

### Манифест части `POOL/cas/manifests/<ns>/<E-hex>-<b-hex>/<ord>.zst` {#manifest}

Ключ содержит `writer_epoch` и монотонные `build_seq`, `ordinal` (`stageManifest`); тело детерминировано.

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| MF1 | манифест | писатель: `PartWriteTxn::stageManifest` → `CasRefLedger::stagingPutIfAbsent` (`Pool/CasRefLedger.cpp:250`), mount-план | `INM`, движок повторяет | store: epoch в ключе; тело до precommit (V: `stageManifest` → `precommitAdd` → `ensureBlobPresent` требует `Durable`) | поздний PUT после orphan sweep → снова тело без владельца → debris (Z2; V) | CaGcRootLocalPartManifestCore:Land, BodyAccepted, NoManifestIdReuse | — |
| MF2 | манифест | писатель: `PartWriteTxn::promote` чтение тела (`Pool/CasPartWriteTxn.cpp:828`) | `GET` | — | — | CaGcRootLocalPartManifestCore:CommittedManifestBodyRequired | — |
| MF3 | манифест | писатель: `cleanupStagedManifestDebrisBestEffort` (`:1208`) | `HEAD` + `IM` | store; свой ключ; пропускает precommit-тело | поздний delete своего не-precommit тела → безвредно (V) | CaGcRootLocalPartManifestCore:GOrphanSweep (обобщённо) | — |
| MF4 | манифест | GC orphan sweep: номинации в `Gc::runRegularRound` (`CasGc.cpp:1374`) после `gc/state` CAS | `HEAD` + `IM(t_nominated)` | store + RULE 1 `manifestDeletionPremise` (печать в строго большем epoch, свёрнутая GC) | зомби-писатель precommit-ит после удаления → проигрывает печати; поздний delete лидера: тот же детерминированный ETag → то же тело (V) | CaRefFoldClampRecoveryCore:DeleteNominatedManifest, PostCasDelete; CaGcRootLocalPartManifestCore:GOrphanSweep | — |
| MF5 | манифест | GC: удаление owner-removed тел (`CasGc.cpp:1332`) после принятых декрементов | `WOD` | ключ write-once; владение необратимо (precommit A3: чужой id не принимается, `precommitAdd`) | поздний `WOD` свергнутого лидера по ключу, который никто не пересоздаёт → безвредно; исключение — перевыпуск epoch (U13, I) | CaGcRootLocalPartManifestCore:GDeleteManifest, MonotoneGC | — |
| MF6 | манифест | декомиссия: `sweepNamespace` (`Gc/CasOrphanManifestSweep.cpp:560`) по листингу `cas/manifests/<srid>/` | LIST + `HEAD` + `IM` | store + watermark из `mount` (`prefixEligibleOn`) + RULE 1 | как MF4 (V) | CaGcRootLocalPartManifestCore:GOrphanSweep | `3b4037ae265`: на `drain_op` с проверкой потери claim |
| MF7 | манифест | чтения: `Pool/CasManifestReader.cpp:56` (mount), `Pool/CasKeyReader.h:34`, GC read-ahead, `Tools/CasFsck.cpp:645,923` | `GET` | T1 для mount-чтения | устаревших тел нет (неизменяемы) | нет | — |
| MF8 | манифест | GC: `planManifestCursorPage` LIST (`Gc/CasOrphanManifestSweep.cpp:696`), декомиссия LIST | LIST | — | — | нет | — |

### Ref-log `POOL/cas/ns/stream/<life>/_log/<E-seq>.zst` (включая `EpochSeal`) {#ref-log}

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| L1 | ref-log | писатель (insert/merge/mutation/ALTER/DROP/relink/precommit/abandon/stale-precommit sweep): `CasRefLedger::commitRefChunk` (`Pool/CasRefLedger.cpp:3731`) | `INM`, движок повторяет | store: плотные id + печать преемника (Z1) | поздний append ложится до печати → законная запись; после → 412, occupant чужой (V) | CaRefTableSnapshotLogCore:FrontierStartLog, FrontierOldWriterLosesToSeal (поздн.); CaRefLaneCore:ObserveOccupied, ForeignWrite; CaCasMountCore:StragglerLands (поздн.) | — |
| L2 | ref-log (`EpochSeal`) | recovery преемника: `runRecoveryWalkOnce` (`:1154`) | `INM`, `Retry::once` | store | конфликт → adopt seal или straggler `T+1` (V). Печать ниже checkpoint удаляема (`planRefCleanup`, V) → очень поздний append зомби в `(E,T+1)` после этого ложится; он ниже checkpoint → recovery его не читает, GC удалит → debris (codex Z1 PARTIAL; вред I) | CaRefTableSnapshotLogCore:FrontierSuccessorSealCreate, FrontierResolveLostSeal; CaCasMountCore:SealSlot; CaWriterEpochBackfillCore | — |
| L3 | ref-log | писатель: `resolveWedgeOnce` (`:2348`) — повтор заранее построенных байтов wedge | `INM`, `Retry::once` | store: свои байты = landed | — (V) | CaRefLaneCore:BeginResolve, ApplyResolution | — |
| L4 | ref-log, снимок | recovery: `runRecoveryWalkOnce` (`:967,1010,1092`), `Pool/CasRefProtocol.cpp:885,971,1007,1028,1077`; fsck `:379` | `GET` | неизменяемость | — | CaRefTableSnapshotLogCore:Reconstruct | — |
| L5 | ref-log, снимок | GC: `Gc::cleanupRefObjects` (`CasGc.cpp:3764`) | `WOD` чанками; перед чанком перечитывает catalog etag и `gc/state` lease | store: write-once + `planRefCleanup` по checkpoint-тройке | поздний `WOD` свергнутого лидера по ключам ниже checkpoint — история необратима; пересоздание только debris (L2) → безвредно (V механизм) | CaRefTableSnapshotLogCore:GcCleanupLog, GcCleanupSnap, CleanupLogGate; поздний append после удаления печати — нет | — |
| L6 | ref-log, снимок, `_ckpt`, `_files` мёртвой жизни | GC janitor: `NamespaceJanitor::runOnePage` (`Gc/CasNamespaceJanitor.cpp:22`) | LIST `cas/ns/` + `HEAD` + `IM` | store: жизнь вне каталога, `incarnation` случайна и не переиспользуется | поздний delete по мёртвой жизни → безвредно; поздняя запись зомби в мёртвую жизнь → debris до следующего прохода (V) | CaRefNsCleanupStaleLeaderCore:StaleJanitorDelete (поздн.); CaNamespaceJanitorCursorCore:AuthorizedFoldPage | — |
| L7 | ref-log | GC: `Gc::enumerateRefPrefix` LIST (`CasGc.cpp:4185`); orphan sweep чтение курсора `activeManifestKeys` (`Gc/CasOrphanManifestSweep.cpp:262`); rebuild LIST (`:4462`) | LIST/`GET` | — | LIST не доверяется для удаления (V `planRefCleanup` по checkpoint) | CaRefDeltaIntakeCore:WalkStep, HideLog | — |
| **L8** | ref-таблица (вид) | recovery зомби: `runRecoveryWalkOnce` переходит через печать на/выше своего живого epoch (комментарий «including when that epoch is at or above our own live one») | `GET` | **T1** | зомби применяет печать и записи преемника к своему виду, дальше не определено (Z15; V механизм, I вред) | нет | — |
| L9 | ref-log | второй экземпляр, тот же `(uuid, epoch)`: оба пишут одни `(E,seq)` | `INM` | store ловит чужого occupant → fault | два писателя одной полосы → `CORRUPTED_DATA`/fault, не тихая порча (Z16; I) | нет | см. MT2 |

### Снимок `.../_snap/<E-seq>.zst` и `_ckpt` `POOL/cas/ns/state/<life>/_ckpt` {#snapshot-ckpt}

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| S1 | снимок | писатель: `tryPublishSnapshotAndAdvanceCheckpointOnceOnRuntimeImpl` (`Pool/CasRefLedger.cpp:4544`) | `INM`, байты = состояние на id | store: ключ = последний лог, зомби пишет только свои `(E,x)` | поздний снимок зомби согласован с его журналом; без продвижения `_ckpt` не авторитетен → debris (Z4; V) | CaRefTableSnapshotLogCore:FrontierPublishSnapshotBody, FrontierSnapshotAboveCheckpoint | — |
| S2 | снимок | GC: удаление старше checkpoint — см. L5 | `WOD` | store | см. L5 | CaRefTableSnapshotLogCore:GcCleanupSnap | — |
| K1 | `_ckpt` | писатель (снимок, recovery frontier `publish_recovered_frontier`, рождение `completeCreation`): `publishCkpt` (`Pool/CasRefCkpt.cpp:195`) | `RMW` с по-полевым максимумом | store: монотонное слияние, identical-skip, отказ на уменьшение `life_epoch` | поздний `IM(e)` ложится, только если тело не менялось; печать преемника меняет `_ckpt` → 412; ABA невозможна (Z3; V) | CaRefTableSnapshotLogCore:CkptMerge, FrontierIssueCheckpoint, FrontierStaleAdvance (поздн.) | — |
| K2 | `_ckpt` | чтения: `readCkpt` (recovery, GC fold, orphan sweep), декомиссия `HEAD` (`Tools/CasDecommission.cpp:237`) | `GET`/`HEAD` | — | — | CaRefTableSnapshotLogCore:FrontierReadCheckpoint | — |
| K3 | `_ckpt` | удаление мёртвой жизни — см. L6 | `IM` | store | см. L6 | CaRefNsCleanupStaleLeaderCore | — |

### Файл таблицы `POOL/cas/ns/state/<life>/_files/<name>` {#namespace-files}

Жизнь берётся из каталога при открытии буфера; рестарт того же `srid` её не меняет (V `writeFile`).

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| **P1** | `_files` | писатель: `ContentAddressedTransaction::writeFile` (Rewrite/Append, `:851`), `moveFile` (`:1483`), `moveDirectory` (`:1301`) → `CasPlainObjects::casPutObject` | `RMWP`: `decide` всегда возвращает те же байты | **T1** | зомби отправил PUT `mutation_N.txt` (или CSN-append с прочитанным заранее префиксом) → преемник переписал файл → PUT зомби ложится → потерянное обновление / откат CSN (Z5; V механизм, I вред) | нет (обобщённо CaMountObserveCore:Write, NoWriteAfterFence; CaCasMountCoreLateRenew:WriteAfterRenew) | — |
| **P2** | `_files` | писатель: `removeFile` (`:1664`), `moveFile` (`:1485`), `removeRecursive` (`:1166`) → `casRemoveObject` | `RC` | **T1** | поздний DELETE зомби удаляет файл, записанный преемником → потеря файла таблицы (Z5; I) | нет | — |
| P3 | `_files` | чтения/LIST: `getNamespaceFile`, `listNamespaceFiles` (`Pool/CasPlainObjects.cpp:29,59`) | `GET`/LIST, mount-план | T1 | устаревший префикс для Append → входит в P1 | нет | — |
| P4 | `_files` мёртвой жизни | janitor — см. L6 | `IM` | store | debris | CaRefCatalogCore:OrphanWrite (поздн.), Janitor | — |

### Loose-объект `POOL/roots/<srid>/<path>` {#roots}

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| **R1** | `roots/` | писатель: `writeFile` для не-табличного пути (`ContentAddressedTransaction.cpp:863`), `moveFile` (`:1507`), startup write probe | `RMWP`, байты фиксированы | **T1** | как P1, ключ по пути общий у всех экземпляров `srid` (Z5; I) | нет | — |
| **R2** | `roots/` | писатель: `removeFile` (`:1678`), `moveFile` (`:1508`) | `RC` | **T1** | как P2 (Z5; I) | нет | — |
| R3 | `roots/` | чтения: `getMountpointObject`, `mountpointObjectExists`, `Pool::listMirroredChildren` (`Pool/CasPool.cpp:1926`), `serverRootSubtreeEmpty` → `prefixHasAnyKey` (`Pool/CasServerRoot.cpp:62`) | `GET`/`HEAD`/LIST | T1 / bootstrap | пустота subtree решает перевыпуск epoch → U13 | нет | — |
| R4 | `roots/<srid>/` | декомиссия: `deleteListedPrefix` (`Tools/CasDecommission.cpp:63`) | LIST + `IM` | store + claim декомиссии (T3/T4) | поздняя запись жертвы после дренажа → воскрешение loose-файла (I) → U13 | нет | `3b4037ae265`: `drain_op` и `throwIfClaimLost` |

### Каталог `POOL/cas/ref_catalog` {#catalog}

Один объект на пул; любая запись меняет ETag. Записи монтирования идут через hot-key lane
(`Backend/CasHotKeys.cpp:submit`: `IM` от кешированного тела, на конфликте — `decide` по свежему).

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| C1 | каталог | bootstrap: `CasRefCatalog::initializeEmptyForNewPool` (`Pool/CasRefCatalog.cpp:72`) из `Pool::open` | `INM` | store | поздний create над существующим → 412 | CaRefCatalogCore:Init | — |
| C2 | каталог | писатель: `createNamespace` → `createNamespaceStep1` (вставка `Creating` с новой случайной `incarnation` и `creator`) | `RMW` (hot keys), `decide` перерешает | store: `IM` всего каталога; `decide` проверяет отсутствие строки | поздний PUT ложится только над неизменённым каталогом (V) | CaRefCatalogCore:Create, NewInc | — |
| C3 | каталог | писатель: `completeCreation` (`Creating→Live`, после `_ckpt` рождения) | `RMW`, `decide` сравнивает строку по значению + `op.admitted()` | store: строка меняется при перехвате создателя (`creator`) | поздний go-live зомби после `reconcileStaleCreator` → строка другая → mismatch (V) | CaRefCatalogCore:GoLive, ZombieGoLive (поздн.) | — |
| **C4** | каталог | писатель-преемник: `reconcileStaleCreator` (`Pool/CasRefLedger.cpp:1382`) | `RMW` по значению | **T3**: `isCreatorFenceTerminal` (`Pool/CasServerRoot.cpp:1258`) — gc_fenced или clean farewell | ложный фенс живого создателя → его строка переходит к преемнику; дальше создатель пишет `_ckpt`/логи жизни, которую ведёт другой (I) | CaRefCatalogCore:ReconcileCreating, ReconcileObserve, CreatorDies; INV_RECONCILE_SAFE | — |
| **C5** | каталог | писатель: `cancelStalledCreating` (`Pool/CasRefLedger.cpp:5117`) | `RMW` по значению, удаляет строку | **T3** (тот же сертификат) | удаление строки живого создателя → его объекты становятся мёртвой жизнью, janitor удаляет (I) | CaRefCatalogCore:EntryDelete, OrphanWrite (поздн.), WITNESS_ORPHAN_EATEN | — |
| **C6** | каталог | писатель DROP: `beginRemoving` (`Pool/CasRefCatalog.cpp:382`) из `dropNamespaceImpl` (`Pool/CasRefLedger.cpp:5204`) | `RMW`, `decide` по значению строки + `op.admitted()` | **T1** | строка `Live` не меняется, когда преемник пишет в таблицу → поздний `DROP` зомби ставит `Removing` над живой таблицей; терминальный `RemoveNamespace` зомби проигрывает печати → строка застревает (Z7; V механизм, I вред) | CaRefCatalogCore:Drop, ObserveRemoving; поздн. — нет | — |
| C7 | каталог | GC drain: `deleteCompletedRemovingAtSnapshot` (`Pool/CasRefCatalog.cpp:452`) из `Gc/CatalogLifecycleReconciler.cpp:93` | `IM(e_snapshot)` + `refresh_authority` + `cleanup_evidence` из fold seal | store | поздний erase свергнутого лидера ложится только над тем же каталогом (G7; V) | CaRefPreFoldDrainCore:DeleteSuccess, AdoptHeldOverDeposedRequest (поздн.), DeleteUnknownLanded; CaRefPreFoldDrainAllRowsCore:ExactCatalogCAS | — |
| C8 | каталог | чтения: `CasRefCatalog::read` (GC fold, janitor, cleanup authority, orphan sweep, декомиссия, `Backend/CasSentinelProbe.cpp:118`), кеш hot keys | `GET`; кеш — подсказка | решение «не писать» по кешу перечитывается (V `CasHotKeys::submit`) | устаревший кеш → `IM` со старым etag → 412 | CaRefCatalogCore:StaleAfterWrite | `3b4037ae265`: typed catalog observation (`7d26dba`) |

### GC-управление `POOL/gc/*` {#gc-control}

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| GS1 | `gc/state` | GC: `Gc::acquireOrRenewLease` (`CasGc.cpp:4949`) | `RMW` (`seq+1`; steal после двух наблюдений `(owner,seq)` и `gc/hb`) | store; исключительность лидера — по времени, но только живучесть (T10) | поздний renew свергнутого → 412 | CaGcLeaseCore:Create, Renew, ObserveOrSteal, NoFalseSteal | — |
| GS2 | `gc/state` | GC: коммит раунда `runRegularRound` (`CasGc.cpp:1164`) | `IM(state_etag)` | store; деструктивное до CAS — только по ранее опубликованному | раунд свергнутого испаряется (G1 отчёта; V) | CaGcRootLocalPartManifestCore:GAdopt; CaRetiredInRunFoldAbortWitness:LeaderAdopt | — |
| GS3 | `gc/state` | оператор: `Gc::rebuildBaseline` (`CasGc.cpp:4793`) | `IM` | store | конкурирующий писатель → «re-run» | CaGcAckFloorCore:GRebuild | — |
| GS4 | `gc/state` | чтения: `refreshAuthority` (`:4916`), cleanup authority (`:3824`), orphan sweep (`Gc/CasOrphanManifestSweep.cpp:102`), `Pool::currentGcRound` (`Pool/CasPool.cpp:1844`), `dropNamespaceImpl` (`Pool/CasRefLedger.cpp:5201`), fsck | `GET` | — | устаревший раунд в `removal_started_round` → только задержка | нет | — |
| H1 | `gc/hb` | GC: `Gc::pulseHeartbeat` (`CasGc.cpp:4930`) | `GET` + `IM`/`INM`, `Retry::once` | store; `hb_seq` монотонен | поздний пульс свергнутого → контендер видит «жив» → не крадёт → живучесть (V; codex) | CaErasureProof:GcHbPulse; CaGcLeaseCore:ObserveOrSteal | — |
| H2 | `gc/hb` | GC-контендер: чтение в `acquireOrRenewLease` (`:4993`) | `GET` | — | — | CaGcLeaseCore | — |
| MS1 | `gc/maintenance_state` | janitor: `casGcMaintenanceState` (`Gc/CasGcMaintenanceState.cpp:37`) — курсор, сброс при порче/ошибке LIST | `IM`/`INM` | store; курсор leak-only | поздний курсор → пропуск или повтор страницы → утечка до круга (I; codex) | CaNamespaceJanitorCursorCore:AuthorizedFoldPage, DeferPage | — |
| MS2 | `gc/maintenance_state` | чтение `readGcMaintenanceState` (`:14`) | `GET` | — | — | — | — |
| A1 | `gc/gen/<g>/attempt/<a>/blob_target/<sh>/<seq>` | GC fold: `putDeterministicArtifact` (`Gc/CasBlobInDegree.cpp:406,766`); `a = state.lease.seq` (`CasGc.cpp:2128`) | `INM` + byte-adopt | store: ключ по attempt уникален на лидера | артефакты свергнутого — в своём attempt, невидимы (G2; V) | CaGcRootLocalPartManifestCore:GMintAttempt, GDeposedFinalWrite (поздн.) | — |
| A2 | `.../fold_seal` | GC fold (`CasGc.cpp:3716`) | `INM` + byte-adopt | store | как A1 | CaGcRootLocalPartManifestCore:GCoordSeal; CaB140DangleMerge:GCommit | — |
| A3 | `.../outcomes/<round>/<sh>.zst` | GC: `runRegularRound` (`CasGc.cpp:1051`) | `INM` + byte-adopt/merge | store | как A1 | CaRetiredInRun:Settle | — |
| A4 | `fold_seal` (rebuild) | оператор: `rebuildBaseline` (`CasGc.cpp:4782`), поколение выше любого существующего | `INM` | store | — | CaGcAckFloorCore:GRebuild | — |
| A5 | `gc/gen/<g>/` | GC: `pruneSupersededGenerations` → `deletePrefixWholesale` (`CasGc.cpp:3942`) до CAS | LIST + `IM(listed etag)` | store + `referenced_generations` из ранее опубликованного | prune свергнутого лидера: его порог ниже порога живого, поколения живого `> G` не трогает; поздние артефакты зомби в удалённое поколение → утечка навсегда (курсор монотонен) (G6; I) | CaB140DangleMerge:GTrim; CaGcAckFloorZombie (поздн.) | — |
| A6 | `gc/gen/*` | чтения: `readFoldSeal` (`:4083`), `newestFoldSealRef` LIST (`:1651`), `probeGenerationForSeal` (`:1775`), stream runs (`Gc/CasBlobInDegree.cpp:352`), fsck | `GET`/LIST/stream | — | — | нет | — |

### Плоскость монтирования `POOL/gc/server-roots/<srid>/{mount,epoch,owner}` {#mount-plane}

`mount` = `{uuid, epoch, seq, started_at, expires_at, min_active_build_sequence, gc_fenced, write_attempt_id}`;
`seq` растёт на каждой записи → ABA нет. Наблюдатели: GC (`computeHeartbeatFloor`) и рестарт (`claimMountAwaitingExpiry`).

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| MT1 | `mount` | старт: `claimMount` при отсутствии (`Pool/CasServerRoot.cpp:787`) | `INM` | store | поздний create после удаления декомиссией → перевыпуск слота (U13, I) | CaCasMountCore:ClaimMount, ClaimAttempt | одна условная запись, renewer стартует от неё (`56d876f`, `dabbfab`) |
| **MT2** | `mount` | старт/self-remount: `claimMount` при `existing.writer_epoch == our_epoch` | `IM(e)` `seq+1` | **нет** | второй экземпляр с тем же `(uuid, epoch)` переписывает слот; оба считают себя держателем до первого конфликта renew (Z16; V код, I вред) | CaCasMountCore:ClaimEqualOverOther, OneWritableIncarnationPerUuid | **убрано**: «same_epoch_occupied» → отказ; старший epoch в слоте → `ABORTED` (`17c7f28`) |
| **MT3** | `mount` | старт: `claimMount` reclaim при `gc_fenced`/clean farewell/`proven_dead`/`unsafe` | `IM(e_observed)` | store для объекта; сертификат смерти — **T3/T4/T5** | ложная смерть → преемник пишет, пока зомби ещё пишет классы U3–U5 (I) | CaCasMountCore:ObservedReclaim, ClaimOwnExpired; CaMountObserveCore:NoFenceInsideAuthority | reacquire GC-фенсенного слота по сертификату (`ab96ea8`, `e38f56b`, `c6e0847`); self-remount удалён (`9a6bee1`) |
| **MT4** | `mount` | старт: `claimMountAwaitingExpiry` (`:940`) — наблюдение стабильности токена | `GET` + T4 | **T4** | см. MT3 | CaMountObserveCore:ObsSample, ObsDecide, FencingWaitCoversDeclaredLease | формула порога та же (`L + L/20 + period`); держатель со старшим epoch → отказ |
| MT5 | `mount` | renewer: `MountLeaseRenewer::claim` из `start` (`:1368`) — mint при отсутствии или adopt своего | `GET` + `INM`/`IM` | store; при чужом epoch/uuid — отказ | задержанный старт после удаления слота декомиссией → mint слота зомби (U13, I) | CaCasMountCore:AdoptRead, AdoptWrite | **убрано**: renewer стартует от тела claim, слот не переписывает (`56d876f`) |
| MT6 | `mount` | renewer: `renew` (`:1623`), `Retry::untilLeaseSafe` | `IM(own)` | store (Z10) | поздний renew после фенса/claim преемника → 412 (V) | CaMountRenewRetryCore:LandGhost, LateDeliveryRefused, LateDeliveryCannotOverwriteSuccessor (поздн.) | renew до однозначного ответа, без дедлайна (`75c46ce`, `2eba892`); отсутствие слота — только с согласия LIST (`1d30083`); потеря слота → stop сервера (`794f604`) |
| MT7 | `mount` | farewell: `MountLeaseRenewer::terminate` (`:1727`) на `farewell_requests` | `IM(own)`, `Retry::untilLeaseSafe` + окно | store (Z11) + правило «нет неясных запросов» (состояние) + на базе ещё дедлайн аренды | поздний farewell над чужим → 412; farewell = сертификат мгновенного reclaim, верен, пока правило полно (I) | CaDiskLifecycle:FIntent, I2EarnedFarewell, NoLandingAfterFarewell | пишется и после истечения аренды, `Retry::within` (`4fc32d9`, `a18ad0d`); удерживается при publish снимка (`592d89a`); план `lease_requests` (`6eca7a1`) |
| **MT8** | `mount` | GC: `computeHeartbeatFloor` (`:1025`) фенс-аут | `RMW` → `gc_fenced=true`, `seq+1` | store для объекта; решение — **T3** | поздний фенс-аут ложится, только если токен не менялся; ложный фенс живого → U7 (G5; V механизм) | CaMountObserveCore:ObsFenceWrite, NoFenceInsideAuthority; CaCasMountCore:GcFence | без изменений (функция идентична) |
| MT9 | `mount` | чтения: `listMounts` (`:1190`), `probeNonTerminalMountSlots` (`:1140`), `isCreatorFenceTerminal` (`:1258`), `floorForNamespace` (`Gc/CasOrphanManifestSweep.cpp:48`), `Pool::openForDecommission` (`Pool/CasPool.cpp:940`), `allocateWriterEpoch` `probeSentinel` (`:642`) | `GET`/LIST/`probeSentinel` | — | устаревший watermark в orphan sweep → только задержка (RULE 1 главнее) | CaCasMountCore:RenewGetMiss, RenewListStep | lifecycle по часам аренды, локальная строка без I/O (`9c412e2`) |
| **MT10** | `mount` | декомиссия: `deleteSlotObject` (`Tools/CasDecommission.cpp:103`) по захваченному farewell | `IM(e_farewell)` | store + claim декомиссии (T3/T4) | после удаления — MT1/MT5 зомби перевыпускает слот (U13, I) | CaCasMountCore:ClearExpiredMount | `throwIfClaimLost` перед каждым шагом (`eee465d`) |
| E1 | `epoch` | старт: `allocateWriterEpoch` (`:598`) | `RMW` `{N+1}` | store: монотонен, ABA нет (Z12) | поздний `IM(N)` → 412 (V) | CaCasMountCore:AllocEpoch, AllocAttempt, AllocResolve, ClaimNeverRegressesEpoch | одна неизменяемая условная запись (`f95950c`, `c4d5f2f`); не пересоздаётся в self-remount (`ef8a1c5`); `next_writer_epoch = 0` → `CORRUPTED_DATA` |
| **E2** | `epoch` | старт/декомиссия: отсутствующий счётчик → mint при пустом subtree и `probeSentinel(mount) = KeyAbsent`; `DecommissionRecovery` — по `!gc_fenced && expires_at_ms > now_ms` | `INM` | пустота subtree (`serverRootSubtreeEmpty`) + **wall-clock** для декомиссии | после удаления `epoch`/`mount` декомиссией и до tombstone `owner` тот же `uuid` перевыпускает epoch 1 (codex; V код, I сценарий) | CaCasMountCore:RemintEpoch, RemintEpochDecom, SabResetEpoch | логика та же (`decideAbsentCounter`) |
| **E3** | `epoch` | декомиссия: `deleteSlotObject` (`Tools/CasDecommission.cpp`) | `IM(e_claimed)` | store + claim | открывает окно E2 (I) | нет | `throwIfClaimLost` |
| O1 | `owner` | старт: `claimOwnerOrThrow` (`:525`) — create только над пустым subtree | `INM` | store + пустота subtree | поздний create над существующим → 412 | CaCasMountCore:ClaimOwnerEmpty, RejectForeignOwner | typed outcome (`84aedfd`); не пересоздаётся в self-remount (`ef8a1c5`) |
| O2 | `owner` | чтения `readOwnerObject` (`:433`), декомиссия | `GET` | — | — | — | — |
| **O3** | `owner` | декомиссия: tombstone `retired_at_ms` (`Tools/CasDecommission.cpp:478`) | `IM(e_owner)` | store; тот же `uuid` owner не переписывает → tombstone ложится и над живым перевыпущенным экземпляром (codex) | живой сервер с tombstoned owner (I) | нет | — |

### Staging `POOL/staging/<srid>/<random32>.tmp` {#staging}

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| ST1 | staging | писатель: `writeFile` S3-staging (`ContentAddressedTransaction.cpp:902`), конверт со свежим `incarnation_tag` | `blind PUT` мимо `CasRequests`, `SDK`; `checkFenceOrThrow` при finalize | ключ случайный | поздний PUT → debris; повтор того же staging-объекта → см. B9 | CaBlobPublishCore:StagedEnvelope | — |
| ST2 | staging | писатель: `uploadPendingBlobs` поток для перепубликации (`:293`) | `stream`, mount-план | T1 | объект удалён ST4 → вставка падает | нет | — |
| ST3 | staging | писатель: `cleanupPendingTempFiles` после commit (`:199`) | `blind DEL` своего ключа | ключ уникален | безвредно | нет | — |
| **ST4** | staging | старт: `sweepOwnMountStaging` (`Pool/CasServerRoot.cpp:1812`) из `ContentAddressedMetadataStorage::startup` (`:894`) после `Pool::open` → `mountWritable` (claim) | LIST + `blind DEL`, `IObjectStorage` напрямую | **нет** | зомби-старт: claim → пауза до LIST → преемник того же `srid` пишет staging → зомби удаляет его объекты → copy/stream преемника падает (Z13, codex; V порядок, I сценарий). Вред — отказ вставок, не потеря (precommit-first) | нет | без изменений |
| ST5 | staging | декомиссия: `deleteListedPrefix` (`Tools/CasDecommission.cpp:294`) | LIST + `IM` | store + claim | поздний staging жертвы → debris | нет | `drain_op` |

### Пул и пробы `POOL/_pool_meta`, `POOL/_probe/<uid>` {#pool-probe}

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| PM1 | `_pool_meta` | bootstrap: `PoolMeta::createOrValidate` (`Pool/CasPoolMeta.cpp:158`) | `INM` | store | — | нет | — |
| PM2 | `_pool_meta` | старт: регистрация алгоритма `admitOrValidate` (`:80`) | `RMW` grow-only | store: множество только растёт | поздний `IM` → 412 (codex) | нет | — |
| PM3 | `_pool_meta` | чтения: `createOrValidate` (`:125`), `Pool::refreshAdmittedAlgos` (`Pool/CasPool.cpp:271`), `probePoolBootstrapResidual`, `Backend/CasSentinelProbe.cpp:49` | `GET`/LIST | монотонный кеш | — | нет | — |
| PR1 | `_probe/<uid>` | старт: `runCapabilityProbe` (`Backend/CasProbe.cpp`) create/replace/clobber/remove/list | `INM`/`IM`/`IM`-del | ключ случайный | поздний probe-write → debris под `_probe/` (codex) | нет | — |
| PR2 | `_probe/`, `_pool_meta`, каталог | sentinel probe `Backend/CasSentinelProbe.cpp:11,76` | `probeSentinel`/LIST | — | — | нет | — |

### Без объекта: протокол и чтения {#protocol}

| id | Объект | Актор и функция | Запрос | Защита | Риск | TLA+ | Серия |
|---|---|---|---|---|---|---|---|
| **X1** | ответ relink-confirm | отправитель: `CasRefLedger::confirmExactRef` (`Pool/CasRefLedger.cpp:410`), получатель промоутит без per-file HEAD (`ContentAddressedMetadataStorage.cpp:2380-2410`) | без I/O | **T1** (`fence_ok_fn` = `mayMutate`) | `Yes` зомби из устаревшей памяти → получатель коммитит манифест, чьи блобы GC уже удалил (Z8; V механизм, I вред) | CaRelinkConfirmCore:RConfirm, FenceLoss, Gate1Answer; CaRelinkLaneComposition:ConfirmSource | — |
| **X2** | чтения монтирования | `CasManifestReader`, ref-таблица в памяти, B7 | `GET` / память | **T1** / нет | устаревшие ответы, ошибки (Z9; I) | нет | — |
| **X3** | все T1-записи | `CasOperation::writeLoop` | gate → `backend->write` | **T1** только до отправки | пауза между `gate` (`:914`) и `write` (`:948`) или SDK-повтор → запрос уходит после дедлайна (codex §4; V) | CaErasureProof:AbandonZombie, ZombieLand (поздн.); CaMountObserveCore:Write | без изменений в `writeLoop` по смыслу |
| **X4** | Keeper/RMT | ReplicatedMergeTree | — | таймаут сессии Keeper | действия реплики после отставки (Z17; не исследовано) | нет | — |
| X5 | bootstrap residual | `probePoolBootstrapResidual`, `probeNonTerminalMountSlots` (`Pool/CasPool.cpp:383`) | LIST/`GET` | — | — | нет | — |
| X6 | аномалии | `Pool::reportImpossibleInterference` (`Pool/CasPool.cpp:1796`) | `GET` | — | — | нет | — |

## Непокрытые места вызова {#unmapped}

Пусто. Перечень мест вызова получен полными `git grep` без обрезки:

```bash
P=src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed
git grep -n -E '\.(create|replace|readModifyWrite|readModifyWriteOnPresence|removeCurrent|removeManyWriteOnce|publish|probeSentinel|stream|forEachListedKey)\(' d76b59f9295 -- "$P/" ':!*tests/*' ':!*gtest*' ':!*.md'   # 68 строк
git grep -n -E '(\.|->)(remove|read|head|list)\(' d76b59f9295 -- "$P/" ':!*tests/*' ':!*gtest*' ':!*.md'
git grep -n -E '(objectStorage\(\)|object_storage)\s*->\s*[a-zA-Z]+\(' d76b59f9295 -- "$P/" ':!*tests/*' ':!*gtest*'
```

Соответствие (файл: строки → id):

| Файл | Строки | id |
|---|---|---|
| `Backend/CasHotKeys.cpp` | 266 | C2–C6 |
| `Backend/CasProbe.cpp` | 28, 62, 64, 74, 81, 89, 109, 121, 130, 151, 161, 181, 186 | PR1 |
| `Backend/CasSentinelProbe.cpp` | 11, 49, 76, 118 | PR2, PM3, C8 |
| `ContentAddressedTransaction.cpp` | 199 / 293 / 644 / 902 / 851, 1301, 1483 / 1166, 1485, 1664 / 863, 1507 / 1508, 1678 | ST3 / ST2 / B7 / ST1 / P1 / P2 / R1 / R2 |
| `ContentAddressedMetadataStorage.cpp` | 894 / 2172 | ST4 / B7 |
| `Gc/CasBlobInDegree.cpp` | 352 / 408, 766 | A6 / A1 |
| `Gc/CasGc.cpp` | 397, 405 (helper) / 443, 445 / 1051 / 1164 / 1254, 3952–3967, 4055 / 1332 / 1374, 1378 / 1651, 1775, 4083 / 2081 / 3716 / 3824 / 3885–3891 / 4185, 4462 / 4283, 4500, 4697 / 4344 / 4782 / 4793 / 4830, 4855 / 4916 / 4934–4946 / 4958 / 4993 | MF5+L5 / B5 / A3 / GS2 / A5 / MF5 / MF4 / A6 / B4 / A2 / GS4 / L5 / L7 / GS3–GS4 (rebuild чтения) / B6 / A4 / GS3 / B6, GS4 / GS4 / H1 / GS1 / H2 |
| `Gc/CasGcMaintenanceState.cpp` | 14 / 37 | MS2 / MS1 |
| `Gc/CasGcMetaWriter.cpp` | `writeCondemnedMeta`, `deleteConfirmedMeta` | M6 / M7 |
| `Gc/CasGcReadAhead.cpp` | 95, 101, 130, 136 | M8, MF7 |
| `Gc/CasNamespaceJanitor.cpp` | 42, 111, 131 | L6 |
| `Gc/CasOrphanManifestSweep.cpp` | 64 / 102 / 106 / 262 / 621, 649, 652 / 696 | MT9 / GS4 / A6 / L7 / MF6 / MF8 |
| `Gc/CatalogLifecycleReconciler.cpp` | 93 | C7 |
| `Pool/CasBlobMeta.cpp` | 25 / 32 / 39 / 46 | M5 / M1, M4, M6 / M2, M6 / M7 |
| `Pool/CasKeyReader.h` | 34 | MF7, L4 |
| `Pool/CasManifestReader.cpp` | 56 | MF7 |
| `Pool/CasPartWriteTxn.cpp` | 354 / 359 / 370 / 402 / 495 / 828 / 1229, 1230 | M1 / M2 / M3 / B1 / B2, B3 / MF2 / MF3 |
| `Pool/CasPlainObjects.cpp` | 19 / 29, 59 / 41 / 94 | P1, R1 / P3, R3 / P2, R2 / R3 |
| `Pool/CasPool.cpp` | 271 / 940 / 1796 / 1844 / 1926; bootstrap `admit` 413–536 | PM3 / MT9 / X6 / GS4 / R3; X5, PR1, C1, PM1 |
| `Pool/CasPoolMeta.cpp` | 80 / 125 / 158 | PM2 / PM3 / PM1 |
| `Pool/CasRefCatalog.cpp` | 50 / 77 / 188 / 516 | C8 / C1 / C2–C6 / C7 |
| `Pool/CasRefCkpt.cpp` | 187 / 259 | K2 / K1 |
| `Pool/CasRefLedger.cpp` | 255 / 967, 1010, 1092 / 1154 / 2348 / 3731 / 4544 / 5201 | MF1 / L4, L8 / L2 / L3 / L1 / S1 / GS4 |
| `Pool/CasRefProtocol.cpp` | 885, 971, 1007, 1028, 1077 | L4 |
| `Pool/CasServerRoot.cpp` | 62 / 435 / 564 / 612 / 642 / 793, 800 / 835 / 873 / 980 / 1043, 1062 / 1148, 1157, 1194, 1199, 1261 / 1372, 1375, 1419 / 1623 / 1766 / 1829 | R3 / O2 / O1 / E1, E2 / E2, MT9 / MT1 / MT2 / MT3 / MT4 / MT8 / MT9 / MT5 / MT6 / MT7 / ST4 |
| `Tools/CasDecommission.cpp` | 63, 70, 79 / 103 / 237 / 280 / 353, 375, 437, 447, 465 / 478 | R4, ST5 / MT10, E3 / K2 / MF6 / MT9, O2 / O3 |
| `Tools/CasFsck.cpp` | 60, 379, 529, 645, 656, 756, 818, 834, 923, 961, 1060 | L4, MF7, B6, GS4, A6 (только чтения; `:447` `verdicts.publish` — локальный отчёт, не store) |

Вне `CA/` записей в пул нет (`CasInspect` разбирает переданные байты, I/O не делает — V grep).

## Расхождения с прошлыми отчётами {#discrepancies}

Номера строк `no-clock-fencing.md` взяты с `3b4037ae265`; на базе другие (например `writeLoop` резерв `:914`, а не
`:935`; `computeHeartbeatFloor` `:1025`, а не `:966`; `refAppendFenceOk` `Pool/CasMountRuntime.cpp:155`).

| Пункт | Было | На базе после проверки |
|---|---|---|
| Z1 | Claude: защищён без часов; codex: PARTIAL | **PARTIAL, V**: `planRefCleanup` удаляет логи (и печати) ниже checkpoint, кроме `retained_log_proof`. Поздний append в `(E,T+1)` после удаления печати ложится, но ниже checkpoint → debris. Отказ «навсегда» не обеспечен; вреда нет (I) |
| Z2 | порядок «тело до precommit» не проверен | **V**: `stageManifest` (`INM`) → `precommitAdd` (append) → `ensureBlobPresent` требует `PrecommitState::Durable` |
| Z5 | Claude: без смены формата нет; codex: PARTIAL | механизм **V** на базе; вариант codex (файлы как inline-записи манифестов под ref) не проверялся |
| Z6 | — | **V**: `reconcileMetaClean` `RMW` пишет `Clean` поверх любого не-`Clean` (`Pool/CasPartWriteTxn.cpp:370-391`) |
| Z7 | — | **V**: `beginRemoving` `decide` = сравнение строки по значению + `op.admitted()` (`Pool/CasRefCatalog.cpp:382`) |
| Z8 | Claude: `GET (E,T+1)→404` закрывает; codex: REFUTED | принимаю REFUTED: печати ниже checkpoint удаляемы (см. Z1), `404` не доказывает отсутствие преемника (I) |
| Z11 | farewell по состоянию, без часов | на базе farewell ещё ограничен `Retry::untilLeaseSafe(confirmed_deadline_boot_ms, ...)` (`Pool/CasServerRoot.cpp:1766`) — не отправляется после дедлайна аренды. На `3b4037ae265` — `Retry::within`, пишется и после истечения |
| Z13 | Claude: безвредно; codex: PARTIAL | **V хуже**: `sweepOwnMountStaging` идёт напрямую через `IObjectStorage` (LIST + `removeObjectIfExists`), без фенса и без T1; вызывается в `startup` после claim в `Pool::open`. Вред — отказ вставок преемника (I) |
| Z14 / G3 | Claude: безопасно; codex: REFUTED | **REFUTED подтверждено, V**: `publish` с `WriteSettings{}`, `copyObject` с профилем по умолчанию. Уточнение: ABA только на ETag-диалекте; на GCS повтор даёт новый generation |
| G3 (manifests) | codex: owner-removed манифесты через `WOD` | **V** (`CasGc.cpp:1332`) |
| G4 | поздний `Condemned` безвреден | **V**, уточнение: тело `Clean{0,size}` одинаково для всех инкарнаций, поэтому `IM(e_Clean)` совпадает с любым будущим `Clean` — ABA типична, вред только перепубликация |
| G5 | codex: PARTIAL | **V**: фенс-аут — `RMW` по наблюдённому токену, решение по часам GC; функция на базе и на `3b4037ae265` идентична |
| Z15 | открыто | **V** на базе: комментарий в `runRecoveryWalkOnce` прямо разрешает переход через печать на/выше живого epoch |
| Z16 | «предпосылка спеки, не фенсится» | **хуже на базе, V**: `claimMount` при том же epoch переписывает слот (`seq+1`, «own claim replayed»); на `3b4037ae265` — отказ (`17c7f28`) |
| §4 codex | разрыв допуск→отправка | **V**: `gate` `:914`, `backend->write` `:948`, без перепроверки |
| codex: декомиссия | принятая гонка | **V**: порядок `mount` → `epoch` → tombstone `owner`; на базе нет проверки потери claim (добавлено в серии `eee465d`). Новое: `MountLeaseRenewer::claim` на базе минтит отсутствующий слот (`:1372-1385`); `DecommissionRecovery` решает по wall-clock `expires_at_ms > now_ms`; owner-removed `WOD` манифестов + перевыпуск epoch 1 → возможное удаление живого тела (I) |
| codex: `deleteConfirmedMeta` | пропущен | **V**: `GET` + `IM`, только `Condemned{≤ round}` (M7) |
| codex: `gc/hb`, `gc/maintenance_state`, регистрация алгоритма, bootstrap, probe | пропущены, вреда нет | **V**: `Retry::once`/`IM`; leak-only; grow-only `RMW`; `INM`; случайный ключ (H1, MS1, PM2, C1/O1/PM1, PR1) |
| §1 «`heartbeat_floor` только живучесть» | codex: PARTIAL | **V**: побочный эффект `gc_fenced` — сертификат для `claimMount` reclaim и `isCreatorFenceTerminal` (C4, C5) |
| новое | — | C4/C5 (`reconcileStaleCreator`, `cancelStalledCreating`) зависят от сертификата T3 — отдельный класс, в отчётах не выделен |
| новое | — | B2: поздний PUT блоба с новым конвертом безвреден благодаря ветке `Replaced` в GC (re-condemn текущего токена) |
