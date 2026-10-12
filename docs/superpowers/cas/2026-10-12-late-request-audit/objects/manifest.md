# Аудит: манифест части `POOL/cas/manifests/<ns>/<E-hex>-<b-hex>/<ord>.zst` {#manifest-audit}

База `d76b59f9295` (`cas-janitor-budget`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
**V** — прослежено в коде базы; **I** — вывод. Строки инвентаря: MF1–MF8 (`docs/superpowers/cas/2026-10-12-late-request-audit/inventory.md#manifest`).

## 1. Объект {#object}

| Поле | Значение |
|---|---|
| Ключ | `Layout::manifestKey`: `<prefix>/cas/manifests/<ns>/<writer_epoch-hex>-<build_seq-hex>/<ordinal>.zst` (`CA/Formats/CasLayout.h`). `ns` начинается с `srid` (V: `floorForNamespace` режет `ns` по `/`). Жизнь (`incarnation` каталога) в ключ **не входит** (V) |
| Откуда части ключа | `epoch` = `Pool::liveWriterEpoch` (один `RMW` счётчика `epoch` на каждый mount, E1); `build_seq` = `CasMountRuntime::allocateBuildSeq`, счётчик **в памяти процесса**, старт `next_build_seq = 1` (`CasMountRuntime.h`); `ordinal` = `PartWriteTxn::next_manifest_ordinal` (V) |
| Тело | `PartManifest{ref, root_namespace_id, entries, payload_digest}`, `encodePartManifest` канонический → `sealObject` (V `PartWriteTxn::stageManifest`). Одинаковые входы → одинаковые байты |
| Что меняет ETag | Только новый create после удаления. S3: ETag = MD5 (или MD5 частей для multipart) — **одинаковые байты дают тот же ETag (ABA)**; GCS: новый generation (V `conditionalWriteSettings`: на Generation-диалекте single-part; I для MD5) |
| Владелец | Писатель `srid`/epoch до precommit; после precommit — ref-лог namespace (owner binding `Precommit`/`Committed`); удаляет только GC (owner-removed) или orphan sweep / writer cleanup для непредкоммиченных тел |
| Жизнь | create-once (`WriteOnceKey`, `writeOnceManifestKey`) → чтения → одно удаление. Повторного create того же ключа протокол не делает: `ordinal` и `build_seq` монотонны, `epoch` уникален (V); исключения — перевыпуск epoch (U13) и два процесса с одним epoch (MT2) |

## 2. Операции {#operations}

| # | Актор / функция | Запрос | Профиль / цикл над ним | V/I |
|---|---|---|---|---|
| MF1 | писатель: `PartWriteTxn::stageManifest` → `CasRefLedger::stagingPutIfAbsent` → `CasOperation::create` | `INM` PUT | mount-план (`mount_requests.admit()`, T1); `Retry::standard` (окно 90 с), каждая попытка — `gate(reservedFor(0,2))`; попытка Single-Attempt, `s3_max_unexpected_write_error_retries_override = 1`; неясный исход → resolve-GET, `Conflict` с теми же байтами принимается как свой (`stageManifest`, ветка `Conflict`). `GaveUp`/`Refused` → исключение, ключ не повторяется (ordinal ушёл вперёд) | V |
| MF1a | то же, большое тело на ETag-диалекте | multipart с `INM` на complete, части последовательно (`s3_allow_parallel_part_upload = false`) | один `gate` на всю запись, части не перепроверяются | V настройки, I отсутствие gate между частями |
| MF2 | писатель: `PartWriteTxn::promote` | `GET` тела, проверка `refMatchesBody`/`manifestNamespaceMatches` | mount-план, `Retry::standard`; отсутствие → fail-closed | V |
| MF3 | писатель: `cleanupStagedManifestDebrisBestEffort` (из `abandon`) | `HEAD` + `IM(etag)` | mount-план; пропускает `precommit_manifest` при любом `precommit_state != NotAttempted`; ошибки глотаются | V |
| MF4 | GC: план `planManifestCursorPage` (fold, до `gc/state` CAS) → удаление в фазе `orphan_sweep` `Gc::runRegularRound` (после CAS) | LIST `cas/manifests/`, GET тела, затем `HEAD` + сверка с `nomination.token` + `IM` | open-план (`store->openRequests().admit()` без liveness, V), `Retry::standard`; `Mismatch` → `CORRUPTED_DATA` | V |
| MF4p | премиса MF4/MF6 для namespace **с** строкой каталога | `prefixEligibleUnder` (watermark из `mount`) + `manifestDeletionPremise` (RULE 1: `writer_epoch < cov.last_folded_ref_id.writer_epoch`; RULE 2: не цель хвостовой removal) + `active` (owner-set) | V |
| MF4c | премиса MF4 для namespace **без** строки каталога | ни watermark, ни premise, ни owner-set: кандидат сразу (`planManifestCursorPage`, комментарий «created by a now-dead life») | V |
| MF5 | GC: `foldManifestEdges` (sign<0) кладёт `(id, etag)` в `mf_cleanup` → фаза `manifest_deletes` → `removeCohortWriteOnce` → `removeManyWriteOnce` | `WOD` (`DeleteObjects`, без условия); на GCS — по ключу (`removeChunkWriteOnceOrOneByOne`) | open-план, `Retry::standard`, чанк переотправляется целиком; `etag` только в событии | V |
| MF6 | декомиссия: `sweepNamespace` по группам из LIST `cas/manifests/<srid>/` (`Tools/CasDecommission.cpp`), до удаления `mount` | `HEAD` + `IM` | open-план админ-пула; `Gone`/`Mismatch` терпимы | V |
| MF7 | чтения: mount (`CasManifestReader`), GC fold (`foldManifestEdges`), fsck | `GET` | mount-план (T1) / open | V (по инвентарю, fsck не перечитан) |
| MF8 | LIST: курсор sweep, декомиссия | LIST | open | V |

## 3. Сценарии {#scenarios}

| id | Кто опаздывает | Что успели другие | Что делает опоздавший запрос | Вред | На чём держится сегодня | V/I |
|---|---|---|---|---|---|---|
| S1 | MF1: попытка `INM` PUT, ушедшая в `GaveUp` (сеть/пауза), долетает позже; живой писатель epoch `E` | писатель перестейджил с новым `ordinal` | создаёт тело без владельца | debris; **живёт до смены epoch**: RULE 1 не даёт удалить манифест epoch `E`, пока курсор namespace в `E`; writer cleanup его не знает (в `staged_manifests` только `Committed`) | ключ не переиспользуется (`ordinal` монотонен) | V |
| S2 | MF1 зомби-писателя `E` (SIGSTOP после истечения аренды), преемник `E'>E` того же `srid` | `E'` пишет в namespace, печать `E` свёрнута GC, orphan sweep удалил тела `E` | тело ложится снова | debris; повторный sweep при обходе курсора (RULE 1 уже истинно) | ключ с `E` в пространстве зомби; precommit зомби отвергнет печать ref-лога | V |
| S3 | MF1 той же попытки, долетевшей **после** `WOD` (MF5) этого же ключа | тело было `Committed`, precommit, promote, drop, fold `-1`, удаление | create снова | debris; владельца не будет: `precommitAdd` закрыт (txn завершён, A3) | движок шлёт попытки строго по очереди (`writeLoop`), попытка N+1 после конца N; вариант требует применения хранилищем через часы — невозможно на практике | I |
| S4 | precommit-append (ref-лог) после тела | — | precommit всегда после `Committed` тела (`stageManifest` → `precommitAdd`) | — | порядок вызовов (`ContentAddressedTransaction`, `prepareEntries`); `Uncertain` precommit тело защищает (`cleanupStagedManifestDebrisBestEffort`) | V |
| S5 | MF3 writer cleanup (`IM(etag)`), поздний | никто не пересоздаёт ключ | удаляет своё непредкоммиченное тело или `404/412` | нет | ключ свой; другой процесс этого ключа не создаёт | V |
| S6 | MF4 orphan `IM(token)` свергнутого GC-лидера (пауза между `gc/state` CAS и хвостом) | новый лидер провёл раунды | удаляет кандидата, уже удалённого — `Gone`; или debris S1/S2 | нет | номинация зафиксирована в `gc/state` до удаления; ключ не переиспользуется; open-план не проверяет лидерство, но и не нужно | V |
| S7 | MF4, ABA: между номинацией (GET) и `HEAD` ключ удалён другим (MF6/MF5) и пересоздан поздним S1/S2 тем же содержимым | — | S3: тот же ETag → удаление debris; GCS: новый generation → `Mismatch` → `CORRUPTED_DATA` в фазе `orphan_sweep` | S3: нет; GCS: исключение раунда в последней фазе, остальные номинации раунда не удалены (leak до следующего обхода) | детектор «illegal ABA» в `runRegularRound` | V код, I расписание |
| S8 | MF5 `WOD` свергнутого лидера / повтор чанка движком / повтор SDK `DeleteObjects` | ключ никто не пересоздаёт | удаление отсутствующего ключа | нет | write-once ключ; owner-removed тело не пере-владеется (A3 в `precommitAdd`) | V |
| **S9** | MF5 `WOD` GC-зомби (пауза минуты) с ключами старого epoch `k` декомиссованного `srid` | декомиссия: drop namespace, sweep, удаление `mount`/`epoch`; тот же `uuid` в окне E2 перевыпускает epoch 1..k, открывает те же таблицы (тот же `ns`), `build_seq` снова с 1 | удаляет **новое живое тело** по совпавшему ключу `ns/k-b/o` | **висячий committed манифест**: часть нечитаема; fold `-1` этой части позже упрётся в отсутствующее тело (fail-closed, hold namespace) | ничего: `WOD` без условия, open-план без фенса; держится только на редкости (U13) | I |
| S9b | то же, но не `WOD`, а MF4/MF3 `IM(старый etag)` | — | новое тело — другие байты → другой ETag → `412`; одинаковые байты (та же вставка) на S3 → удалит | редко: висячий манифест | ETag (условно) | I |
| S10 | MF1 нового epoch 1 при остатке старого тела по тому же ключу | субтри не пуст | — | — | перевыпуск epoch требует пустого `cas/manifests/<srid>/` (`casManifestsServerPrefix` в проверке пустоты, `CasLayout.h`); иначе `INM` → `Conflict` с чужими байтами → `CORRUPTED_DATA` в `stageManifest` (отказ вставки) | V код проверки (по комментарию), I полнота |
| S11 | SDK-повтор `INM` PUT | — | нет повтора: Single-Attempt + `override = 1` | — | `conditionalWriteSettings` | V |
| S12 | повтор движком после неясной попытки: попытка 1 применяется после ответа на resolve-GET «нет» | попытка 2 | `412` попытке 2 → `Conflict` с теми же байтами → принят как свой | нет | детерминированные байты + сравнение в ветке `Conflict` | V |
| S13 | второй экземпляр того же `srid`, разный epoch | оба живы | ключи не пересекаются; RULE 1 даёт удалить тела младшего epoch только после свёрнутой печати | нет для манифеста (вред в ref-логе — его аудит) | уникальный epoch (E1) + печать | V |
| S14 | второй экземпляр с тем же `(uuid, epoch)` (MT2 на базе) | оба стартуют с `build_seq = 1` | `INM` одного получает `Conflict` с чужими байтами → `CORRUPTED_DATA`; при одинаковых байтах — принят как свой; поздний MF3 одного по ETag удаляет совпавшее тело другого | отказ вставок; редко — висячий манифест | ничего, кроме различия байт | V код, I вред |
| **S15** | **устаревший срез каталога в MF4c**: писатель стейджит первый манифест namespace, у которого ещё нет строки каталога (рождение — в `precommitAdd` → `appendRefOps`); GC: LIST увидел тело, `CasRefCatalog::read` после LIST строки не увидел | писатель: строка каталога, precommit, promote — за миллисекунды | GC номинирует тело без премисы, ретирует source edges, после `gc/state` CAS удаляет `IM(token)` | **promote успел → висячий committed манифест** (часть потеряна, fold позже fail-closed); promote не успел → `promote` падает fail-closed, debris | ничего: инвариант «строка каталога раньше любого объекта жизни» не выполнен для манифеста — ключ без жизни, `stageManifest` до рождения. Достижимо при `moveDirectory` таблицы (`republishRef` → `prepareEntries` → `stageManifest` до `namespaceLife(to_ns)`, `ContentAddressedTransaction.cpp`) и при любом первом билде в нерождённый namespace | V порядок и ветка, I достижимость (не проверено, всегда ли `to_ns` рождён раньше) |
| S16 | устаревший `mount` (watermark) в MF4p | renewer поднял floor | старый floor меньше → меньше кандидатов | нет (консервативно) | `prefixEligibleUnder` монотонен по floor | V |
| S17 | устаревшее чтение тела (MF2, MF7, fold) | — | тело неизменяемо: видно то же тело или отсутствие | отсутствие → fail-closed (promote) / барьер/hold (fold) / ошибка `SELECT` (U11) | write-once | V |
| S18 | поздний `HEAD`→`IM` MF6 декомиссии против MF4 GC | оба удаляют одни ключи | `Gone`/`Mismatch` терпимы в `sweepNamespace` | нет | `IM` + отсутствие пересоздания | V |

Итого сценариев: **18** (+ 2 подварианта). Вредных: S9/S9b (U13, время ни при чём, нужна пауза GC на минуты), S14 (MT2), **S15 (гонка без зомби)**.

## 4. Нужная строгость гарантии {#strictness}

| Вред | Сценарий | Вероятность | Требование |
|---|---|---|---|
| висячий committed манифест → потеря части, затем hold namespace в GC | S15 | окно — несколько RTT между PUT тела и чтением каталога GC; без паузы. Пауза писателя между `stageManifest` и `precommitAdd` (медленный S3, GC JVM-подобная пауза не нужна) расширяет окно. Требует первого билда в нерождённый namespace | **невозможно**: это не «опоздание», а гонка обычного кода |
| то же | S9 | пауза GC-лидера на всё время декомиссии + рестарт того же `uuid` в окне E2 + совпадение `ns` | невозможно желательно; принимается «минимизировано + детект», если U13 закрыт (epoch не переиспользуется) |
| отказ вставок | S14, S10 | MT2 на базе; на `3b4037ae265` отказ (`17c7f28`) | детект достаточно (уже `CORRUPTED_DATA`) |
| debris / leak | S1, S2, S7(GCS) | любая сетевая неясность | минимизировано + детект; S1 до смены epoch — стоит счётчик |

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| MF1 `INM` PUT, single-part | `gate` перед каждой попыткой: остаток аренды > `2E+M` = 2·(5+2)+2 = 16 с (V `writeLoop`, `CasMountRuntime::admit`); попытка: Single-Attempt клиент с `object_storage_attempt_timeout_ms = T` и `connect_timeout_cap_ms = C` (V `conditionalWriteSettings`); окно `Retry::standard` 90 с | таймаут Poco — на операцию сокета, не на запрос: медленная, но живая отправка тела длиннее `T` (I); DNS, получение credentials (I); пауза между `gate` и `send` (U14); применение хранилищем после последнего байта |
| MF1a multipart | то же один раз | число частей × `T` без повторного `gate` (I); `CompleteMultipartUpload` может прийти после истечения аренды |
| MF2 GET | `gate` `reservedFor(0,1)`; `T`, `C` | то же per-socket |
| MF3 `HEAD`+`IM` | mount-план, `gate` на каждый | то же |
| MF4/MF5/MF6 удаления | open-план: только окно 90 с и `T`/`C`; **аренда GC не проверяется** (`openRequests().admit()` без liveness в `runRegularRound`) | весь срок паузы процесса: свергнутый лидер шлёт удаления после пробуждения |

Проталкивание дедлайна в транспорт (`lease_valid_until − M` перед каждой операцией сокета): для манифеста почти ничего не даёт.
- Закрывает: S1/S2 частично (меньше поздних тел зомби) — но они и так безвредны.
- Не закрывает S9: удаления идут по open-плану; дедлайна аренды GC в транспорте нет, его нужно было бы вводить отдельно (аренда `gc/state`). И даже с ним остаются пауза между проверкой и `send` и позднее применение `DeleteObjects`.
- Не закрывает S15: там нет опоздания, только устаревший срез.
- Не закрывает S14: оба экземпляра держат «живую» аренду.

## 6. Перенос гарантии на уровень хранилища {#store-level}

| Вариант | Что делает | Формат | Миграция | Объём | Риск | TLA+ |
|---|---|---|---|---|---|---|
| **A. Рождение namespace до тела** (S15) | в `prepareEntries`/`ContentAddressedTransaction::commit` звать `Pool::namespaceLife(ns)` до `stageManifest`; тогда комментарий-инвариант MF4c верен | нет | нет | ~5–10 строк, 2 файла | низкий: `namespaceLife` уже идемпотентен; лишний cold-read каталога на первый билд | шаг «stage до birth» и срез каталога в sweep (см. §7) |
| **A'. Премиса для MF4c** (S15, страховка) | для namespace без строки каталога требовать `prefixEligibleUnder(floorForNamespace(...))`: активный билд имеет `b ≥ min_active_build_sequence` → не кандидат; устаревший `mount` консервативен | нет | нет | ~10 строк, `Gc/CasOrphanManifestSweep.cpp` | низкий; dead-life debris уходит чуть позже (после renew floor) | то же |
| **B. Не переиспользовать epoch** (S9, S10) | декомиссия не удаляет `epoch`, а оставляет счётчик (tombstone вместе с `owner`); перевыпуск epoch 1 невозможен → ключи манифестов уникальны навсегда | читатели старого формата не ломаются (объект тот же, меняется только «не удаляем») | нет | ~20 строк, `Tools/CasDecommission.cpp` + проверка пустоты субтри (`epoch` перестаёт считаться признаком занятости) | средний: затрагивает U13/E2/O3 целиком | `CaCasMountCore:RemintEpochDecom` → убрать действие, проверить `NoManifestIdReuse` |
| **C. `IM` вместо `WOD` для MF5** (S9, S14) | `mf_cleanup` уже хранит `etag` из fold; удалять `IM(etag)` по ключу (или `DeleteObjects` с per-object ETag, где хранилище это поддерживает — I) | нет | нет | ~30 строк, `Gc/CasGc.cpp` | +1 запрос на ключ на GCS/без batch-ETag; S3 ABA одинаковых байт остаётся | расщепить `GDeleteManifest` |
| D. Оставить + контроль | счётчик номинаций MF4c (namespace без строки каталога); fsck: committed ref → отсутствующее тело (`CasFsck`, `dangling`); `CORRUPTED_DATA` «ManifestId collision» в `stageManifest` и «illegal ABA» в `orphan_sweep` — в алерт; счётчик `retained_unconsumed_seal` уже есть для S1 | — | — | ~10 строк | вред S15 — потеря части, детект постфактум | — |

Рекомендация: A (+ A' как пояс), B в рамках U13, C — опционально.

## 7. TLA+ {#tla}

| Сценарий | Модель / оператор | Покрыто? |
|---|---|---|
| S4 порядок тело → precommit | `CaGcRootLocalPartManifestCore:WStageManifest`, `WPrecommitAdd`, `WManifestBodyArrives` (`EnableMissingBody`) | да |
| S5, S6, S8 удаление write-once ключа | `GDeleteManifest`, `GOrphanSweep`, `NoManifestIdReuse` | да, атомарно; позднего применения нет |
| S9 reuse + отложенная очистка | `GStaleReuseCleanup` под `SabotageReuseManifestId` — показывает вред (INV_NO_LOSS) при переиспользовании id | только как саботаж; перевыпуск epoch как причина reuse не связан с `CaCasMountCore:RemintEpochDecom` |
| S1–S3 поздний create после удаления | нет: после удаления `everEdged` запрещает повторный `WStageManifest` | нет |
| S15 устаревший срез каталога | нет: `GOrphanSweep` проверяет `owner[m] = None` атомарно в шаге удаления; ветки «нет строки каталога» нет; `GMarkSweepEligible` — оракул, не RULE 1 | **нет** |
| RULE 1 (печать в старшем epoch) | нет отдельного оператора; `CaRefFoldClampRecoveryCore:DeleteNominatedManifest`/`PostCasDelete` покрывают только «номинация принята до удаления» | частично |
| S7 ABA на GCS / S3 | нет токенов манифеста в модели | нет |

Минимальное добавление в `CaGcRootLocalPartManifestCore`:
1. `GObserveSweep(n)` — снимок `owner`/«namespace рождён» в переменную; `GOrphanSweep` удаляет по снимку. Писатель: `WStageManifest` разрешён до `WBirth(n)`. Свойство `CommittedManifestBodyRequired` должно упасть без варианта A и держаться с ним.
2. `GDeleteManifestLate(m)` — удаление по ранее взятому `mfCleanup` после `WRemint` (снимает `m` из `everEdged`). Держится только с вариантом B или C.

## 8. Вывод {#verdict}

- Манифест **safe по ключу** для всех чисто «поздних» запросов (S1–S8, S11–S13, S16–S18): безопасность — уникальный epoch в ключе + write-once + печать ref-лога; часы не участвуют. Debris S1 живёт до смены epoch.
- **Unsafe без времени**: S15 — гонка устаревшего среза каталога в orphan sweep с первым билдом нерождённого namespace → висячий committed манифест (I, нужна проверка достижимости через `moveDirectory`). S9 — `WOD` GC-зомби по ключу, переиспользованному после перевыпуска epoch (U13, I). S14 — MT2.
- Проталкивание дедлайна в транспорт ничего из этого не закрывает.
- Рекомендация: A + A' (~20 строк, 2–3 файла, без смены формата), B внутри U13; модельное дополнение §7.1. Трудозатраты: ~0.5 дня код+тест на A/A', ~1 день TLA+.
