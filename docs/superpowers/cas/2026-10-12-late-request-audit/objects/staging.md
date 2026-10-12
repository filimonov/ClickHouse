# Аудит опоздавших запросов: staging `POOL/staging/<srid>/<random32>.tmp` {#staging-audit}

База `d76b59f9295` (`cas-janitor-budget`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
**V** — прослежено в коде базы; **I** — вывод. Строки инвентаря: ST1–ST5, тема T9, Z13, U9.

## 1. Объект {#object}

| Поле | Значение |
|---|---|
| Ключ | `physicalKey(pool_prefix + "/staging/" + server_root_id) + "/" + getRandomASCIIString(32) + ".tmp"` (`ContentAddressedTransaction.cpp:writeFile`, `ContentAddressedMetadataStorage::stagingKeyPrefix`). **V** |
| Префикс | `staging/<srid>/` — без `writer_epoch`, без `ServerUUID`. Общий для всех инкарнаций одного `srid`. `srid` — настройка диска `server_root_id`, постоянна между рестартами (`ContentAddressedMetadataStorage.cpp:286`). **V** |
| Тело | `[CABL envelope][payload]`. Заголовок со свежим `incarnation_tag`, `build_id = 0` (`buildS3StagingBlobHeader`). Тело пишется один раз и не меняется. **V** |
| ETag | Меняется только при новом PUT. Ключ случайный, повторный PUT того же ключа возможен лишь как повтор SDK тех же байтов. **V** |
| Условные запросы | В живом пути — нет ни одного. Только декомиссия удаляет по `IM(listed etag)` (`Tools/CasDecommission.cpp:deleteListedPrefix`). **V** |
| Владелец | Транзакция вставки; ключ живёт только в памяти (`PendingBlob::staging_key`), ни в одном долговечном объекте не записан. **V** (поиск `staging_key` по `CA/`) |
| Включение | Только при `cas_staging_backend = s3` и не read-only; по умолчанию `local` (`ContentAddressedMetadataStorage::startup`). **V** |
| Жизнь | PUT в `writeFile` → `publishStaging` (после `precommitAdd`) копирует/стримит в `blobs/` → удаление в `cleanupPendingTempFiles` только при `committed`. При abort объект остаётся до следующего старта монтирования (`sweepOwnMountStaging`). GC и fsck `staging/` не видят (`Formats/CasLayout.h:398`). **V** |

## 2. Операции {#operations}

| id | Актор | Функция | Запрос | Профиль | Допуск / фенс |
|---|---|---|---|---|---|
| ST1a | писатель | `writeFile` → `IObjectStorage::writeObject(..., WriteMode::Rewrite)` → `WriteBufferFromS3` | blind PUT (≤ 32 MiB) или MPU: `CreateMultipartUpload`, `UploadPart`×N (в `nextImpl`, по мере заполнения), `CompleteMultipartUpload` | клиент по умолчанию `client.get()`: SDK до `s3_retry_attempts = 500`, задержка 25 мс…5 с; сверху цикл `WriteBufferFromS3` `max_unexpected_write_error_retries = 4` (`WriteBufferFromS3.cpp:makeSinglepartUpload`, `completeMultipartUpload`) | только `checkFenceOrThrow(generation)` в `CaContentWriteBuffer::finalizeImpl` перед `sink->finalize()`: `mayMutate()` = `now < deadline` **без резерва**, + поколение. Части MPU уходят до проверки. **V** |
| ST1b | писатель | `CaContentWriteBuffer::cancelImpl` → `WriteBufferFromS3::tryToAbortMultipartUpload` | `AbortMultipartUpload`, best effort | по умолчанию | нет. **V** |
| ST2 | писатель | `uploadPendingBlobs` → `source.open` → `op.stream(staging_key, Retry::standard())` | GET (стрим) | через `CasRequests`, mount-план | `mountRequests().admit()` (T1 на чтение). **V** |
| B3 | писатель | `PartWriteTxn::ensureBlobPresent` → `VerbatimStagedBlobPublication` → `ObjectStorageBackend::publish` → `copyObject` | CopyObject / UploadPartCopy из staging в `blobs/` | `WriteSettings{}` + `NativeOnly`, клиент по умолчанию, SDK повторяет | `op.publish` под `publication_policy = asSingleAttempt()` (движок не повторяет), T1 в начале. Только первая публикация после `Absent` (`beginPublication`). **V** |
| ST3 | писатель | `cleanupPendingTempFiles` при `committed` | blind DEL своего ключа | `removeObjectIfExists`, по умолчанию | нет. **V** |
| ST4 | старт | `ContentAddressedMetadataStorage::startup` → после `openPoolView` (→ `Pool::open` → `mountWritable` → claim) → `Cas::sweepOwnMountStaging` (`Pool/CasServerRoot.cpp`) | LIST всего префикса (`max_keys = 0`, все страницы) + blind DEL каждого ключа | `IObjectStorage` напрямую, клиент по умолчанию, мимо `CasRequests` | **нет**: ни фенса, ни поколения, ни времени. `noexcept`, ошибки глотаются. **V** |
| ST5 | `FORGET`/декомиссия | `deleteListedPrefix(op, ".../staging/<victim>/")` | LIST + `IM(etag)` DEL | `CasRequests`, `Retry::standard` | claim жертвы `openForDecommission` (`NoWait`). **V** |
| — | GC, fsck | — | не трогают `staging/` | — | **V** (`CasLayout.h:398`, `CasServerRoot.h:626`) |

## 3. Сценарии {#scenarios}

A — старая инкарнация `srid`, B — преемник того же `srid` (тот же `ServerUUID`: тот же PV на другом узле, клон каталога данных, рестарт при зависшем старом процессе).

| id | Кто опаздывает | Что успели другие | Что делает опоздавший | Вред | Что держит сегодня | V/I |
|---|---|---|---|---|---|---|
| S1 (Z13) | ST4 A: claim прошёл, пауза (SIGSTOP/свап/VM freeze) между claim и концом LIST ≥ `L + наблюдение` | B ждёт стабильности токена (`claimMountAwaitingExpiry`, `L·1.05+P` ≈ 41.5 с), claim, начинает вставки, пишет staging | A делает LIST, видит ключи B, удаляет их | отказ вставок B (`FILE_DOESNT_EXIST` «disappeared before retagged blob publication» или retry-later), потери нет | **ничего** | V порядок, I расписание |
| S2 | ST4 A: пауза между LIST и DEL | B пишет staging | A удаляет только ключи из снимка LIST | нет: ключи B случайны и минтятся после claim B, а он после ответа LIST A | порядок: LIST A → claim A (на сторе) → claim B → PUT B | I |
| S3 | ST4 A: сетевой разрыв во время LIST; SDK повторяет страницу (500 попыток) через весь разрыв | как S1 | страница LIST применяется после PUT B; удаление ключей B | как S1; паузы процесса не нужно, нужен разрыв ≥ ~40 с во время старта | ничего | I |
| S4 | ST4/ST3 DEL: позднее применение или повтор SDK | — | удаляет ключ из своего снимка / свой ключ | нет: ключи уникальны, повторно не минтятся | случайный ключ (32 символа) | V |
| S5 | писатель A (зомби) ещё копирует/стримит свой staging | B стартовал и сделал sweep | B удалил staging A; copy A получает `NoSuchKey`, stream — короткое чтение | нет сверх фенса A: блоб не опубликован → часть не продвинута; precommit-first | `precommitAdd` до `uploadPendingBlobs`; `requireAdmitted`; проверка размера в `ObjectStorageBackend::publish` («nothing published») | V |
| S6 | ST1a A: пауза между `checkFenceOrThrow` и `sink->finalize()`, или SDK-повторы PUT/Complete за дедлайн, или позднее применение стором | B сделал sweep на старте | объект появляется после sweep | debris в `staging/<srid>/` до следующего старта; потребителя нет (публикация A отказана T1) | ничего; вред ограничен утечкой | V профиль, I расписание |
| S7 | ST1a A: процесс убит/завис посреди MPU | — | части лежат как незавершённый MPU; поздний `Complete` → как S6 | утечка хранилища навсегда: LIST объектов MPU не видит, sweep их не прерывает | ничего (только lifecycle-правило бакета, если настроено) | V |
| S8 | ST1a: повтор SDK того же PUT | ST3 уже удалил ключ после commit | ранняя попытка применена стором после DEL | воскрешённый staging-объект → debris | ничего; вред — утечка | I |
| S9 | ETag ABA: те же байты staging перезаписаны (повтор SDK) | — | ETag тот же; живой путь условий на staging не использует; ST5 удалит любой из двух | на staging — нет. Но envelope staging фиксирован, а `copyObject` идёт с профилем SDK: повтор copy после GC-удаления блоба воскрешает тот же `incarnation_tag` в `blobs/` — это B9 (отчёт по блобу) | `beginPublication` запрещает только повтор движком, не SDK | V профиль, I расписание |
| S10 | второй экземпляр, тот же `srid` и `ServerUUID`, A жив и продлевает | B стартует | `claimMountAwaitingExpiry` → `LiveDoubleStart`, `startup` бросает до sweep | нет | claim по токену на сторе | V |
| S11 | то же, но B с `cas_unsafe_remount_no_delay` (T5) | A жив | B перезабирает слот сразу, sweep удаляет живой staging A | отказ вставок A; A потом фенсится | разрешение оператора; ничего больше | V |
| S12 | другой `ServerUUID`, тот же `srid` | — | `ForeignOwner` → `openPoolView` бросает до sweep | нет | owner-якорь | V |
| S13 | не зомби: живой процесс, abort/исключение вставки | — | staging абортнутой транзакции оставлен намеренно (`cleanupPendingTempFiles`, ветка `!committed`) | утечка, растёт с числом неудачных вставок до рестарта; GC/fsck не видят | ничего | V |
| S14 | ST1a зомби X после `FORGET` X (ST5 уже прошёл) | декомиссия вычистила `staging/X/` | поздний PUT/Complete X | debris навсегда: `srid` X выведен, sweep его больше не запустит | ничего | I |
| S15 | B3/ST2 B читает staging, удалённый в S1 | — | copy: `NoSuchKey` → `beginPublication` потрачен → stream → `open` бросает → после `max_publication_attempts` retry-later | отказ вставки, частичного блоба нет (`NativeOnly` copy атомарна; stream сверяет размер) | атомарность copy, проверка размера | V |
| S16 | устаревшее чтение для решения: LIST sweep на сторе без строгой согласованности LIST | — | LIST пропускает ключи | только debris; LIST не может вернуть ключ, записанный позже ответа | — | I |
| S17 | собственные писатели того же процесса | — | sweep идёт до публикации `cas_store`; `stagingKeyPrefix` требует `store()` | нет: свой писатель не может начать staging до sweep | порядок в `startup` | V |

Итого: 17 сценариев; вред по сторе: S1, S3, S11 (отказ вставок), S6, S7, S8, S13, S14 (утечка). Потери данных, воскрешения ссылки, висячей ссылки, клина нет.

## 4. Нужная строгость гарантии {#rigor}

- **Худший вред** — отказ INSERT у живого преемника (S1/S3). Данные не теряются: блобы публикуются после `precommitAdd`, часть без блоба не продвигается. **V**
- **Вероятность S1/S3** — низкая. Нужны все три: `cas_staging_backend = s3` (по умолчанию выключено); преемник с тем же `srid` и `ServerUUID`; остановка старого процесса ≥ ~41.5 с ровно между claim и концом LIST (SIGSTOP, VM freeze) или разрыв той же длины. Обычный сетевой сбой не хватит. **I**
- **Утечки (S7, S13)** — вероятность высокая: каждая неудачная вставка (S13) и каждый убитый процесс во время MPU > 32 MiB (S7). Без детектора. Это главный практический вред объекта.
- **Вывод по строгости** — S1/S3 допустимо «свести к минимуму + детектировать», но фикс порядка (O1) делает их невозможными за ~20 строк, так что лучше сделать невозможными. Утечки — достаточно «детектировать + чистить».

## 5. Таймауты {#timeouts}

Константы (`src/IO/S3Defines.h`): connect 1000 мс, `request_timeout_ms` 30000 (Poco send/receive на одну операцию сокета), adaptive timeouts включены, `retry_attempts` 500, задержка ≤ 5000 мс, `max_unexpected_write_error_retries` 4, `max_single_part_upload_size` 32 MiB, `min_upload_part_size` 16 MiB. **V**

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| ST1a PUT/Complete | проверка `now < deadline` перед `finalize` (без резерва); per-socket 30 с; connect 1 с | SDK 500 попыток × (≤ 30 с + ≤ 5 с) — десятки минут; × 4 цикла `WriteBufferFromS3`; задержка между проверкой и `send`; DNS, получение credentials |
| ST1a UploadPart | ничего, кроме per-socket | вся загрузка MPU до `finalize` без проверки аренды |
| ST2 stream | T1 на открытие (`reservedFor(0,1)`) | чтение тела после допуска (per-socket, без общего лимита) — безвредно |
| B3 copy | T1 на старте `op.publish` | SDK-повторы CopyObject и циклы `copyS3File` (UploadPartCopy) |
| ST3 DEL | per-socket | SDK 500 попыток; аренды не касается |
| ST4 LIST+DEL | **ничего, связанного с арендой** | все страницы LIST и все DEL, каждая до 500 попыток; sweep не знает об аренде |
| ST5 | `CasRequests` `Retry::standard` | — |

**Дедлайн аренды в транспорт** (`lease_valid_until − M` перед каждой операцией сокета, таймаут сокета = min(конфиг, остаток)):

- S1/S3 закрываются по времени, если sweep пойдёт под фенсом монтирования. Между дедлайном A и первым возможным PUT B есть запас `0.05·L + P` ≈ 11.5 с (+ `M`). Открытым остаётся: пауза процесса между проверкой и `send` страницы LIST; страница LIST, обслуженная стором позже чем через ~11.5 с после отправки. Это safe-by-time, не safe. **I**
- S6, S8, S14 сужаются, но остаются (пауза после проверки, позднее применение после последнего байта). Вред — debris.
- S7, S13 транспорт не трогает: это утечки без опоздания.

## 6. Перенос гарантии на уровень хранилища {#store-level}

| Вариант | Суть | Формат | Миграция | Объём | Риск | TLA+ |
|---|---|---|---|---|---|---|
| **O1. LIST до claim** | В `startup` снять LIST `staging/<srid>/` до `openPoolView`; удалять ключи снимка только после успешного claim. Любой ключ B минтится после claim B, claim B — после claim A (условная запись `mount` по токену), claim A — после ответа LIST A. Значит, снимок A не содержит ключей ни одного преемника. Поздний DEL A безвреден (S4). | не меняется | нет | ~20–30 строк: `ContentAddressedMetadataStorage.cpp:startup`, разделить `sweepOwnMountStaging` на list/delete в `Pool/CasServerRoot.{h,cpp}`; +1 gtest | низкий: объекты, созданные предшественником между LIST и claim, ждут следующего старта. Опирается только на исключительность claim (уже доказана `CaCasMountCore`) | малое: см. §7 |
| O2. `writer_epoch` в ключе | `staging/<srid>/<writer_epoch>/…`; sweep удаляет только эпохи `< своей` | ключ staging меняется, но он нигде не хранится долговечно; старые бинари чистят префикс рекурсивно — не ломаются | новый sweep должен чистить и плоские ключи старого формата | ~40 строк, 3 файла | **зависит от монотонности epoch**: T8 (reclaim требует «другой», не «больший») и U13 (декомиссия выдаёт epoch 1 заново) — зомби со старой большей эпохой удалит staging новой эпохи 1. Без фикса T8/U13 хуже O1 | средне |
| O3. Утечки | (a) удалять staging при abort (ключ принадлежит одной транзакции, долговечно нигде не записан — I); (b) при старте, тем же снимком до claim, `ListMultipartUploads` + `AbortMultipartUpload` под `staging/<srid>/` (нужен новый метод `IObjectStorage`, только S3) или требовать lifecycle-правило `AbortIncompleteMultipartUpload` в доке | не меняется | нет | (a) ~10 строк, 1 файл; (b) ~60 строк, 4 файла или только дока | (a) низкий, если подтвердить, что `PendingBlob` не переживает транзакцию; (b) средний | не нужен |
| Как есть + контроль | ProfileEvent на число удалённых sweep объектов и на «staging disappeared» в `source.open`; fsck: перечислить `staging/<srid>/` для выведенных `srid` и объекты старше claim текущей инкарнации; `blob_storage_log`: Delete staging-ключа процессом A, затем GET 404 у B — подпись Z13 | — | — | ~30 строк | — | — |

Рекомендация: O1 + O3(a) + доку про lifecycle MPU. O2 не нужен при O1.

## 7. TLA+ {#tla}

| Сценарий | Модель / оператор | Покрытие |
|---|---|---|
| S9 (ABA через verbatim copy) | `CaBlobPublishCore`: `StagedEnvelope`, `BeginPublication`, `LateBackendLand`, `StagedRetagWitnessAction` | staging-объект там — вечный источник с фиксированным envelope; его удаление и повторы SDK copy не моделируются |
| S5, S15 (precommit-first) | `CaBlobPublishCore`: `DurablePrecommit`, `Defect("publish_before_precommit")`; `CaBuildRootPrecommit` | да, на уровне блоба |
| S10–S12 (claim, twin, T5) | `CaCasMountCore`: `ClaimMount`, `ObservedReclaim`, `RejectForeignOwner`, `TwinUuids`, `SupersededWriterMakesNoMutation` | исключительность claim — да; запись в staging — нет |
| S1–S4, S6–S8, S13, S14, S16 | — | **не моделируются**: в моделях нет staging-префикса и sweep |

Минимальное расширение (по образцу `CaCasMountCoreStraggler`): переменные `staged ⊆ Incs × Keys`, `snap[i]`, `deletedLive`; действия `StagePut(i)` (под `CanWrite(i)`), `SweepList(i)`, `SweepDel(i, k)` для `k ∈ snap[i]`; константа `SweepBeforeClaim`. Инвариант `NoLiveStagingDeleted`: `SweepDel(i, k)` не трогает ключ инкарнации `j ≠ i`, для которой `LiveIncarnation(j)`. Ожидание: при `SweepBeforeClaim = FALSE` — контрпример (S1), при `TRUE` — держится. ~60 строк, одна `.cfg` на каждый порядок.

## 8. Вывод {#verdict}

- Сегодня: **unsafe** для S1/S3 (zombie-start sweep удаляет staging живого преемника; держится ни на чём), вред — отказ вставок, не потеря. Остальное — утечки (S6–S8, S13, S14) без детектора; S13 и S7 происходят в обычной работе.
- Объект включается только при `cas_staging_backend = s3`; по умолчанию сценарии недостижимы.
- Действие: O1 (LIST до claim, DEL после) — делает S1/S3 невозможными по порядку на сторе, без изменения формата, ~25 строк + gtest + малое расширение `CaCasMountCore`. Плюс O3(a) (удаление staging при abort) и ProfileEvent/fsck на утечки.
- Дедлайн в транспорт для этого объекта не нужен после O1; без O1 он даёт только safe-by-time с запасом ~11.5 с.
