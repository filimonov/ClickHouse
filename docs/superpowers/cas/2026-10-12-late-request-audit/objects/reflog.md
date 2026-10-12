# Аудит: ref-log `POOL/cas/ns/stream/<life>/_log/<E-seq>.zst` (вкл. `EpochSeal`) {#reflog-audit}

База `d76b59f9295` (`cas-janitor-budget`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
**V** — прослежено в коде базы; **I** — вывод. Код не исполнялся, TLC не запускался.

## Итог в одну строку {#tldr}

Сам объект защищён хранилищем (write-once `INM`, плотные id, печать в слоте `T+1`). Открытых мест два, оба —
через `_ckpt`, а не через ключ лога:

- **R4/R5**: поздний append после удаления печати GC ложится, `_ckpt`-merge отвечает `IdenticalSkip`, и
  подтверждение держится только на T1 → при сбое T1 подтверждённая, но невидимая запись (потеря acked-записи).
- **R11**: монтирование с эпохой ниже уже запечатанной (T8, нормальный путь, без нарушения времени) или зомби
  пишет `(E,T+2)` над собственной печатью → `_ckpt` отказывает (`CORRUPTED_DATA`), подтверждения нет, но таблица
  на этом монтировании заклинена, на ключе остаётся мусор.

U12/L8 (recovery переходит через печать на/выше своего epoch) **сам по себе безвреден** (codex прав), кроме ветки R11.

## 1. Объект {#object}

| Поле | Значение |
|---|---|
| Ключ | `Layout::refLogKey(life, id)` = `namespaceStreamPrefix(life) + "_log/" + renderRefTxnId(id) + ".zst"` (`Formats/CasLayout.h`), V |
| id | `RefTxnId{writer_epoch, ref_sequence}`; следующий id — чистая функция состояния: `nextRefTxnId` (`Pool/CasRefProtocol.cpp`): тот же epoch → `seq+1`, иначе `{live_epoch, 1}`. Счётчика нет, V |
| Тело | `RefLogTxn{ns, txn_id, ops, prev_epoch_seal}`; `prev_epoch_seal` обязателен ровно на `seq=1` не-генезисного epoch (`chainLinkFor`, `validateEpochSealGrammarContextual`), V |
| `EpochSeal` | `RefLogTxn` с одной операцией `EpochSeal`, без содержимого таблицы (`makeEpochSealTxn`, `Pool/CasRefLedger.cpp`). Эффект только позиционный: занимает `(E,T+1)` мёртвого epoch. Байты детерминированы по `(ns, id, prev seal)` → два recoverer пишут одинаковые байты (I) |
| ETag | Объект write-once: ETag не меняется, пока объект не удалён. Пересоздание возможно только после удаления (GC `WOD`, janitor) |
| Владелец | писатель таблицы текущего монтирования (append); recovery монтирования с `live_epoch` выше (печати); GC (удаление ниже checkpoint); janitor (удаление мёртвой жизни) |
| Время жизни | от append до `planRefCleanup` (лог `< checkpoint_snapshot_id`, `≤` курсора fold, не `retained_log_proof`) или до janitor мёртвой жизни |
| Связанный объект | `_ckpt` (`cas/ns/state/<life>/_ckpt`): `committed_through`, `last_epoch_seal`, `checkpoint_snapshot_id`. Лог становится историей только после публикации frontier в `_ckpt` (`commitRefChunk`, `resolveWedgeOnce`), V |

## 2. Операции {#operations}

| # | Актор | Функция | Запрос | Профиль |
|---|---|---|---|---|
| O1 | писатель (insert/merge/mutation/ALTER/DROP/relink/precommit/abandon/stale-precommit sweep) | `CasRefLedger::commitRefChunk` → `op.create(key, bytes, Retry::standard())` | `INM` create | Native conditional, `SingleAttempt` клиент (`max_retries=0`, `requestTimeoutMs=T`, connect cap `C`), `s3_max_unexpected_write_error_retries_override=1` (`ObjectStorageBackend::conditionalWriteSettings`); движок `CasOperation::writeLoop` перевыпускает те же байты по `Retry::standard`, каждая попытка через `gate(reservedFor(0,2))` (T1). V |
| O2 | писатель | `resolveWedgeOnce` → `op.create(wedge.key, wedge.bytes, Retry::once())` | `INM`, заранее построенные байты | то же, одна попытка. V |
| O3 | recovery | `runRecoveryWalkOnce` → `op.read(refLogKey, Retry::standard())` по арифметической цепочке от checkpoint | `GET` | SingleAttempt read на Native-монтировании; каждое чтение через gate T1 + liveness (stop/cancel/catalog/remount). V |
| O4 | recovery | `runRecoveryWalkOnce` → `op.create(seal, Retry::once())` в отсутствующий слот мёртвого epoch (`epoch < live_epoch`, `Live`) | `INM` | одна попытка под generation допуска recovery. V |
| O5 | recovery | `readCheckpointSnapshotBase` (`Pool/CasRefProtocol.cpp`) — чтение лога базы и предыдущей печати | `GET` | V |
| O6 | GC fold intake | `Gc` ref intake (`CasGc.cpp`, цикл `while (expected)`), `crossEpochFromSeal` | `GET`, ограничено `_ckpt.committed_through` | V |
| O7 | GC | `Gc::enumerateRefPrefix` | LIST | только кандидаты/свидетели; не авторитет для удаления. V |
| O8 | GC cleanup | `CasGc.cpp` ref cleanup: `planRefCleanup` → `removeCohortWriteOnce` чанками; перед чанком `authorityHolds` перечитывает каталог (etag) и `gc/state` (owner, seq) | `WOD` (`DeleteObjects` без условия; GCS — по одному) | `Retry::standard`. V |
| O9 | janitor | `NamespaceJanitor::runOnePage` (`Gc/CasNamespaceJanitor.cpp`) | LIST + `HEAD` + `IM` delete | мёртвая жизнь целиком. V (по inventory L6) |
| O10 | fsck | `Tools/CasFsck.cpp` | `GET`/LIST | только чтение |

`_ckpt`-публикации (O1/O2/O4 после create) — объект `_ckpt`, здесь только как условие подтверждения.

## 3. Сценарии {#scenarios}

E — epoch опоздавшего писателя, `T` — последний его лог, S — преемник (`live_epoch` > E), Z — зомби.

| id | Кто опаздывает | Что успели другие | Что делает поздний запрос | Вред | На чём держится | V/I |
|---|---|---|---|---|---|---|
| R1 | O1 `(E,T+1)` Z: пауза/разрыв/позднее применение | S ещё не читал слот `(E,T+1)` | ложится; S при recovery читает его как обычную запись, затем ставит печать в `(E,T+2)` | нет: запись становится историей до передачи, линеаризуется до takeover | store: `INM` + recovery читает слот до печати (`runRecoveryWalkOnce`: present-ветка, `++sequence`) | V |
| R2 | O1 `(E,T+1)` Z | S прочитал `(E,T+1)` = 404, готовит печать | ложится между чтением и `INM` печати | нет: печать получает `Conflict`, occupant не печать → straggler принят, печать повторяется в `T+2` (`CASRefRecoveryStragglerAdopted`) | store: `INM` печати | V |
| R3 | O1/O2 `(E,T+1)` Z | S поставил печать `(E,T+1)` | `412`; settle read видит печать | нет: `Occupant::SuccessorSeal` → `lane_state=Closed`, `INVALID_STATE`, не подтверждено (`commitRefChunk`, `resolveWedgeOnce`) | store: ключ занят печатью | V |
| **R4** | O1 `(E,T+1)` Z: пауза **между gate и отправкой** на минуты (SIGSTOP), отправка после | S запечатал `(E,T+1)`, записал ≥2 txn в `E+1`, опубликовал снимок не на `(E+1,1)`, GC свернул и удалил печать (`planRefCleanup`: `< checkpoint`, `≤ cursor`, не `retained_log_proof`) | `INM` ложится в пустой ключ. Затем вклад `_ckpt{committed_through=(E,T+1)}`: `mergeCommittedThrough` (эпохи `E` и `E+1`, durable `last_epoch_seal=(E,T+1)` ≥ вклада) пропускает, `merged == durable` → `IdenticalSkip` → далее `refuseUnlessAdmitted` | если T1 пропустил: install + **подтверждение записи, которую никто не прочтёт** (recovery идёт от снимка в `E+1`, курсор GC уже выше) → потеря acked-записи; манифест без владельца → orphan sweep. Если T1 отказал: `NeedsRecovery`, не подтверждено, мусор ниже checkpoint | **только T1** (`op.admitted()` в `publishCkpt` decide и `refuseUnlessAdmitted` перед install). Если `_ckpt` уже в `E+2`: `mergeCommittedThrough` бросает `CORRUPTED_DATA` → не подтверждено (store) | механизм V (`planRefCleanup`, `mergeCommittedThrough`, `publishCkpt` decide, `commitRefChunk` post-durable), достижимость I |
| **R5** | O2 `resolveWedgeOnce` Z, тот же ключ и байты | как R4 | как R4 (`publishCkptContribution` с `committed_through=wedge.txn_id`) | как R4 | как R4 | V/I |
| R6 | O4 печать `(e,k)` recovery зомби-монтирования (epoch ниже нового) | новый recoverer уже поставил ту же печать / GC её удалил | до удаления: `412`, occupant — печать, adopt. После удаления: печать ложится снова ниже checkpoint. Вклад `_ckpt` у зомби: `mergeCommittedThrough` бросает (разрыв > 1 epoch) или identical | мусор ниже checkpoint до следующей уборки; ложный `CORRUPTED_DATA` в логе зомби | store: `INM` + детерминированные байты печати; merge `_ckpt` | V механизм, I вред |
| R7 | повтор движка / SDK | первая попытка O1 в полёте, движок по таймауту перечитал (404) и перевыпустил | вторая ложится; первая позже получает `412` | нет: settle read, `Occupant::Ours` по равенству байт | store: `INM`; SDK-повтора нет (`getSingleAttemptClient`: `max_retries=0`; WriteBufferFromS3 override=1) | V |
| R8 | ETag ABA | — | у `INM` нет токена; `WOD` без условия; janitor `IM(HEAD-etag)` | нет: write-once ключ, ABA возможна только на пересозданном мусоре (R4/R6), его удаление безвредно | store | I |
| R9 | второй экземпляр, тот же `(uuid, epoch)` (T6/T7) | оба выводят один id | проигравший видит чужой occupant | `Foreign` → `Faulted` + аномалия (отказ записи, громко); при совпадении байт оба «Ours», вред нет | store: `INM` + сравнение байт | V (ветка `Foreign`), I достижимость |
| R10 | O3 recovery Z через печать своего epoch (L8/Z15), S уже писал в `E_s` | S: печать `(E,T+1)` + записи `(E_s,…)` | вид Z = история S; следующий id `(E,1)` без chain link | нет: deposed-гейт в `flushRefBatch` (`next_id.ref_sequence == 1 && !chainLinkFor`) → `INVALID_STATE`, ничего не отправлено | код: детерминированный гейт + `applyTxnInPlace` strict-increase | V |
| **R11** | O3 + O1: монтирование с epoch `E` поднимает таблицу, у которой уже есть печать `(E,k)`, а записей в `E+1…` нет. Источники: (а) **T8** — запоздавший стартер с меньшим epoch законно берёт слот после clean farewell старшего (`claimMount`: reclaim при любом *другом* epoch); (б) зомби с ошибкой T1 | старший `A` (`E+1`) поднял таблицу, запечатал `E`, в таблицу не писал | walk: печать `(E,k)` → `(E+1,1)` 404 → `E+1 ≥ live_epoch` → break; `greatest_applied=(E,k)` (печать) → `nextTxnId(E)=(E,k+1)`, `seq≠1`, гейт молчит → `INM (E,k+1)` ложится. Вклад `_ckpt{(E,k+1), seal=∅}` → `checkRefCkptInvariants` бросает `CORRUPTED_DATA` | не подтверждено (`requireRecovery`). Recovery не видит `(E,k+1)` (прыгает через печать) → `Ready` → тот же id с новыми байтами → `Foreign` → `Faulted`. Таблица на этом монтировании недоступна для записи; мусор над печатью; ложная тревога о порче | store: инвариант `_ckpt` («печать того же epoch ниже frontier запрещена») | V каждый шаг, I сквозной путь |
| R12 | O4 печать recovery монтирования со старшим epoch по живому младшему (T8) | младший `B` законно жив на `E` (после T8) | зомби старшего (с ошибкой T1) запечатывает `(E,k)` — живой epoch `B` | `B` получает `SuccessorSeal` → `Closed`: отказ записи у живого держателя | ничего, кроме T1 зомби; корень — T8 | I |
| R13 | O8 поздний `WOD` свергнутого лидера GC | checkpoint ушёл дальше, новый лидер убирает сам | удаляет логи/снимки ниже **старого** checkpoint | нет: они ниже и нового; `retained_log_proof` нового ≥ старого checkpoint (печать новой базы лежит выше старой базы); удаление мусора R4/R6 безвредно. Исключение — повтор отставленной жизни (T5 каталога): ключи той же `incarnation` | store: монотонный `_ckpt` + checkpoint-тройка; `authorityHolds` перед чанком | V механизм, I для T5 |
| R14 | O8 vs recovery по старому `_ckpt` | recovery сэмплировала старый checkpoint | GC удалил лог/базу в её диапазоне | нет: 404 внутри committed → перечитать `_ckpt`, etag другой → restart; база пропала → `classifyMissingSampledBase` → restart | store: etag `_ckpt` | V |
| R15 | O7 LIST устарел/неполон | — | кандидат пропущен | только утечка; удаление решает checkpoint, не LIST | `planRefCleanup` | V |
| R16 | O1 зомби в отставленную жизнь | каталог отставил жизнь, janitor идёт | ложится в мёртвый префикс | мусор до прохода janitor | ключ содержит `incarnation`; `catalog_life_invalidated` | V (inventory L6); T5 — зависимость |
| R17 | устаревшее чтение для решения | — | recovery: 404 → `INM` печати (решает store); финал: перечитать `_ckpt`, сравнить etag и тело (`final_ckpt`); писатель: гейт по `rt->last_epoch_seal` из памяти | нет: все решения повторно проверяются условным запросом или etag `_ckpt` | store | V |
| R18 | O6 GC читает straggler R1 до печати | — | fold учитывает его рёбра | нет: тот же вид, что примет S | `committed_through` ограничивает чтение | I |

Сценариев: **18**.

## 4. Нужная строгость гарантии {#strictness}

| Сценарий | Худший вред | Что нужно на практике | Вердикт |
|---|---|---|---|
| R4/R5 | подтверждённая запись исчезает (INSERT вернул OK, части нет; манифест уберёт orphan sweep) | пауза SIGSTOP-класса между gate и отправкой длиной ≥ takeover (`L·1.05+P` ≈ 41.5 с) + recovery таблицы S + ≥2 txn S + снимок + раунд GC с уборкой (минуты) **и** ошибка T1 после пробуждения (`op.admitted()` по boot-clock пропускает просроченную аренду). Сетевой blip не хватает: запрос применяется в пределах Poco-таймаутов, а печать к тому времени ещё на месте | вероятность ничтожна, но вред — тихая потеря acked-данных. Должно быть **невозможно**: фикс дешёвый (§6 вариант A) |
| R11 | отказ записи таблицы на монтировании до перемонтирования с epoch > старшего; мусорный объект; ложный `CORRUPTED_DATA` | (а) T8: запоздавший стартер с меньшим epoch после clean farewell старшего — без паузы, обычный рестарт двух процессов с одним `uuid`; (б) зомби — как R4 | fail-closed, данных не теряет. «Минимизировать + обнаружить» приемлемо, но корень (T8) чинить; плюс гейт писателя (§6 вариант B) |
| R12 | отказ записи у живого держателя | T8 + зомби старшего epoch | закрывается фиксом T8 |
| R6, R13–R16 | мусор, ложные тревоги | обычные паузы | приемлемо как есть |
| R9 | отказ записи, аномалия | T6/T7 | закрывается фиксом T7 |

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| O1 append `INM` | движок: каждая попытка через `gate(reservedFor(0,2))` = 2 конверта `E=T+2C` (T1); `Retry::standard` — окно политики; клиент `SingleAttempt`: `requestTimeoutMs=T` (5 с), connect cap `C` (1 с), `max_retries=0`; `100-continue` от 1 MiB (`getSingleAttemptClient`); тела больше порога multipart на ETag-сторе идут последовательными частями (`s3_allow_parallel_part_upload=false`) — V | Poco send/receive — на одну операцию сокета, не на запрос: медленная капля обходит `T` (I). Пауза между `gate` и `backend->write` (`writeLoop`) — без перепроверки (U14, V). DNS/учётные данные — вне `T` (I). Позднее применение хранилищем после последнего байта |
| O2 wedge `INM` | то же, одна попытка, generation исходного допуска | то же |
| O3/O5 `GET` | gate `reservedFor(0,1)` + liveness; SingleAttempt | то же |
| O4 печать | `Retry::once`, generation допуска recovery, `checkRecoveryStillAdmitted` перед create | то же |
| O8 `WOD` | GC-аренда (`gc/state` owner+seq) перечитывается перед чанком; `Retry::standard` | между `authorityHolds` и `DeleteObjects` времени нет (I); вред — только R13 |
| Подтверждение O1/O2 | `publishCkpt` decide: `op.admitted()`; `refuseUnlessAdmitted` перед install (T1) | — это и есть единственная защита R4/R5 |

**Дедлайн аренды в транспорт** (проверка `lease_valid_until − M` перед каждой операцией сокета, таймаут сокета =
min(конфиг, остаток)):

- R4/R5: **не закрывает**. Вред возникает не из-за длинного запроса, а из-за паузы *до* отправки и решения
  подтвердить по `IdenticalSkip`. Проверка перед `send` сужает окно до «пауза между проверкой и `send`», но
  SIGSTOP ровно там остаётся. Подтверждение решает T1 после ответа; та же зависимость от часов.
- R11 (а): **не касается** — время не нарушено, монтирование законно живо.
- R6, R12: сужает, не закрывает (та же пауза перед `send`, позднее применение хранилищем).
- Позднее применение после последнего байта: открыто при любом транспортном таймауте.

## 6. Перенос гарантии на уровень хранилища {#store-level}

| Вариант | Что делает | Формат | Миграция | Объём | Риск | TLA+ |
|---|---|---|---|---|---|---|
| **A. Отказ «вклад = печать»** в `publishCkpt` decide | если вклад не печать (`contribution.last_epoch_seal != contribution.committed_through`) и `contribution.committed_through == durable.last_epoch_seal` → вернуть отдельный исход «запечатано» (не `IdenticalSkip`); писатель трактует как `SuccessorSeal`: `Closed`, `INVALID_STATE`, без install. Обоснование: печать стоит ровно на нашем id ⇒ наш объект не может быть в цепочке; честный straggler (R1) даёт печать выше своего id, равенства нет; при `_ckpt` в `E+2` merge и так бросает | не меняется | нет | ~20–30 строк: `Pool/CasRefCkpt.cpp` (decide, новый `CkptPublishOutcome`), `Pool/CasRefLedger.cpp` (2 места: `commitRefChunk`, `resolveWedgeOnce`) | низкий; закрывает R4/R5 без часов | `CaRefTableSnapshotLogCore`: печать — объект в `logs`, удаляемый `GcCleanupLog`; поздний PUT по `\notin logs`; подтверждение через `CkptMerge`; инвариант «acked ⊆ достижимое от `ckpt`» |
| **B. Гейт писателя «печать своего epoch»** | в `flushRefBatch` рядом с deposed-гейтом: если `preview_epoch_seal && preview_epoch_seal->writer_epoch >= live_epoch` → `INVALID_STATE` deposed, ничего не отправлять; recovery, пересёкшая печать epoch `≥ live_epoch`, ставит этот признак явно | не меняется | нет | ~10–15 строк, `Pool/CasRefLedger.cpp` | низкий; закрывает R11 до отправки (нет мусора, нет ложного `CORRUPTED_DATA`, понятная ошибка «deposed») | модель с двумя epoch и писателем младшего epoch после печати (§7) |
| **C. Печати не удалять** | `planRefCleanup` сохраняет максимальный id каждого мёртвого epoch из листинга (печать всегда последняя в своём epoch) | не меняется | нет | ~15 строк GC | утечка: объект на таблицу на epoch — растёт с перемонтированиями; закрывает R4/R5/R6 позиционно, но A дешевле | `GcCleanupLog` не трогает печати |
| **Корень R11/R12: T8** | `claimMount` reclaim требует `our_epoch > existing.writer_epoch` | не меняется | нет | ~5 строк `Pool/CasServerRoot.cpp` | низкий | в теме T8 |
| **Оставить + контроль** | детекторы: `CASRefAppendSealRejected` (R3), `CASRefRecoveryStragglerAdopted` (R1/R2), `CASRefRecoveryEpochSealAdopted`; новый счётчик на `CORRUPTED_DATA` из `checkRefCkptInvariants`/`mergeCommittedThrough` при вкладе писателя (R11, R6); fsck: «не-печать на id = `_ckpt.last_epoch_seal`» (след R4/R5) и «лог выше печати в том же epoch» (R11) | — | — | ~40 строк fsck + счётчик | R4/R5 обнаруживаются только после потери | — |

Рекомендация: A + B + фикс T8. Все без смены формата.

## 7. TLA+ {#tla}

| Сценарий | Модель / оператор | Покрыто? |
|---|---|---|
| R1, R2 | `CaRefTableSnapshotLogCore`: `LatePredecessorPut`, `RAdoptStraggler`, `RSealSlot`; `CaCasMountCore`: `StragglerLands`, `SealSlot` | да (один epoch, один слот) |
| R3 | `CaRefTableSnapshotLogCore`: `WResolveSealRejected`, `WResolveDurable` (`pendingSlot \notin sealedIds`) | да |
| R4, R5, R6 | `GcCleanupLog`/`CleanupLogGate` удаляют только из `logs`; печать живёт в ghost `sealedIds`, который никто не уменьшает, а `WResolveDurable`, `LatePredecessorPut`, `WOrphanLands` проверяют `\notin sealedIds`. Модель считает печать вечной | **нет** — именно разрыв Z1 |
| R7 | `WAttemptAmbiguous`, `WResolveDurable` | да |
| R9 | `CaRefLaneCore`: `ForeignWrite`, `ObserveOccupied` | частично (чужой писатель, не близнец) |
| R10, R11, R12 | id — одно целое пространство `Seqs` без epoch; нет писателя с `live_epoch` ниже печати | **нет** |
| R13–R15 | `GcCleanupLog`, `GcCleanupSnap`, `RWalkVanish`, `RRestartOrStuck` | да (кроме T5) |
| R16 | `CaRefNsCleanupStaleLeaderCore:StaleJanitorDelete` | да |
| `FrontierOldWriterLosesToSeal` | ставит свидетеля, запрос старого писателя не выдаёт (codex прав) | — |

Минимальные добавки:

1. `CaRefTableSnapshotLogCore`: печать — элемент `logs` (вид `"seal"`), `GcCleanupLog` может её удалить при
   `L < ckpt.base`; поздние PUT проверяют `\notin logs`; `FrontierAcknowledge` через `CkptMerge` с правилом
   `IdenticalSkip`; инвариант `AckedReachable` (каждый подтверждённый id достижим из `ckpt` по цепочке).
   Ожидание: контрпример R4 без варианта A, зелёный с ним. ~40 строк.
2. Две эпохи: `writer_epoch ∈ {1,2}`, recovery эпохи 2 ставит печать в эпоху 1, писатель эпохи 1 продолжает
   после печати (T8 или зомби). Инвариант «нет durable не-печати выше печати того же epoch». Ожидание:
   контрпример R11 без варианта B. ~60 строк, отдельный модуль.

## 8. Вывод {#verdict}

- Ref-log: **safe** на уровне ключа (INM + плотные id + печать) для R1–R3, R7–R10, R13–R18.
- **safe-by-time**: R4/R5 — подтверждение позднего append после удаления печати держится только на T1;
  вред — потеря подтверждённой записи. Вариант A (~25 строк, без смены формата) делает это невозможным.
- **fail-closed, но не safe**: R11/R12 через T8 (без нарушения времени) — заклинивание таблицы и мусор;
  вариант B (~15 строк) + `claimMount` «только больший epoch» (~5 строк).
- U12 как отдельная угроза снята (R10), кроме ветки R11.
- Объём: ~45 строк кода в 3 файлах + 2 добавки в TLA+ (~100 строк).
