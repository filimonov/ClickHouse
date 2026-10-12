# Аудит поздних запросов: снимок `_snap` и `_ckpt` {#snapckpt-audit}

Исполнитель `snapckpt`. База `cas-janitor-budget` @ `d76b59f9295`. `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
**V** — прослежено в коде базы (`file:function`); **I** — вывод. Код не исполнялся, TLC не запускался.
Строки инвентаря: S1, S2, K1, K2, K3 (`inventory.md#snapshot-ckpt`).

## 1. Объект {#object}

| Объект | Ключ | Тело и токен | Владелец | Жизнь |
|---|---|---|---|---|
| Снимок | `POOL/cas/ns/stream/<life>/_snap/<E>-<seq>.zst` (`CasLayout.h:refSnapshotKey`) | `sealObject(RefSnapshot, encodeRefTableSnapshot)`: каноничный текст состояния таблицы на id `(E,seq)`, детерминирован (`CasRefSnapshotFormat.h`). Write-once (`WriteOnceKey`), до 64 MiB текста (`ref_snapshot_max_bytes`) (V) | писатель ref-таблицы монтирования `E` | от `INM` до удаления GC строго ниже `checkpoint_snapshot_id` или janitor мёртвой жизни (V) |
| `_ckpt` | `POOL/cas/ns/state/<life>/_ckpt` (`refCkptKey`), `<life>` содержит incarnation | `RefCkpt` из 4 опциональных скаляров: `life_epoch`, `committed_through`, `checkpoint_snapshot_id`, `last_epoch_seal`; каноничный текст без сжатия (`CasRefCkptFormat.h`). Меняется только `publishCkpt` через `RMW`: тело = `mergeCkpt(durable, contribution)`, запись только если тело изменилось (identical-skip) (V `CasRefCkpt.cpp:publishCkpt`) | все писатели жизни: создатель, писатель лога, публикатор снимка, recovery, разрешение wedge | от первого `INM` до janitor мёртвой жизни (`CasNamespaceJanitor.cpp:runOnePage`); других удалений нет (V grep `refCkptKey`) |

Свойство, на котором держится всё ниже: **тело `_ckpt` строго монотонно** в частичном порядке полей. `mergeCkpt` берёт поле-максимум. `mergeCommittedThrough` берёт старший id и бросает `CORRUPTED_DATA` при несмежных epoch. `publishCkpt` отказывает в уменьшении `life_epoch` и не пишет при `merged == durable` (V). Поэтому в пределах одного ключа тело не повторяется. ETag-MD5 и generation тоже не повторяются (V для тела, I для MD5-коллизий).

## 2. Операции {#operations}

| # | Актор и функция | Объект | Запрос | Профиль |
|---|---|---|---|---|
| O1 | публикатор снимка: `CasRefLedger::tryPublishSnapshotAndAdvanceCheckpointOnceOnRuntimeImpl` | `_snap` | `op.create` = `INM`; на конфликте свои байты = успех, чужие → `CORRUPTED_DATA` | `Retry::standard` (90 s). Каждая попытка `SingleAttempt`, `max_unexpected_write_error_retries=1` (`CasObjectStorageBackend.cpp:conditionalWriteSettings`). Повтор только в `writeLoop` после чтения-разрешения. На ETag-хранилище тело >32 MiB уходит multipart: части последовательно, `INM` на `CompleteMultipartUpload`. GCS: одна часть (V) |
| O2 | то же, после O1: `publishCkptContribution{committed_through=x, checkpoint_snapshot_id=x}` | `_ckpt` | `RMW` (`readModifyWrite`: `GET` → `decide` → `IM(e)`/`INM`) | чтение `Retry::standard`, запись как O1. На конфликте `decide` заново по свежему телу (V) |
| O3 | писатель лога: `commitRefChunk` → `birth_contribution{life_epoch}` до лога, `commit_contribution{committed_through=id, last_epoch_seal}` после лога | `_ckpt` | `RMW` | как O2. `FencedOut`/исключение → `NeedsRecovery` (V) |
| O4 | recovery: `runRecoveryWalkOnce` → `publish_recovered_frontier`, затем `{committed_through=seal, last_epoch_seal=seal}` | `_ckpt` | `RMW` + точное перечитывание `readCkpt` | как O2 (V) |
| O5 | `resolveWedgeOnce` → frontier wedge | `_ckpt` | `RMW` | как O2 (V) |
| O6 | создатель: `CasRefCatalog::completeCreation` шаг 2 `{life_epoch=creator.writer_epoch}` | `_ckpt` | `RMW` | `policy` вызывающего (V) |
| O7 | чтения `readCkpt`: recovery (sample, missing-base, final install), orphan sweep (`CasOrphanManifestSweep.cpp:589,831`), fsck. GC fold: `GET` + `decodeRefCkpt` (`CasGc.cpp:~1592`). Decommission: `HEAD` (`CasDecommission.cpp:237`) | `_ckpt` | `GET`/`HEAD` | `Retry::standard`. На монтировании под `gate`, у GC открытая плоскость (V) |
| O8 | чтение базы `readCheckpointSnapshotBase` (`CasRefProtocol.cpp`): recovery, GC cleanup, fsck, `recoverRefTableDetailedFromAuthority` (orphan sweep, rebuild) | `_snap` + лог того же id + seal-предшественник | `GET` по точному ключу | `Retry::standard` (V) |
| O9 | GC: `Gc::cleanupRefObjects` → `planRefCleanup` → `removeCohortWriteOnce` | `_snap` строго ниже `checkpoint_snapshot_id` | `WOD` (`DeleteObjects` без условия; на GCS по одному) | `Retry::standard`. Перед каждым чанком `authorityHolds`: перечитывает каталог (etag + строка + life) и `gc/state` lease. Мимо фенса монтирования (V) |
| O10 | janitor: `NamespaceJanitor::runOnePage` | `_snap`, `_ckpt` мёртвой жизни | LIST `cas/ns/` → `HEAD` (если LIST без токена) → `IM`-delete | `Retry::standard`. `op.admitted()` проверяется перед каждым удалением (V) |

## 3. Сценарии {#scenarios}

Z-виды: пауза = SIGSTOP/GC/swap; late = хранилище применяет запрос поздно; resend = повтор SDK/движка; srid2 = второй экземпляр того же `srid`; stale = устаревшее чтение; old-GC = задержанный поток старого раунда.

| id | Кто опаздывает | Что успели другие | Что делает поздний запрос | Вред | Безопасность держится на | V/I |
|---|---|---|---|---|---|---|
| SK1 | O3 `commit_contribution` зомби `E` (пауза после `gate` или late) | преемник `E+1` прочитал `_ckpt`, нашёл лог зомби `(E,s)` как единственного преемника фронтира, опубликовал `(E,s)`, запечатал `(E,T+1)`, записал `_ckpt` | `IM(e_old)` → 412: тело менял преемник. Повтор `decide` у зомби: слияние даёт тело, равное хранимому → нет записи | нет | store: ETag сменил преемник (O4). Монотонность: тело не возвращается к `e_old` | V |
| SK2 | то же, но `IM(e_old)` зомби ложится **раньше** записи преемника | преемник взял sample `e_old` | `_ckpt.committed_through=(E,s)`. Это правда: лог `(E,s)` durable | нет. У преемника `IM(e_old)` → 412, `decide` по свежему телу. Если зомби лёг между sample и final install, `final_ckpt->etag != accepted` → `return nullopt` → recovery заново | store: `RMW` + проверка ETag и тела в final install (`runRecoveryWalkOnce`) | V |
| SK3 | O2 публикатор снимка зомби: `_snap/(E,x)` уже durable, `IM` на `_ckpt` поздний | преемник продвинул `_ckpt` (seal, frontier, свой снимок) | 412 → `decide`: `checkpoint_snapshot_id` = max → тело не меняется → `IdenticalSkip`/`Published`; зомби считает снимок «принятым», но `admitted()` под lock → `false` | debris: `_snap/(E,x)` ниже будущего checkpoint, GC удалит (O9) | store: монотонное слияние + ключ снимка с epoch `E` (не пересекается с ключами `E+1`) | V |
| SK4 | O2 зомби **читает** `_ckpt` уже после записи преемника (`e_s`), у преемника ещё нет своего снимка (`checkpoint_snapshot_id=(E,w)`, `w<x`) | преемник запечатал `(E,T+1)`, `T≥x`, записал `_ckpt` | слияние **продвигает** `checkpoint_snapshot_id` до `(E,x)`, `committed_through` остаётся `(E+1,k)`; инварианты проходят (`checkRefCkptInvariants`). `IM(e_s)` ложится, если зомби прошёл `gate` по времени или застрял между `gate` и send | нет: `_snap/(E,x)` = состояние по логам `≤(E,x)`, а это общая история (преемник прошёл те же логи). Recovery от `(E,x)` идёт через seal к `E+1`. Идущий recovery преемника видит новый ETag → restart | store: детерминированный снимок по durable-логам; ETag-ревалидация sample. Инвентарь S1 («без `_ckpt` не авторитетен») неполон: зомби может сделать свой снимок авторитетным, но только правдивым | V механизм, I «правдивость» (state зомби = только установленные им транзакции) |
| SK5 | O1 `INM` `_snap/(E,x)` применяется поздно, после того как `_ckpt` ушёл выше `x` и GC удалил `_snap/(E,x)` | публикатор повторил, продвинул `_ckpt`; GC удалил ключ | ключ отсутствует → `INM` ложится: воскрешённый снимок ниже checkpoint | debris до следующего раунда GC: `planRefCleanup` берёт кандидатов из LIST, `x < checkpoint` → удаление | store: `snapshotDeletableUnderCkpt` строго ниже; recovery читает только `checkpoint_snapshot_id` | V |
| SK6 | O1 resend того же `INM` (повтор `writeLoop` после неоднозначной попытки или поздняя первая попытка) | — | 412 → чтение-разрешение: байты свои → `Committed(resolved_by_read)` | нет | store: `INM` + детерминированные байты | V |
| SK7 | O2–O6 resend `IM(e)` (поздняя первая попытка после успешного повтора) | своя или чужая запись сменила ETag | 412 | нет. Если чужая запись дала те же байты, `writeLoop` объявляет `Committed` по чтению: тело содержит вклад, вред нет | store: `IM` + монотонность | V |
| SK8 | ETag ABA: два писателя из одного `B1` получают одно `B2` | A пишет `B2` (`e2`) | `IM(e1)` у B → 412 → `decide`: merged == durable → нет записи | нет. Совпадения ETag при разных телах нет: тело уникально в истории ключа. Повторной записи тех же байт тоже нет (identical-skip) | store: монотонность + identical-skip (`publishCkpt`) | V |
| SK9 | ETag ABA через удаление и пересоздание `_ckpt` | janitor удалил `_ckpt` жизни, которую каталог потом **вернул** (T5: ABA каталога, `inventory-codex.md` C2/C8); кто-то пересоздал `_ckpt` через `INM` телом, равным одному из старых (например, `{life_epoch=E}`) | очень поздний `IM(e_old)` с тем же MD5 ложится поверх нового тела. Слияние считалось от старого тела | потеря полей `_ckpt` воскрешённой жизни → `CORRUPTED_DATA` в recovery или потеря истории. Но корень — сама T5: janitor уже удалил `_snap`/`_log` этой жизни | ничто, кроме необратимости отставки жизни (в базе её нет) | I |
| SK10 | O10 janitor старого раунда (old-GC/пауза): `IM`-delete `_ckpt`/`_snap` мёртвой жизни с токеном из LIST/`HEAD` | — | ложится, если токен не менялся | нет: жизнь вне каталога, новая жизнь получает новый incarnation → другой ключ. Исключение — T5 (SK9) | store: life в ключе + `IM` | V (без T5) |
| SK11 | O9 GC-лидер свергнут (old-GC): `DeleteObjects` чанка по плану от старого `_ckpt` | новый лидер; писатели продвинули `_ckpt` | удаляет `_snap` строго ниже **старого** checkpoint. Старый ≤ текущего (монотонность) → ниже текущего тоже | нет: удалённые снимки никто не назначит базой. Recovery с sample старой базы → 404 → ETag сменился → `RestartRecovery` | store: монотонность `_ckpt` + строгая граница (`planRefCleanup`). `authorityHolds` (каталог, lease) — только сужение окна, не безопасность | V |
| SK12 | stale: recovery взял `_ckpt` (база `X`), GC удалил `X` после продвижения | — | `GET _snap/X` → 404 | нет: `classifyMissingSampledBase` → ETag сменился → `RestartRecovery`. Тот же разбор для 404 лога внутри committed-диапазона | store: ETag меняется при любом изменении тела | V |
| SK13 | stale: `recoverRefTableDetailedFromAuthority` (orphan sweep, rebuild, fsck) с устаревшим `_ckpt` — без ревалидации ETag | GC (SK11) удалил базу | `CORRUPTED_DATA` | orphan sweep: пропуск удалений по namespace (fail-closed, V `CasOrphanManifestSweep.cpp`). Rebuild: отказ раунда rebuild (ложная тревога, liveness). Fsck: ложное повреждение | fail-closed у вызывающего; ложная тревога не дублируется | V (вред — доступность/диагностика) |
| SK14 | stale: GC fold/cleanup читает старый `_ckpt` | `_ckpt` ушёл выше | witness ниже, граница удаления ниже | недочистка, исправится в следующем раунде | монотонность: старое ≤ нового | V |
| SK15 | stale: orphan sweep строит protection-set по старому `_ckpt` | писатель закоммитил новый манифест после чтения `_ckpt` | манифест вне `active` | вне объекта (манифест). Предварительно: precommit и его frontier в `_ckpt` идут до PUT манифеста, LIST видит манифест только после frontier, sweep читает `_ckpt` после LIST страницы → покрыт | передаю владельцу манифестов | I |
| SK16 | O6 поздний `completeCreation` зомби-создателя `{life_epoch=E_z}` | `reconcileStaleCreator` передал `Creating` актору `E_r>E_z`; тот записал `life_epoch=E_r`, затем birth | `INM` (зомби читал «нет объекта») → 412; `IM` от старого тела → 412. Повтор `decide`: `lifeEpochWouldDecrease` → `FencedOut` (не допущен) или `CORRUPTED_DATA` в процессе зомби (допущен по времени) | нет на хранилище; у зомби ложная тревога, если он ещё «допущен» | store: `INM`/`IM` + отказ на уменьшение | V |
| SK17 | O3 зомби пишет лог `(E,T+1)` после удаления печати (`inventory.md` L2), затем `commit_contribution` | `_ckpt` прошёл несколько epoch: `committed_through=(E+2,·)` | `mergeCommittedThrough` бросает `CORRUPTED_DATA` в `decide` зомби (epoch не смежны). Если `E+1`: тело не меняется | нет на хранилище; у зомби `CORRUPTED_DATA` вместо `FencedOut`. `mergeCkpt` не смотрит на `op.admitted()`, в отличие от проверки `life_epoch` | store: невалидное слияние не дойдёт до записи | V |
| SK18 | srid2: два экземпляра с одним `(uuid, E)` (T6/T7) публикуют снимки | — | одинаковое состояние → одинаковые байты → `INM`-конфликт принят как свой. Разное состояние на одном `(E,x)` → `CORRUPTED_DATA` (`a DIFFERENT object occupies snapshot`). `_ckpt`: монотонное слияние, без потерь | обнаружение (fault), не тихая порча. Расхождение логов ловит L9 раньше | store: `INM` + сравнение байт | V механизм, I достижимость |
| SK19 | late `INM` `_ckpt` (birth или frontier зомби) в жизнь, которую janitor уже вычистил | жизнь вне каталога | ключ отсутствует → создаётся `_ckpt`-«фантом» | debris; janitor удалит на следующем проходе (`parseRefCkptKey` такие ключи распознаёт) | store: life в ключе | V |

Итого 19 сценариев. Потеря данных возможна только в SK9, и только при T5.

## 4. Нужная строгость {#strictness}

| Что | Худший вред | Расписание | Достаточно «свести к минимуму и обнаружить»? |
|---|---|---|---|
| `_ckpt` в живой жизни (SK1–SK8, SK16–SK17) | нет. Худшее — ложный `CORRUPTED_DATA` в процессе зомби | любой сетевой сбой | да, уже невозможно по механизму хранилища |
| `_snap` (SK3, SK5, SK6, SK11, SK18) | debris до раунда GC; fault при srid2 | late apply, разрыв | да |
| stale-чтения (SK12–SK14) | ложная тревога rebuild/fsck | конкуренция с GC, без пауз | да |
| SK9 (T5 + ABA) | `_ckpt` воскрешённой жизни испорчен. Её `_snap`/`_log` к этому моменту уже удалены janitor | нужна достижимая T5 (две одинаковые попытки вставки в каталог поперёк отмены) плюс очень поздний `IM` к `_ckpt`. Отдельная пауза уровня SIGSTOP не нужна, нужна сама T5 | нет: закрывать надо T5 (необратимая отставка). Отдельно для `_ckpt` делать нечего |

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| `_ckpt` `GET` (O7) | `gate(reservedFor(0,1))` на монтировании (`CasMountRuntime::admit`: остаток аренды > `E+M`); окно `Retry::standard` 90 s; Poco-таймауты на каждую операцию сокета; connect cap `C`. Чтения GC идут через открытую плоскость, фенса аренды нет (V) | момент, когда хранилище отдаёт снимок тела; устаревание после ответа |
| `_ckpt` `IM`/`INM` (O2–O6) | `gate(reservedFor(0,2))` = `2E+M` перед каждой попыткой (`CasRequests.cpp:writeLoop`); `SingleAttempt`, `object_storage_attempt_timeout_ms=T`, connect cap `C`, `max_unexpected_write_error_retries=1`. Тело ~сотни байт, один send (V) | пауза между `gate` и send; применение после последнего байта; повторы `writeLoop` до 90 s, каждый под своим `gate` |
| `_snap` `INM` (O1) | тот же `gate` `2E+M` один раз на попытку. Таймауты сокета на каждую операцию. Тело до 64 MiB (после zstd меньше) | весь PUT: при медленной сети send идёт кусками, каждый со своим таймаутом → длительность не ограничена `T`. Multipart на ETag-хранилище: части последовательно, каждая — отдельный запрос; `gate` перед частями не вызывается (I по `WriteBufferFromS3`). Позднее применение `CompleteMultipartUpload` |
| `_snap` `WOD` (O9) | нет фенса аренды (GC). `authorityHolds` перед чанком; `Retry::standard` | всё: пауза потока GC и поздний `DeleteObjects` |
| janitor `IM`-delete (O10) | `op.admitted()` (liveness GC) перед удалением | пауза после проверки, позднее применение |

**Перенос дедлайна аренды в транспорт.** Проверка `lease_valid_until − M` перед каждой операцией сокета и таймаут сокета = min(настроенный, остаток):
- Безопасность этих объектов от времени не зависит (§3). Закрывать нечего.
- Что даст: меньше debris в SK5 и SK19; длинный multipart снимка обрывается на сроке аренды, а не в произвольный момент; SK16/SK17 реже доходят до ложного `CORRUPTED_DATA`.
- Что останется: пауза между последней проверкой и `send`; применение после последнего байта (SK5, SK19); весь путь GC (O9, O10) — у него нет аренды монтирования.

## 6. Гарантия на уровне хранилища {#store-level}

| Вариант | Суть | Формат | Миграция | Объём | Риск | TLA+ |
|---|---|---|---|---|---|---|
| A. Оставить + контроль (рекомендую) | всё store-safe. Детекторы: `CASRefCheckpointNotAdvanced`, `CASRefCheckpointIdenticalSkip`; fsck: число `_snap` ниже `checkpoint_snapshot_id` (debris SK5); `_ckpt`-фантомы мёртвых жизней (SK19, уже anomaly janitor); `CORRUPTED_DATA` recovery из `classifyMissingSampledBase` | нет | нет | 0 | нет | нет |
| B. Отказ `mergeCkpt` с учётом фенса | в `publishCkpt::decide` поймать `CORRUPTED_DATA` из `mergeCkpt`; при `!op.admitted()` → `Decline::Fenced`. Убирает ложную тревогу SK17 | нет | нет | ~10 строк, 1 файл (`Pool/CasRefCkpt.cpp`) + gtest | низкий: меняется только класс ошибки уже отклонённой записи | не нужно |
| C. Необратимая отставка жизни | ревизия/nonce в каталоге, чтобы отставленный incarnation не возвращался. Закрывает SK9 и K3/L6 вместе с T5 | меняет тело каталога (FROZEN с 26.6.4) | да | вне объекта | — | по T5 |

Добавлять в `_ckpt` версию или nonce не нужно. Монотонное тело уже служит версией, и ETag от него не повторяется.

## 7. TLA+ {#tla}

| Сценарий | Модель:оператор | Покрытие |
|---|---|---|
| SK1/SK2 (поздний frontier-CAS) | `CaRefTableSnapshotLogCore`: `FrontierIssueCheckpoint`, `FrontierMoveFence`, `FrontierCheckpointLinearizes` (токен `checkpointToken = issuedToken`), свидетель `W_ISSUED_CAS_LINEARIZED_AFTER_FENCE_MOVE` (`_frontier_witness_issuedlinearizes`) | да, для одного писателя. Codex (K1 TLA-WRONG) прав про `FrontierStaleAdvance`, но не видел этих операторов: они моделируют выданный CAS, который линеаризуется после смены фенса |
| SK3/SK4 (поздний CAS снимка) | `WriterCkptAdvance`, `FrontierPublishSnapshotCheckpoint` | нет: атомарное слияние, нет sample/token и нет поздней доставки |
| SK5, SK11, SK12, SK14 | `GcCleanupSnap` + `CleanupSnapGate`, `RFetchBase` (три исхода), `_sab_staleckptcorruption` | частично: граница и ревалидация есть; поздний `WOD` по устаревшему плану и поздний `INM` снимка после удаления — нет |
| SK8 ABA | — | токен — счётчик `@ + 1`, ABA невыразима. Корректно, пока тело монотонно |
| SK9, SK10 | `CaRefNsCleanupStaleLeaderCore:StaleJanitorDelete`, `Recreate` | нет: `Recreate` берёт новый `nextInc`, возврат того же incarnation не моделируется (T5) |
| SK13, SK15–SK19 | — | нет |

Минимальное дополнение (одно на SK3/SK4/SK11): в `CaRefTableSnapshotLogCore` разделить `WriterCkptAdvance` на `Issue` (sample `ckpt` + token) и `Land` (только если token не менялся, слияние от sample). Добавить `StaleGcPlan` (план `S < sampled.base`, удаление позже). Инвариант — существующие `INV_RECOVERY`/`INV_NOFAIL`. Оценка: ~40 строк, один cfg.

## 8. Вывод {#verdict}

- **Безопасность сегодня: safe (store-enforced).** Монотонное тело `_ckpt` + `IM`, write-once снимки с epoch в ключе, удаление строго ниже checkpoint. От времени зависит только классификация ошибок (SK16, SK17).
- Единственный вредный сценарий (SK9) — следствие T5 (ABA каталога), а не дефект этого объекта.
- Действие: вариант A; по желанию B (~10 строк, без формата). SK9 закрывать в T5.
- Расхождения с инвентарём: S1 неполон (SK4: зомби может сделать свой снимок базой, но правдивой). Ссылка на TLA+ в K1 верна частично (см. §7). Комментарий в `CasGc.cpp` («absent checkpoint WIDENS» cleanup) противоречит коду `cleanupRefObjects` (`if (!checkpoint ...) continue`) — только прозаическая ошибка.
