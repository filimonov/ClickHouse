# Аудит `gcctl`: плоскость управления GC и опоздавшие запросы {#gcctl-audit}

База `d76b59f9295` (`cas-janitor-budget`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
**V** — прослежено в коде базы (`file:function`); **I** — вывод. Код не исполнялся, TLC не запускался.
Строки инвентаря: GS1–GS4, H1–H2, MS1–MS2, A1–A6. Соседние объекты (`.meta`, блобы, каталог, `mount`) —
только как ссылки; их разбирают другие аудиты.

## 1. Объект {#object}

| Ключ | Тело, влияющее на ETag | Кто пишет | Время жизни |
|---|---|---|---|
| `POOL/gc/state` | `GcState`: `lease{owner, seq}`, `round`, `snap_generation`, `snap_attempt`, `snap_pruned_through`, `manifest_sweep_cursor`, `gc_shards`. Каждая запись меняет `seq` (renew/steal) или `round` (коммит) → байты уникальны, ABA нет (V `Gc::acquireOrRenewLease`, `Gc::runRegularRound`) | лидер GC; `rebuildBaseline` | вечно; в проде не удаляется (V: `gcStateKey` вне `CasGc.cpp` только читается) |
| `POOL/gc/hb` | `GcHeartbeat{owner, hb_seq}`, `hb_seq` монотонен (V `Gc::pulseHeartbeat`) | поток пульса любого планировщика с `i_am_leader` | вечно |
| `POOL/gc/maintenance_state` | `GcMaintenanceState{janitor_cursor}` — **только курсор, без счётчика**. Одинаковый курсор → те же байты → тот же ETag на S3 (ABA возможна) (V `CasGcMaintenanceStateFormat.h`) | janitor (`NamespaceJanitor::runOnePage`) | вечно |
| `gc/gen/<g>/attempt/<a>/fold_seal` | детерминированное тело фолда; ключ write-once | лидер с `a = lease.seq`; rebuild с `a = max(attempt_of)` (1..k) | до prune поколения `g` |
| `gc/gen/<g>/attempt/<a>/blob_target/<sh>/<seq>` | run in-degree, write-once | как выше | как выше |
| `gc/gen/<g>/attempt/<a>/outcomes/<round>/<sh>.zst` | аудит исходов, write-once | лидер | как выше |

Идентичность лидера: `gc_id` — случайный u128 на экземпляр планировщика (V `CasGcScheduler::CasGcScheduler`);
второй процесс с тем же `srid` получает другой `gc_id`. `REBUILD` берёт одноразовый случайный `gc_id`
(V `ContentAddressedMetadataStorage::runGcRebuildNow`).

Плоскость запросов: `Pool::gc_requests` — фенс открыт, `admit` отказывает только после `beginTeardown`
(V `Pool::Pool`, лямбда `teardownBegun() ? LostOrRearmed : Ok`). Резерв времени (`needed`) игнорируется.
На этой же плоскости идёт bootstrap монтирования (`claimOwnerOrThrow`, `allocateWriterEpoch`, `claimMount*`
в `Pool::open` / remount) — это объект аудита `mount`, здесь не разбирается.

## 2. Операции {#operations}

Профили (V): условные записи — `SingleAttempt` на уровне SDK (`conditionalWriteSettings`), над ними цикл
движка `CasOperation::writeLoop` с повтором тех же байт под тем же условием и разрешением по чтению;
`Retry::standard()` = окно 90 с на вызов, `Retry::once()` = одна попытка (`CasRetry.h`).
Попытка ограничена `attempt_timeout_ms` (T = 5 с) + connect (C).

| id | Объект | Актор / функция | Запрос | Профиль | Защита |
|---|---|---|---|---|---|
| GS1 | `gc/state` | `Gc::acquireOrRenewLease` (renew / steal / create) | `RMW`: decide → `IM`/`INM`, на конфликте decide заново по свежему телу | standard | store |
| GS2 | `gc/state` | `Gc::runRegularRound` коммит раунда | `IM(state_etag)` из renew этого раунда | standard | store |
| GS3 | `gc/state` | `Gc::rebuildBaseline` | `IM(state_etag)` после своего `acquireOrRenewLease(allow_steal=false)` | standard | store |
| GS4a | `gc/state` | `Gc::refreshAuthority` (janitor, drain) | `GET` → флаг `authority_held` | standard | — (check-then-act) |
| GS4b | `gc/state` | `cleanupRefObjects::authorityHolds` перед каждым чанком | `GET` каталога + `gc/state` | standard | — (check-then-act) |
| GS4c | `gc/state` | `readAdoptedFoldSeal` (orphan sweep), `dropNamespaceImpl` (`removal_started_round`), fsck, `previewDeletes` | `GET` | standard | — |
| GS4d | `gc/state` | `Pool::currentGcRound` | `GET` | — | **в проде не вызывается** (V: единственные вхождения — объявление и определение) |
| H1 | `gc/hb` | `Gc::pulseHeartbeat` (поток `heartbeatLoop` + `onLeaseAcquired`) | `GET` + `IM`/`INM` | `once`, исход отброшен | store, монотонный `hb_seq` |
| H2 | `gc/hb` | чтение в decide `acquireOrRenewLease` | `GET` | standard | — |
| MS1 | `maintenance_state` | `runOnePage`: публикация курсора; сброс в `""` при порче и при ошибке LIST | `IM`/`INM` | standard / `once` (сброс при LIST) | store, leak-only |
| MS2 | `maintenance_state` | `readGcMaintenanceState` | `GET` | standard | — |
| A1 | runs | `foldDeltasIntoGeneration` → `putDeterministicArtifact` | `INM` + byte-adopt, иначе `CORRUPTED_DATA` | standard | store |
| A2 | `fold_seal` | `fold` фаза `fold_seal_write`; `rebuildBaseline` | `INM` + byte-adopt | standard | store |
| A3 | outcomes | `runRegularRound` | `INM`; на конфликте — принять существующее тело для подсчёта | standard | store |
| A5a | `gc/gen/<g>/` | `pruneSupersededGenerations` (до коммита) | LIST + `IM(listed etag)` | standard | store + `referenced_generations` |
| A5b | `gc/gen/<g>/` | hand-off reclaim (после коммита) | LIST + `IM(listed etag)` | standard | store + разность seal-ов |
| A6 | `gc/gen/*` | `readFoldSeal`, `newestFoldSealRef`, `probeGenerationForSeal`, stream runs | `GET`/LIST | standard | — |

Деструктивные шаги раунда, опирающиеся на лидерство (V `Gc::runRegularRound`, порядок фаз):
`pre_fold_ref_drain` (каталог, `refreshAuthority` перед каждым erase) → `heartbeat_floor` (фенс-аут
`mount`, вне объекта) → fold (запись артефактов под своим `attempt`) → `pending_deletes` (только
`delete_pending` родителя, точный токен) → outcomes → `meta_pool_wait` → prune → **CAS `gc/state`** →
hand-off → manifest `WOD` → janitor (одно чтение авторитета на страницу) → `cleanupRefObjects` (чтение
авторитета на чанк) → orphan sweep (точный токен).

## 3. Сценарии {#scenarios}

L1 — свергнутый лидер (пауза SIGSTOP/GC/swap после `acquireOrRenewLease`, либо поток старого раунда);
L2 — новый лидер (украл после двух наблюдений замёрзших `(owner, seq)` и `gc/hb`).
«Окно кражи» ≈ 2 тика цикла по `gc_interval_sec` (60 с) при замёрзшем пульсе (`hb_interval` = interval/4).

| id | Кто опаздывает | Что успели другие | Что делает опоздавший при применении | Вред | Безопасность держится на | V/I |
|---|---|---|---|---|---|---|
| S1 | L1: коммит раунда `IM(e_s)` (пауза перед отправкой) | L2 украл: `gc/state` = `(L2, s+1)`, новый ETag | 412 → resolve read → `ABORTED` «gc/state moved» | нет; артефакты L1 — в своём attempt | ETag `gc/state` меняет кража L2 (`RMW` пишет `seq+1`) | V `runRegularRound` |
| S2 | L1: коммит раунда, первая попытка ушла, ответа нет; движок исчерпал окно (`GaveUp`), раунд брошен; запрос применяется позже | никто не писал `gc/state` | коммит ложится: раунд `r+1` принят, хотя L1 считает его проваленным | утечка: пропущены post-CAS hand-off и `WOD` манифестов `mf_cleanup`; манифесты подберёт orphan sweep, поколение hand-off — только fsck | все артефакты раунда записаны до отправки коммита (порядок фаз), следующий раунд читает свежий `gc/state` через `RMW` | V порядок; I вред |
| S3 | L1: тот же поздний коммит, но L1 уже начал следующий раунд | renew L1 `IM(e_s)` | линеаризуемо: кто первый — тот проходит; второй получает 412, `RMW` renew решает заново по свежему телу | нет | одно условие `IM(e_s)` на оба запроса | V |
| S4 | L1: renew/steal `RMW` применяется поздно | L2 уже украл/продлил | 412 → decide по свежему телу: владелец чужой, кортеж сдвинут → отказ | нет | `RMW` | V `acquireOrRenewLease` |
| S5 | повтор SDK / движка того же коммита или renew | — | условная запись `SingleAttempt`; движок повторяет те же байты под тем же `IM`, ложится максимум одна; свой исход опознаётся по байтам | нет; байты уникальны (`owner`, `seq`, `round`) | уникальность тела + `IM` | V `writeLoop` |
| S6 | ABA `gc/state` | — | невозможна: каждая запись двигает `seq` или `round`; `REBUILD` ставит `round = max(...)+1` | — | монотонные поля тела | V |
| S7 | L1: пре-CAS prune поколений по устаревшему `state` (порог `g1+1−keep`) | L2 продвинул поколения до `G2`, его seal ссылается на runs | L1 удаляет `(p1, g1+1−keep]` кроме ссылок своего родителя и своего нового seal | нет: runs seal-а L2 либо унаследованы из seal-а `g1` (защищены), либо записаны L2 в поколениях `≥ g1+1` > порога | цепочка: renew L1 — `RMW` по текущему → состояние L1 — префикс цепочки L2; `keep ≥ 1` | V код; I цепочка |
| S8 | L2 prune удаляет родителя L1 (`g1`) и артефакты L1 в `g1+1` | L2 сделал ≥ `keep`=3 фолда | L1 читает родительский seal → нет → `CORRUPTED_DATA` «GC bookkeeping is corrupt … recover with SYSTEM CAS GC REBUILD» (`fold`, `listRefPrefix`, `drainCompletedRemoving`) | ложная тревога о порче; `REBUILD` откажет («another GC leader holds the lease») | fail-closed throw до любого удаления из этого seal | V |
| S9 | L1: поздние `INM` артефактов (runs/seal/outcomes) в `gc/gen/g1+1/attempt/s/` | L2 уже обрезал поколение `g1+1`, курсор `snap_pruned_through` прошёл его | объект ложится в обрезанное поколение | **утечка навсегда**: prune идёт только вперёд, hand-off — только для поколений, на которые ссылался seal; fsck `gc/gen` не проверяет (V grep `Tools/`) | ничего (leak-only) | V механизм; I частота |
| S10 | L1: `pending_deletes` по `delete_pending` родителя, позже L2 | L2 удалил тот же токен (`Absent`), писатель перепубликовал блоб новым конвертом | `HEAD` → токен ≠ → `Mismatch` | нет, пока новый токен ≠ старому | точный токен + свежий конверт писателя | V `performRedeleteIo`; ABA токена — объект «блоб» (T1/U1) |
| S11 | L1: тот же redelete при ETag-ABA блоба | SDK-повтор старого PUT восстановил прежний ETag | удаление по совпавшему токену | висячая ссылка | не держится (U1); устаревший лидер только расширяет окно | I; см. аудит блоба |
| S12 | L1: erase строки `Removing` каталога после `refreshAuthority` (пауза между GET и `IM`) | L2 украл; барьер L2 `drainCompletedRemoving` тем же родителем удаляет ту же строку до любого фолда | `IM(catalog etag)` → 412 (строка уже снята, тело каталога иное) или ложится раньше L2 — тот же результат | нет | pre-fold барьер: L2 не публикует seal с hold, пока строка с готовым доказательством не снята; ETag каталога | V `drainCompletedRemoving`, `deleteCompletedRemovingAtSnapshot`; модель `CaRefPreFoldDrainCore` |
| S13 | то же, но каталог вернул те же байты (ABA каталога) | снят ряд, затем воссоздан тот же ряд | поздний erase ложится | снятие живой строки | не держится — тема T5 каталога | I |
| S14 | L1 в post-CAS хвосте раунда `r` (hand-off, `WOD` манифестов, janitor, ref cleanup, orphan sweep) после кражи | L2 провёл раунды `r+1..r+k` | удаления по фактам, принятым коммитом `r` | нет: цепочка L2 растёт из `r`; поколение hand-off не упомянуто ни одним последующим seal; ключи манифестов и логов write-once | необратимость принятых фактов + write-once ключи | V код; I цепочка |
| S15 | L1: `WOD` манифеста из `mf_cleanup` после долгой паузы | декомиссия + перевыпуск epoch 1 тем же uuid (U13) | ключ `<E-hex>-<b-hex>` переиспользован → удаление живого тела | потеря данных | не держится — тема U13/T12 | I |
| S16 | L1: janitor-страница; авторитет взят до паузы | L2 украл | `op.admitted()` внутри страницы читает кэшированный флаг → продолжает точные удаления dead-life | нет, пока жизнь необратимо мертва | точный токен + необратимость отставки жизни (T5 — оговорка) | V `runNamespaceJanitorPage`, `runOnePage` |
| S17 | L1: поздняя публикация курсора `IM(e_old)` | L2 прошёл круг, курсор вернулся к тем же байтам (ABA на S3) | курсор прыгает назад/вперёд | leak-only: пропуск/повтор страницы до следующего круга | ничего; вред ограничен | V формат; I вред |
| S18 | janitor: ошибка LIST → сброс курсора в `""` (`Retry::once`) | — | круг начинается заново | лишние LIST, без вреда | — | V `runOnePage` |
| S19 | L1: пульс `gc/hb` после кражи | L2 пульсирует | пульс L1 с `owner = L1` двигает пару | живучесть: контендеры (и сам L1 до следующего раунда) видят «жив» и не крадут у умершего L2 ещё ≥1 окно | монотонный `hb_seq`; `i_am_leader` сбрасывается на следующем раунде L1 | V `heartbeatLoop`, `acquireOrRenewLease` |
| S20 | живой лидер с остановленным пульсом > окна (голодание потока, сбой PUT `gc/hb`) | контендер крадёт | два лидера одновременно | только потраченный раунд: коммит проигравшего — 412, его пре-CAS действия безопасны по S7/S10/S12 | один CAS на раунд, attempt по `seq` | V; `CaGcLeaseCore:NoEpochCollision` |
| S21 | второй экземпляр с тем же `srid` | — | свой случайный `gc_id` → обычный контендер | нет | случайный `gc_id` | V |
| S22 | `REBUILD` против свергнутого L1 в одном поколении | `gc/state` отсутствует (катастрофа), rebuild пишет `g = max_gen+1`, attempt 1..k | L1 пишет в `g` с `attempt = seq`; при совпадении `a` — расходящиеся байты | `CORRUPTED_DATA` у второго писателя; смешения нет. Коммит L1 — `IM` по отсутствующему ключу → 412 | byte-adopt `putDeterministicArtifact` | V; I частота (нужен `seq ≤ k`) |
| S23 | устаревшее чтение `gc/state` для `removal_started_round` | раунд сдвинулся | в строку каталога пишется меньший раунд | только диагностика «stuck removal» | поле не участвует в решениях удаления | V `dropNamespaceImpl`, `reportStuckRemovals` |
| S24 | устаревшее чтение seal-а orphan sweep (`gc/state` → seal; seal обрезан между GET) | prune | `nullopt` → нет покрытия → номинаций нет | нет (retain) | fail-safe | V `readAdoptedFoldSeal` |
| S25 | поток старого раунда (meta-writer, redelete-воркер) в следующем раунде | — | невозможно: `SCOPE_EXIT drainOnExitNoThrow`, `waitForAllToFinish` до продолжения | — | happens-before в процессе | V `runRegularRound`, `redeleteBlobs` |
| S26 | поздний `IM`-delete из `deletePrefixWholesale` | ключ `gc/gen/<g>/…` пересоздан | невозможно по смыслу: ключ `(g, a)` пишет только лидер с `seq = a`, после prune `g` никто туда не пишет, кроме зомби S9 | — | write-once ключи | V `Layout`; I |
| S27 | поздний `INM` создания `gc/state` | другой создал | 412 → decide: объект есть → обычная ветка | нет; повторное «исчезновение» после наблюдения — `CORRUPTED_DATA` | `RMW` | V |

Итого 27 сценариев. Вред по данным — только через чужие темы (S11 → U1, S13 → T5, S15 → U13). Собственный
вред объекта: утечка навсегда (S9), утечка до fsck (S2), ложная тревога о порче (S8), leak-only курсор (S17),
живучесть (S19).

## 4. Нужная строгость гарантии {#strictness}

| Свойство | Худший вред | Как достижимо | Требование |
|---|---|---|---|
| исключительность лидера | — (безопасность от неё не зависит, S20) | ложная кража при голодании пульса > 60 с | «минимизировано + видно» достаточно; уже так |
| коммит раунда (GS2) | — | — | store-enforced, сделано |
| артефакты зомби (S9) | утечка run-ов одного раунда (размер — дельта шардов, до МБ) навсегда | пауза L1 ≥ окно кражи + 3 фолда L2 ≈ 5 мин, попадающая между стартом фолда и записью артефактов | «обнаружено + подбирается» достаточно |
| ложная тревога (S8) | оператор видит «bookkeeping is corrupt», может звать `REBUILD` | та же пауза, но до чтения родителя | исправить классификацию; невозможность не нужна |
| поздний коммит (S2) | утечка одного hand-off поколения | `GaveUp` коммита при сетевом сбое — любая заметная сетевая проблема | «минимизировано + обнаружено» (fsck) |
| курсор janitor (S17) | задержка уборки на круг | ABA на S3 + поздний запрос | оставить как есть |

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| `gc/state` RMW / `IM` | окно `Retry::standard` 90 с на вызов; попытка T+C; SDK single-attempt; допуска по времени нет (открытый фенс) | момент применения на стороне хранилища; пауза процесса между decide и отправкой |
| `gc/hb` | одна попытка (`once`), T+C | то же |
| `maintenance_state` | 90 с / `once` | то же |
| артефакты `INM` | 90 с на объект; число объектов — по числу шардов и run-ов | длительность фолда целиком (нет дедлайна раунда; бюджеты раунда — в штуках, не во времени) |
| prune / hand-off LIST+delete | 90 с на каждый LIST и delete; число — бюджеты `gc_round_prefix_wholesale_budget`, `..._handoff_...` | суммарное время |
| весь раунд | ничего | всё |

Перенос дедлайна аренды в транспорт: у GC **нет дедлайна** — аренда GC без часов (наблюдение двух тиков),
аренда монтирования GC-плоскость не гейтит (V `Pool::Pool`). Переносить нечего. Если привязать GC к аренде
монтирования (`lease_valid_until − M` перед каждой операцией сокета), это закроет только фенс-аут
`mount` зомби-лидером (U7, чужой объект). S9, S2, S17 не временные: они остаются при любом дедлайне,
потому что пауза между проверкой и `send` или позднее применение после последнего байта по-прежнему возможны,
а вред — утечка, не потеря.

## 6. Перенос гарантии на уровень хранилища {#store-level}

| Вариант | Что закрывает | Формат | Миграция | Объём | Риск | TLA+ |
|---|---|---|---|---|---|---|
| A. Периодический sweep `gc/gen/<g>/` для `g ≤ snap_pruned_through`, не упомянутых в принятом seal (LIST верхнего уровня `gc/gen/` раз в N раундов, удаление по `listed etag`) | S9, S2 (hand-off), фиксирует «fsck — единственный подборщик» | нет | нет | ~40 строк, 1 файл (`CasGc.cpp`) + тест | низкий: удаляет только поколения ниже курсора и вне `referenced_generations`; та же логика, что prune | дописать в `CaB140DangleMerge` или новый мини-модуль: зомби-запись в обрезанное поколение + sweep, инвариант «ссылка seal-а не удаляется» |
| B. Переклассификация S8: перед `CORRUPTED_DATA` о пропавшем родителе перечитать `gc/state`; если `(owner, seq)` ≠ своему — бросать `ABORTED`/retry-later «deposed» | S8 | нет | нет | ~20 строк, 1 файл, 3 места бросания (`fold`, `listRefPrefix`, `drainCompletedRemoving`) | низкий; fail-closed сохраняется | не нужна |
| C. Счётчик/раунд в теле `maintenance_state` | S17 | **да** (формат заморожен с 26.6.4; старый декодер упадёт на новом поле → janitor сбросит курсор как Corrupt, leak-only) | нет | ~15 строк, 2 файла | низкий, но выигрыш ≈ 0 | не нужна |
| D. Оставить как есть + контроль | S9, S2, S17 | нет | нет | fsck: проверка «объекты под `gc/gen/<g>/` при `g ≤ snap_pruned_through` и `g ∉ referenced`» + счётчик `CASGCDeposedAtCommit` (412 на коммите раунда) | — | — |

Рекомендация: B + (A или проверка fsck из D). C не делать.

## 7. TLA+ {#tla}

| Сценарий | Модель:оператор | Покрытие |
|---|---|---|
| S1, S4, S20 | `CaGcLeaseCore:Retire`, `ObserveOrSteal`, `NoEpochCollision`, `NoFalseSteal` | атомарный CAS; поздняя доставка не моделируется. Расхождения: в модели `hbAdvanced` требует `hbOwner = stOwner`, код намеренно сравнивает только пару наблюдённого `(hb.owner, hb_seq)`; в модели отдельный `stFence`, в коде его роль играет `seq` |
| S7, S10 (по смыслу) | `CaGcAckFloorZombie:GBegin`, `GFinish`, `INV_NO_DANGLE` | свергнутый лидер удаляет только опубликованные `pending`; ABA токена исключена моделью (`nextTok`) |
| S12 | `CaRefPreFoldDrainCore:DeleteSuccess` (позднее применение выпущенного запроса), `AdoptHeldOverDeposedRequest` (саботаж), `DeleteUsesCurrentAdoptedProof` | моделирует ровно поздний erase свергнутого; `_safe` зелёный, `_sab_stale_delete_after_successor_hold` красный (RESULTS) |
| A1–A3 | `CaGcRootLocalPartManifestCore:GMintAttempt`, `GAdopt`, `GDeposedFinalWrite` | невидимость неприн. attempt; prune поколений не моделируется |
| S16, S17 | `CaNamespaceJanitorCursorCore:DeferPage`, `AuthorizedFoldPage` | только фазировка DEFER/fold; позднего курсора и ABA нет |
| S9, S8 | нет. Цитата инвентаря `CaB140DangleMerge:GTrim` для A5 неверна: `GTrim` режет ref-log ниже курсора, не поколения GC | — |
| S2 | нет (коммит в моделях атомарен) | — |
| S14 | нет | — |

Минимальное дополнение: модуль из 3 переменных (`gens`, `pruned_through`, `refs_of_adopted`) и действий
`Fold(l)`, `Commit(l)`, `Prune(l)` по устаревшему виду, `ZombieWrite(l, g)`, `Sweep`; инварианты
«`refs_of_adopted` ⊆ живые объекты» (S7) и liveness «объект в `g ≤ pruned_through` вне ссылок со временем
удалён» (S9, красный без варианта A). ~120 строк, конфиги `_safe`/`_sab_nosweep`.

## 8. Расхождения с инвентарём {#inventory-diff}

| Строка | Было | На базе |
|---|---|---|
| GS4 | `Pool::currentGcRound` — читатель для `precommitAdd` | **V**: в проде не вызывается; комментарий о «более консервативном» направлении к тому же обратный (устаревшее чтение даёт меньший раунд) |
| GS3 | rebuild конкурирует с регулярными раундами | **V**: одноразовый `gc_id` + `allow_steal=false` ⇒ при любом существующем `gc/state` с владельцем ветка всегда «incumbent renewed» → отказ «another GC leader holds the lease». `REBUILD` реально выполняется только при отсутствующем `gc/state`; «healthy FORCE REBUILD» на живом пуле не выполнится. Не про опоздание, но влияет на S8 (оператор не сможет «починить» ложную тревогу) |
| MS1 | «janitor проверяет liveness перед каждым запросом» | **V**: liveness — кэшированный `authority_held`, обновляется один раз на страницу; проверки внутри страницы ловят только teardown. Вред нет (S16) |
| A5 | TLA `CaB140DangleMerge:GTrim` | **TLA-WRONG**: `GTrim` — обрезка ref-log |
| H1 | `CaGcLeaseCore:ObserveOrSteal` покрывает | частично: правило `hbAdvanced` в модели строже кода (см. §7) |
| новое | — | S8: ложный `CORRUPTED_DATA` у свергнутого лидера, родитель которого обрезан преемником |
| новое | — | S2: поздно применённый коммит после `GaveUp` теряет post-CAS хвост (утечка до fsck) |

## 9. Вывод {#verdict}

- **Безопасно (store-enforced).** Данные не зависят от исключительности лидера: один `IM` на `gc/state`
  за раунд, артефакты под `attempt = seq`, пре-CAS удаления только по ранее опубликованному с точным токеном,
  pre-fold барьер каталога. Время влияет только на живучесть (кража) и на фенс-аут `mount` (U7, чужой объект).
- Вред по данным возможен только через чужие дыры: ETag-ABA блоба (U1), ABA каталога (T5), переиспользование
  epoch (U13).
- Собственные остатки: утечка навсегда поздних артефактов зомби (S9), утечка hand-off после позднего коммита (S2),
  ложная тревога о порче (S8), leak-only ABA курсора (S17).
- Действия: B (переклассифицировать S8, ~20 строк) и A (sweep `gc/gen` ниже курсора, ~40 строк) или проверка fsck.
  Формат не меняется. ~1 день вместе с тестами.
