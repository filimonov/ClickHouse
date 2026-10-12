# Протокол: relink-confirm между серверами и чтения монтирования {#protocol}

База `d76b59f9295`. `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
Строки инвентаря: X1, X2, B7. Темы: T10, T11, Z8, Z9. **V** — прослежено в коде базы, **I** — вывод.

Поправка к брифу: `confirmCommittedRef` на базе нет (имя с `3b4037ae265`); на базе это
`CasRefLedger::confirmExactRef` (`CA/Pool/CasRefLedger.cpp`) (V, `git grep`).

## 1. Объект {#object}

Своего ключа нет. Две «плоскости»:

| Часть | Что это | Что меняет состояние | V/I |
|---|---|---|---|
| relink-confirm (X1) | HTTP-вопрос получателя отправителю: «ваш ref `R` всё ещё связан ровно с `ManifestRef M`?» Ответ `Yes`/`Unproven` в cookie | ответ вычисляется из памяти отправителя: строка ref-таблицы, `pending`/`carved`, `lane_state`, `fence_ok_fn` = `Pool::mayMutate` | V `DataPartsExchange.cpp:Service::answerContentAddressedConfirm`, `CasRefLedger::confirmExactRef`, `CasPool.cpp:237` |
| `mayMutate` | `!mount_fence.lost && bootMsNow() < deadline_boot_ms` — чисто локальное время, без I/O | `deadline` = старт попытки renew + `L` | V `CasMountRuntime::mayMutate`, `MountLeaseRenewer::start` |
| ref-таблица в памяти (X2) | `RefTableState` пространства; единственный писатель — этот mount, поэтому `resolveRef` читает её без I/O; `allow_stale` игнорируется | собственные append'ы; recovery | V `CasRefLedger::resolveRef` |
| манифест (X2) | `cas/manifests/<ns>/<epoch>/<build_seq>/<ordinal>.zst`, ID минтится один раз, тело не переписывается; декод кешируется по ID без запросов | создание (`INM`), удаление GC | V `CasManifestReader::readManifestShared` |
| тело блоба (B7) | `blobs/<algo>/S/<hex>`, ключ = хеш содержимого, payload по фиксированному смещению `blob_header_len` | PUT/повторная публикация (новый конверт, тот же payload), удаление GC | V `CasManifestReader::locate`; I «payload одинаков у всех инкарнаций» |

## 2. Операции {#operations}

| # | Операция | Актор / функция | Тип запроса | Профиль, допуск | V/I |
|---|---|---|---|---|---|
| O1 | offer | отправитель `getRelinkOffer` | ref из памяти (`ForceFresh` обходит только кеш view, не ref-таблицу) + `readManifestShared` (кеш или `GET` через `CasRequests`) | `checkOpAdmitted(ContentRead)` = lifecycle `Live`; `GET` под T1 `reservedFor(0,1)` | V `ContentAddressedMetadataStorage::getRelinkOffer`, `CachedPartFolderAccess::resolve/buildView`, `CasOperation::readLoop` |
| O2 | prepare | получатель `prepareAdoptFromManifest` → `prepareEntries`: `adoptEvidence` (без HEAD), `stageManifest` (`INM`), `precommitAdd` (append, `+1` durable) | записи получателя | T1 получателя | V `PartFolderAccess.cpp:prepareEntries`, `PartWriteTxn::adoptEvidence` |
| O3 | confirm | отправитель `confirmExactRef` | **без I/O**; правила 2–6, правило 6 (`fence_ok_fn`) последним | в памяти; `try_to_lock` на `state_mutex` | V |
| O4 | promote | получатель `PreparedPartWrite::promote` | append owner-move; `TrustedManifest`-листья **без per-file HEAD/loadMeta** | T1 получателя | V `CasPartWriteTxn.h` (док `promote`, шаг 3) |
| R1 | resolve / list | `resolveRef`, `listRefs`, `hasAnyRefWithPrefix` | память; `ensureRefTableRecovered` (I/O только при холодной таблице) | `checkOpAdmitted` | V |
| R2 | манифест | `readManifestShared` | кеш декода по ID → иначе `GET` | `CasRequests`, T1 на каждую попытку; на записываемом Native — `SingleAttempt` | V |
| R3 | тело блоба (прямое) | `readBlobPayload` | `object_storage->readObject` мимо `CasRequests` | только `checkOpAdmitted(ContentRead)`; профиль S3 по умолчанию | V `ContentAddressedMetadataStorage::readBlobPayload` |
| R4 | тело блоба (pipeline) | `DiskObjectStorage::prepareRead` → `getBlobViewPlan` → `ReadPipeline` (fs-cache, distributed cache, gather, prefetch) → `storage->readObject` | обычные ranged GET | `checkOpAdmitted` только в `getBlobViewPlan`, дальше без гейта | V `DiskObjectStorage.cpp:prepareRead`, `ReadPipeline.cpp:buildSource` |
| R5 | ContentAddressedTransaction | `ContentAddressedTransaction.cpp:644` (B7) | `readObject` | как R3 | V (инвентарь) |

Допуск чтений: `checkOpAdmitted` пропускает всё при `PoolLifecycle::Live`. `Live → TransientNotLive` —
только через `noteLeaseLost` из `tripMountLost` (renewer `Terminal`, сбой цикла, teardown), не по
часам. Значит после истечения `deadline` чтения кешей и блобов допускаются, пока renewer не объявил
`Terminal`; `GET` манифеста отсекается раньше — T1 в `readLoop` (V `CasMountRuntime::noteLeaseLost`,
`DriverLease::finish`, `CasOperation::readLoop`; I — точный момент `Terminal` после истечения не прослежен).

## 3. Сценарии {#scenarios}

Ключевой факт (V): получатель делает `+1` durable **до** вопроса (O2 → O3, happens-before через HTTP).
GC при удалении перечитывает in-degree (`CasBlobInDegree.cpp:settleEntry`: `indeg > 0` спасает даже
`delete_pending`). Поэтому `Yes` вредит, только если блобы **уже удалены** до `+1` получателя
(или fold пропустил `+1` — holey LIST, отдельный дефект). Удаление требует: связь `R→M` снята
durable → fold с `indeg=0` (condemn, раунд `N`) → удаление не раньше `N+1`; `gc_interval_sec` = 60
(V `ContentAddressedSettings.cpp`). Итого память отправителя должна отставать от store минимум на
~1–2 раунда GC (≥ 60 с) плюс recovery преемника.

### 3a. Relink-confirm (X1, Z8, T10) {#3a-relink-confirm-x1-z8-t10}

| id | кто опаздывает | что успели другие | что делает опоздавший | вред | на чём держится сегодня | V/I |
|---|---|---|---|---|---|---|
| C1 | отправитель: пауза (SIGSTOP/GC/swap) **после** правила 6, `Yes` уходит позже | преемник того же `srid` взял slot, снял `R`, GC удалил блобы | получатель получает `Yes` поздно и промоутит | нет | правда `Yes` нужна в момент `t0` проверки, а не в момент доставки. В `t0` отправитель ещё законный держатель → связь durable-жива в `t0` (правило 3 исключает свои незавершённые мутации); `+1` получателя durable до `t0` → покрытие блобов непрерывно → GC не удалит (`settleEntry`). Время — только в утверждении «законный в `t0`» | V механизм, I вывод |
| C2 | отправитель: пауза **до** правила 6 (между снимком строк и `fence_ok_fn`) | то же | `fence_ok_fn` читает `bootMsNow()` после паузы → `deadline` прошёл → `Unknown` | нет | правило 6 последним, под обоими mutex; BOOTTIME идёт во время SIGSTOP/suspend | V |
| C3 | сеть: ответ `Yes` доставлен поздно (TCP-буфер, ретрансмиты) | то же | как C1 | нет | как C1 | I |
| C4 | повтор HTTP-запроса confirm (повтор клиента/прокси) | — | идемпотентное чтение | нет | ответ без побочных эффектов; `+1` уже durable | V (обработчик без записи) |
| C5 | отправитель с неверным временем: VM заморожена гипервизором, гостевой BOOTTIME не догнал; дрейф > 5 % | преемник взял slot после окна наблюдателя `L·1.05+P` по своим часам, снял `R`, GC удалил блобы (≥ 60 с) | `mayMutate` = true по замороженным часам → `Yes` из памяти | **висячая ссылка у получателя** (`INV-NO-DANGLE`): манифест получателя указывает на удалённые блобы | только время (допущение о часах) | I |
| C6 | второй экземпляр того же `(uuid, epoch)` (U8/Z16: `claimMount` при том же epoch переписывает slot) | экземпляр B снял `R` (merge+cleanup, `DROP PART`), GC удалил блобы | экземпляр A не знает о записях B, его `mayMutate` = true до своего renew → `Yes` | висячая ссылка у получателя | ничего на уровне store. Практически ограничено: A получает 412 на следующем renew (`throwRenewConflict` → `same_epoch_state_uncertain`) за ≤ `P+T` ≈ 15 с, при транзиентных сбоях renew — до `L` = 30 с; удаление блобов требует ≥ 2 раундов GC (≥ 60 с) | V механизм (`claimMount`, `throwRenewConflict`), I тайминг |
| C7 | отправитель восстановился «не полностью» (holey LIST при recovery) | — | память не равна durable-таблице | висячая ссылка | ничего; вне контракта `confirmExactRef` (сам комментарий это говорит) | V (комментарий), I |
| C8 | GC: fold пропустил `+1` получателя (holey LIST, `_sab_holeylist`) | — | — | висячая ссылка даже при правдивом `Yes` | ничего; известная проблема, не временная | V (комментарии `prepareAdoptFromManifest`, `relinkPartToDisk`) |
| C9 | ABA `ManifestRef`: после декомиссии тот же `srid` снова получает epoch 1 (T12), таблица ATTACH с тем же uuid | старый `M = e:b:o` удалён, новый манифест с тем же `e:b:o` | правило 5 сравнивает `ManifestRef` → совпадение по значению, не по объекту | `Yes` про другой манифест; получатель промоутит свои (верные) entries, но защищённость их блобов не доказана | mint-tightening (`precommitAdd`) — только внутри эпохи; ресет epoch его ломает | I |
| C10 | «следующий слот `(E,T+1)` → 404» как замена времени (Z8, предложение Claude) | преемник снял `R`, `planRefCleanup` удалил печати ниже checkpoint | `404` | ложное доказательство | REFUTED (codex): печати удаляемы, `404` не доказывает отсутствие преемника. Принимаю | V `planRefCleanup` (инвентарь Z1), I |

Вывод по 3a: поздняя доставка (C1–C4) безвредна **по store-механизму** (`+1` до вопроса + повторное
чтение in-degree). Вред только когда неверно само утверждение «в `t0` я законный писатель»: C5 (часы),
C6 (двойник того же epoch), C9 (ресет epoch). C7/C8 — не про время.

### 3b. Чтения монтирования (X2, B7, Z9, T11) {#3b-chteniya-montirovaniya-x2-b7-z9-t11}

| id | кто опаздывает | что успели другие | что делает опоздавший | вред | на чём держится | V/I |
|---|---|---|---|---|---|---|
| D1 | зомби: `SELECT` по ref-таблице в памяти | преемник смёрджил/удалил/мутировал части, вставил данные | отдаёт набор частей на момент своей последней записи | устаревший результат (как отстающая реплика); данные не портятся | ничего; сужено lifecycle-гейтом (после `Terminal` — ошибка) | V `resolveRef`, I вред |
| D2 | зомби: чтение блоба (R3/R4) после удаления GC | преемник снял ссылки, GC удалил блоб | GET → `NoSuchKey` | ошибка запроса, не неверные данные: ключ = хеш, payload по фиксированному смещению | контент-адресация | V ключ/смещение, I |
| D3 | зомби: чтение блоба, который удалён и снова опубликован с тем же содержимым (ETag ABA для тела) | GC удалил, кто-то перепубликовал | GET → те же байты payload (другой конверт) | нет | ключ = хеш; `blob_header_len` фиксирован | V `locate`, I |
| D4 | зомби: ranged-чтение, состоящее из нескольких GET, между которыми блоб удалён | GC | часть диапазонов → `NoSuchKey` | ошибка | как D2 | I |
| D5 | зомби: fs-cache / page cache / distributed cache отдаёт байты удалённого блоба | GC удалил | кеш по ключу объекта → верные байты | нет (кеш даже маскирует D2) | ключ = хеш | V `prepareRead`, I |
| D6 | зомби: декод манифеста из `manifest_cache` после удаления манифеста | GC удалил манифест | отдаёт верный декод (ID никогда не переиспользуется) | нет | минт ID один раз | V `readManifestShared` |
| D7 | зомби: `GET` манифеста после истечения `deadline` | — | T1 в `readLoop` отказывает (`NoBudget`/`FenceLost`) | ошибка (доступность), безопасности не добавляет | время (T1) | V |
| D8 | зомби: `GET` манифеста, удалённого GC | GC | `GET` → 404 → `FILE_DOESNT_EXIST` «INV-NO-DANGLE» + событие `ReadMissing` | ошибка + ложная тревога в `cas_log` о нарушении инварианта | ничего | V `readManifestShared` |
| D9 | ресет epoch (T12): тот же ключ манифеста получил новое тело | декомиссия + новый epoch 1, тот же ns | `GET` → чужое тело; `refMatchesBody` проходит (тот же `ManifestRef`) | **неверный результат** (файлы другой части) | ничего | I |
| D10 | ошибка чтения (D2/D4/D8) используется для решения: `ReplicatedMergeTreePartCheckThread` | — | часть признаётся битой → detach / fetch / «lost forever» | detach — запись CAS, отсекается T1; действия в Keeper не отсекаются CAS-арендой (сессия Keeper зомби обычно уже истекла) | частично T1, частично Keeper-сессия | V (комментарий `checkOpAdmitted`), I |
| D11 | устаревшее чтение для решения: `getRelinkOffer` по устаревшей ref-таблице | преемник снял `R` | offer с устаревшим `M` | сводится к C1–C6: решает confirm | см. 3a | V |
| D12 | второй экземпляр того же `srid` (Z16) читает | другой пишет | D1 у обоих | устаревшие ответы | ничего | I |
| D13 | у получателя после устаревшего `Yes` (C5/C6) | блобы удалены | чтения части → D2 → part check → refetch покрывающей части | доступность; потеря данных нет, если строки есть в покрывающей части преемника. Если `R` снят `DROP`/TTL — ненадолго «воскресают» удалённые данные, если блобы ещё живы через dedup | I |

## 4. Нужная строгость гарантии {#strictness}

- **Relink-confirm.** Худший вред — висячая ссылка в пространстве получателя (нарушение `INV-NO-DANGLE`),
  на практике — битая часть и refetch; durable-потеря только если строк нет больше нигде (I). Нужно
  одно из: замороженные часы VM (C5), двойник того же epoch (C6, уже дефект U8), ресет epoch (C9).
  Короткая пауза или сетевой сбой **не** достаточны (C1–C4). Вероятность низкая, но вред — порча
  графа, который fsck потом показывает. «Минимизировать + обнаружить» приемлемо, пока работает
  допущение о часах; если аренда становится только подсказкой живучести (цель `noclock`), `Yes`
  обязан стать доказанным через store — одна `GET` дёшева (§6).
- **Чтения.** Безопасность данных от аренды не зависит: ключи блобов и ID манифестов неизменяемы,
  чтение даёт верные байты или ошибку (D2–D6). Исключение — D9 (ресет epoch), лечится в T12, не здесь.
  Устаревший `SELECT` (D1) — то же, что отстающая реплика; аренда нужна только чтобы ограничить, как
  долго зомби отвечает. Отказ T1 для `GET` манифеста (D7) добавляет только недоступность. Ответ на
  вопрос «нужна ли чтениям аренда»: для корректности — нет; для свежести — нужен признак «я ещё
  писатель», а не бюджет времени на запрос.

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено | V/I |
|---|---|---|---|
| confirm, отправитель | ничего не нужно: вычисление в памяти, `try_to_lock`, проверка `bootMsNow() < deadline` последней | время от проверки до записи ответа в сокет и доставки | V |
| confirm, получатель | HTTP-таймауты interserver fetch (`replicated_fetches_http_*` / `http_*`); после ответа — собственный T1 на `promote` | возраст `Yes` к моменту `promote` | I |
| `GET` манифеста | T1 `reservedFor(0,1)` перед каждой попыткой; `SingleAttempt` на записываемом Native; попытка `T` = 5 с + connect `C` | применение хранилищем после отправки (для чтения не важно) | V `readLoop`, `ContentAddressedMetadataStorage.cpp:822` |
| тело блоба | таймауты S3-клиента диска, повторы SDK и `ReadBufferFromS3` по умолчанию | всё время запроса `SELECT` (минуты); prefetch-пулы; аренда не участвует | V `readBlobPayload`, `ReadPipeline` |
| ref-таблица | — | возраст памяти; гейт только по lifecycle | V |

Перенос `lease_valid_until − M` в транспорт: для relink-confirm неприменим (отправитель не делает I/O);
сценарии C5/C6/C9 он не закрывает — в C5 врут сами часы, в C6/C9 время ни при чём. Для чтений он
только превращает верные (пусть устаревшие) ответы в ошибки; D1 он не закрывает (память, без
сокета), D9 тоже. Вывод: для этого объекта перенос не нужен.

## 6. Перенос гарантии на уровень хранилища {#store-level}

### A. Confirm проверяет `mount`-slot одной `GET` (рекомендуется) {#option-a}

Перед `Yes` (после правил 2–5 и снимка строк в `t_rows`) — `GET gc/server-roots/<srid>/mount`. `Yes`
только если ETag slot ∈ {ETag, записанные нашим renewer'ом: `last_etag` и ETag ожидающей попытки},
и тело называет наш `(server_uuid, writer_epoch)`. Иначе `Unknown`.

Почему достаточно (I, опирается на V): любой чужой актор, способный снять связь `R→M` в этом
пространстве, сначала меняет slot: преемник — `claimMount`/reclaim (`IM`), GC — фенс-аут
`gc_fenced` (`RMW`), декомиссия — удаление slot. Тело slot включает `seq` и
`newMountWriteAttemptId()` (V `MountLeaseRenewer::start/renew`, `encodeBody`) → у каждой записи
свой ETag, ABA нет. `GET` начат после `t_rows`; slot не изменился к моменту линеаризации `GET` → чужих
записей ref-лога до этого момента не было → связь durable-жива в `t_rows`, а `+1` получателя durable
раньше. Закрывает C5 (часы не нужны), C6 (двойник пишет свой ETag), декомиссию (404 → `Unknown`).
C9 закрывает только если ETag нового slot отличается — да, новое тело (I). C7/C8 не закрывает.

| Параметр | Оценка |
|---|---|
| Формат на store | не меняется |
| Миграция | нет; wire-протокол тот же (`Unproven` уже существует) |
| Код | ~80–120 строк, 3–4 файла: `CasRefLedger::confirmExactRef` (разделить на «снимок» без I/O и «проверку slot» вне `ref_queue_mutex`), доступ к `last_etag`/pending ETag renewer'а (`CasServerRoot`), `CasPool`, gtest в `gtest_cas_confirm_exact_ref.cpp` |
| Риск | ломается контракт «ноль I/O»: удалённый peer может вызвать одну `GET` на запрос. Смягчение: `GET` только на пути к `Yes`; single-flight — confirm использует `GET`, начатую после его `t_rows`; лимит на одновременные. `GET` нельзя делать под `ref_queue_mutex` |
| Задержка | +1 RTT к S3 на relink; `Unknown` при гонке с renew — повтор очереди |
| TLA+ | расширить `CaRelinkConfirmCore` (§7) |

### B. Confirm ждёт следующий успешный renew (без новой I/O) {#option-b}

`Yes` только если renew, **начатый** после `t_rows`, закоммитился (`IM` на slot прошёл → слот наш в
момент линеаризации). Store-гарантия та же, что в A, без новых запросов; цена — задержка до `P` = 10 с
на каждый relink (или досрочный renew, а это опять I/O). Код ~60–100 строк (ожидание
генерации renew в `CasMountRuntime`). Хуже A по задержке; лучше по контракту «ноль I/O».

### C. Получатель проверяет блобы после `Yes` (HEAD каждого) — отклонить {#option-c}

N `HEAD` на часть; присутствие в момент `HEAD` не защищает от уже решённого, но ещё не применённого
удаления GC (точный токен, T1 topic). Остаётся временным; дорого.

### «Оставить как есть + контроль» {#keep}

- fsck: висячий ref получателя уже ловится (`INV-NO-DANGLE`, `CasFsck`); добавить атрибуцию —
  `provenance = Attach` + время promote рядом со временем удаления блобов.
- Счётчик: `Yes`, выданный при `bootMsNow()` ближе `M` к `deadline` (ранний сигнал C5).
- Детектор двойника (C6) — это U8; закрыть U8 (отказ `claimMount` при том же epoch, как на
  `3b4037ae265`) убирает C6 и D12 без A.
- Чтения: `ReadMissing` на удалённом манифесте у зомби (D8) — ложная тревога; помечать события при
  `!mayMutate()` как «после потери аренды», чтобы не путать с реальным нарушением.
- Чтения: снять T1 с `GET` манифеста вне операций записи (D7) — гейт только мешает; оставить
  lifecycle-гейт.

## 7. TLA+ {#tla}

| Сценарий | Модель / оператор | Статус |
|---|---|---|
| C1–C3 (поздняя доставка) | `CaRelinkConfirmCore`: `RPublish` → `RConfirm` (`Gate1Answer`) → `RPromote`; `ConfirmedRelinkNeverDangles` | покрыто косвенно: `RConfirm` атомарен, но `RPromote` может идти после `ForeignRemove`/`GFold`/`GSettle`, значит поздний promote смоделирован; `_sab_publishafterconfirm` доказывает, что порядок «`+1` до вопроса» несущий |
| правило 6 | `FenceLoss`, `_sab_nofence` | покрыто |
| C5, C6 (фенс «да», а преемник уже пишет) | — | **не покрыто**: `ForeignRemove` требует `~sFence`, т.е. модель предполагает, что локальная вера теряется раньше чужой записи (то же замечание у codex: TLA-WRONG для X1) |
| C7 | — | не покрыто (полнота recovery) |
| C8 | `_sab_holeylist` | покрыто (нарушение) |
| C9 | — | не покрыто; относится к T12 |
| `CaRelinkLaneComposition:ConfirmSource` | требует `source_exists` | не про зомби |
| двойник того же epoch в mount | `CaCasMountCore` конфиги `_twins*`, `_witness_twolive` | slot-уровень есть, связи с confirm нет |
| D1–D13 (чтения) | — | не моделируются; моделировать не нужно, кроме D9 (в T12) |

Минимальная добавка к `CaRelinkConfirmCore`: разделить `sFence` (локальная вера) и `slotHolder`
(durable). Новый `ForeignClaim`: `slotHolder' = "succ"`, `slotEtag' = slotEtag + 1`, `sFence`
не меняется (саботаж допущения о часах). `ForeignRemove` требует `slotHolder = "succ"` вместо
`~sFence`. В `Gate1Answer` вариант A: `fenced == sFence /\ slotEtag = sKnownEtag`, отдельный шаг
`SlotRead` между снимком и ответом. Ожидание: без A `ConfirmedRelinkNeverDangles` нарушен,
с A — выполняется без `FenceLoss`. Объём: ~40 строк + 2 cfg.

## 8. Вывод {#verdict}

- Relink-confirm: **безопасно по времени (safe-by-time)**. Поздняя доставка `Yes` безвредна по
  store-механизму (`+1` до вопроса + повторное чтение in-degree в GC). Вред (висячий ref у получателя)
  только при ложном «я законный писатель в `t0`»: замороженные часы VM, двойник того же epoch (U8),
  ресет epoch (T12). `404` следующего слота — не доказательство (REFUTED).
- Рекомендация: вариант A — одна `GET` `mount`-slot со сравнением ETag перед `Yes`. ~100 строк, без
  смены формата, плюс ~40 строк TLA+. Закрыть U8 независимо.
- Чтения: **безопасны без аренды** по данным (неизменяемые ключи → верные байты или ошибка). Вред —
  устаревший `SELECT` и ошибки чтения удалённых блобов (доступность). Исключение — D9 через ресет
  epoch (T12). Действий по аренде для чтений не требуется. Опционально: снять T1 с `GET` манифеста
  вне записей и помечать `ReadMissing` после потери аренды.
