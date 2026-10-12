# Аудит объекта: `epoch` и `owner` плоскости монтирования {#epoch-owner}

База `d76b59f9295` (`cas-janitor-budget`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
Серия аренды `3b4037ae265` упомянута только там, где она меняет вывод. **V** — прослежено в коде базы; **I** — вывод.
Строки инвентаря: E1, E2, E3, O1, O2, O3, MT10. Темы: T6, T12, T13.

## 1. Объект {#object}

| Объект | Ключ | Тело (`Formats/CasServerRootFormats.cpp`) | Что меняет ETag | Владелец | Жизнь |
|---|---|---|---|---|---|
| `epoch` | `POOL/gc/server-roots/<srid>/epoch` (`Layout::epochKey`) | `{next_writer_epoch}` и больше ничего; декодер пропускает неизвестные поля (`decodeServerEpoch` → `skipUnknown`) (V) | только значение счётчика. Тело — функция `N`: одинаковое `N` ⇒ одинаковые байты ⇒ на S3 одинаковый ETag (MD5 одиночного PUT) (V код, I для ETag) | процессы с `uuid` из `owner`; инструмент декомиссии под тем же `uuid` (`Pool::openForDecommission` берёт `uuid` жертвы) (V) | создаётся при первом старте, растёт на каждый старт / self-remount / fence-recovery; **удаляется** декомиссией (`Tools/CasDecommission.cpp:decommissionPoolMember`) (V) |
| `owner` | `.../<srid>/owner` (`Layout::ownerKey`) | `{server_uuid, retired_at_ms?}`; неизвестные поля пропускаются (V) | только tombstone (`retired_at_ms`). Обычный старт owner не переписывает (V) | первый `uuid`, захвативший пустой subtree | создаётся один раз; не удаляется кодом; tombstone пишет декомиссия (V) |

Связанные выводы, на которых держится остальной CAS (V):
- `writer_epoch` монотонен и уникален на `srid`: `chassert(writer_epoch > mount_runtime.liveWriterEpoch())` (`Pool/CasPool.cpp`, self-remount), `prefixEligibleUnder` (`Gc/CasOrphanManifestSweep.cpp`: `prefix.writer_epoch < w.writer_epoch` ⇒ «сборка мертва»), `EpochSeal`/`_ckpt` (`Pool/CasRefCkpt.cpp`, комментарий «durable-monotone»).
- Ключи, скоупленные эпохой: манифест `cas/manifests/<ns>/<E-hex>-<b-hex>/<ord>.zst`; `b` = `next_build_seq`, стартует с 1 в каждом процессе (`Pool/CasMountRuntime.h`). Значит `(ns, E)` целиком определяет пространство ключей манифестов процесса (V).
- Ref-log `cas/ns/stream/<life>/_log/<E-seq>`: `life` = `mintFreshIncarnation()` (`Pool/CasRefCatalog.cpp`) — случайный, эпохой не задаётся (V).

## 2. Операции {#operations}

Все операции ниже идут по плоскости без аренды: `gc_requests` (`Fence::open`) при старте/remount, `CasRequests(..., Fence::open())` в декомиссии (V). Политика `Retry::standard()` = `within(90'000)` — дедлайн по стенным часам, проверяется перед каждой попыткой (`CasRetry.h`, `writeLoop: fits`) (V).

| # | Актор | Функция | Объект | Запрос | Профиль |
|---|---|---|---|---|---|
| E-r | старт, remount, декомиссия | `allocateWriterEpoch` → `readModifyWrite` | `epoch` | GET | engine; SDK — single attempt на записываемом Native (I для бэкенда декомиссии) |
| E1 | то же | `allocateWriterEpoch`, ветка «есть счётчик» | `epoch` | `RMW`: `IM(e_N)` `{N+1}` | условная запись single-attempt SDK (`conditionalWriteSettings`), engine reissue до 90 с; разрешение неоднозначности — чтение + **сравнение байтов** (`writeLoop`, `CasRequests.cpp:1074`) (V) |
| E2 | то же | ветка «счётчика нет»: `serverRootSubtreeEmpty` + `probeSentinel(mount)` → `INM` `{2}` (выдаёт 1), либо `DecommissionRecovery`: `{surviving+2}` | `epoch`, читает `mount`, каталог, LIST `cas/manifests/<srid>/`, `roots/<srid>/` | `RMW` над отсутствием = `INM` | как E1 (V) |
| E3 | декомиссия | `deleteSlotObject(epoch_key, claimed_epoch->etag)` | `epoch` | exact-token DELETE | `Retry::standard` (V) |
| E-cap | декомиссия | `op.read(epoch_key)` под захватом | `epoch` | GET | (V) |
| MT10 | декомиссия | `deleteSlotObject(mount_key, farewell_mount->etag)` | `mount` | exact-token DELETE | (V) |
| E-chk | декомиссия | recheck: `op.read(mount_key)`, `op.read(epoch_key)` | оба | GET ×2 | (V) |
| O1 | старт, remount | `claimOwnerOrThrow` | `owner` | GET; при отсутствии и пустом subtree — `INM` `{uuid}`; на конфликте — повтор проверки пустоты и сравнение `uuid` | `Retry::standard` (V) |
| O2 | старт, декомиссия, `CasPool.cpp:156` | `readOwnerObject` / `readOwnerUuid` / `probeSentinel(owner)` | `owner` | GET | (V) |
| O3 | декомиссия | `op.read(owner_key)` → `op.replace(owner_key, tombstone, owner->etag)` | `owner` | GET + `IM(e_owner)` | `Retry::standard`; ambiguous → resolve по байтам (V) |
| — | ничто | DELETE `owner` | — | нет в коде; только ручная операция по тексту ошибок (`claimOwnerOrThrow`, `mountDoubleStartMessage`) (V) | — |

Порядок хвоста декомиссии (V, `decommissionPoolMember`): capture `epoch` под захватом → `admin.reset()` (farewell: `min_active_build_sequence = UINT64_MAX`) → read farewell `mount` → проверка `mount.writer_epoch == epoch.next-1` → DELETE `mount` по farewell-токену → DELETE `epoch` по захваченному токену → recheck (оба отсутствуют) → read `owner` → `IM`-tombstone. Код сам называет окно recheck→tombstone «ACCEPTED RESIDUAL WINDOW».

## 3. Сценарии {#scenarios}

Сокращения: A, B — процессы с одним `uuid` и `srid` (двойник: общий `/var/lib/clickhouse`, дубль пода, ручной второй старт, инструмент декомиссии против рестарта жертвы). D — инструмент декомиссии. Z — зомби (пауза).

| id | Кто опаздывает | Что успели другие | Что делает опоздавший запрос | Вред | На чём держится безопасность сегодня | V/I |
|---|---|---|---|---|---|---|
| S1 (T6) | A: `IM(e_N){N+1}` теряется или висит до таймаута (любой сетевой сбой, пауза не нужна) | B прочитал тот же `N` и записал те же байты `{N+1}` | A не применяется (412 или потерян). Resolve A видит объект с другим ETag и **своими** байтами → `Committed(resolved_by_read)` → A и B оба получают `N` | база: `claimMount` при `existing.writer_epoch == our_epoch` делает refresh (MT2) → оба держатели до первого renew-конфликта (≤ `P`, `same_epoch_state_uncertain` в `throwRenewConflict`). В окне: общий `min_active_build_sequence` → orphan sweep удаляет in-flight манифест двойника (I, потеря данных); `_files`/`roots` (U3/U4), staging (U9) | **ничего** на базе: ETag изменился, но правило «изменилось + байты совпали = моё» его перекрывает. Серия: второй claim получает `same_epoch_occupied` → `LiveDoubleStart` → обнаружено, не предотвращено | V код (`writeLoop`), V MT2; I вред |
| S1c | то же, ветка «счётчика нет»: два `INM {2}` | B создал `{2}` | resolve A: предусловие «отсутствует» сдвинулось, байты равны → `Committed` → оба epoch 1 | как S1 | как S1 | V код; I сценарий |
| S2 | SDK-повтор того же `IM` | — | условные записи single-attempt SDK (`s3_max_unexpected_write_error_retries_override = 1`), SDK не повторяет; engine-reissue своего применённого запроса → 412 → resolve по байтам → `Committed` — верно для одиночного процесса | нет | `IM` + resolve | V |
| S3 | Z: поздний `IM(e_N)` старого аллокатора | преемник поднял счётчик | 412 → `Conflict` → `decide` по свежему телу | нет | store: ETag меняет каждая выдача | V |
| S4 | Z эпохи-1 пула держит `e({k})` (до декомиссии) | D удалил `epoch`; новая эра снова дошла до `{k}` | на S3 ETag = MD5 тела → `IM(e({k}))` снова совпадает → запись `{k+1}` ложится; Z и новая эра выдают `k-1`… — дубль эпохи | как S1 (двойная эпоха) | **ничего** после сброса; без сброса ABA нет (счётчик монотонен). GCS generation не повторяется → там безопасно | I; нужна пауза через всю декомиссию |
| S5 | A: устаревшее чтение `N` | B поднял | `IM` 412 → re-decide | нет | store | V |
| S6 (T12) | процесс жертвы стартует (рестарт-цикл systemd/k8s; пауза не нужна) | D удалил `mount`, затем `epoch` | `claimOwnerOrThrow` проходит (owner ещё не tombstone). `allocateWriterEpoch`: `epoch` нет, subtree пуст (D его вычистил), `probeSentinel(mount) = KeyAbsent` → `INM {2}` → **epoch 1 снова**. `claimMount`: слота нет → mint `(uuid, 1)` | повторное использование эпох 1…`Ea`. D на recheck видит объекты → «owner kept», выход без tombstone | **ничего**: удаление — это и есть открытие окна; `INM` над пустым ключом проходит | V код; I вероятность |
| S7 | GC: `sweepNamespace` прочитал `floorForNamespace` до сброса (`writer_epoch = Ea`, farewell `MAX`) и задержался | новая эра (epoch 1) пересоздала таблицу и публикует манифест `(ns, 1, b)` до precommit | `prefixEligibleUnder`: `1 < Ea` → «мёртвая сборка»; если protection view и `manifestDeletionPremise` не держат ключ — HEAD + `IM`-delete удаляет live-манифест новой эры | потеря данных / висячая ссылка | без сброса — монотонность эпохи (устаревший пол всегда ниже новой эпохи); со сбросом — только `manifestDeletionPremise` (покрытие seal для `Creating`/новой жизни обычно отсутствует → retain) и время | I; нужна задержка GC на хвост + рестарт + публикацию |
| S8 | Z старой эры (epoch `Ev`, `Ev > 1`) — поздние create манифестов / writer-cleanup (`cleanupStagedManifestDebrisBestEffort`: HEAD → `IM`-delete) | новая эра дошла до `Ev` и пишет те же `(ns, Ev, b, ord)` (`b` с 1) | поздний create → занимает ключ новой эры (её `INM` → `Conflict`, ошибка вставки); поздний HEAD+delete → удаляет манифест новой эры | ошибка вставки; потеря данных | без сброса — скоуп ключа эпохой (ключ только у Z); со сбросом — T1 у `mountRequests` (время) | I |
| S8b | — | новая эра на epoch `< Ev` | debris Z с `Ev` > текущей эпохи: `prefixEligibleUnder` → `false` | утечка, пока эра не перерастёт `Ev` | — | V правило, I сценарий |
| S9 | задержанный GC-воркер удаляет манифест старой эры по токену, записанному при fold (`log_mf_cleanup`) | новая эра записала **побайтно тот же** манифест в тот же ключ (то же имя таблицы, epoch, `b`, данные, напр. восстановление) | MD5-ETag совпал → `IM`-delete удаляет live-манифест | потеря данных | ничего после сброса | I; очень маловероятно |
| S10 | — | новая эра получила `(uuid, 1)`, пара была когда-то GC-fenced | в коде память о фенсах — только тело `mount`, оно удалено → пара снова «чистая»; строки каталога со старым `creator` декомиссия требует удалить | нет в коде (I); в модели — нарушение `FenceCostsEpoch` (см. §7) | условие декомиссии «0 строк каталога жертвы» | I |
| S11 (T13) | D: recheck → `read owner` → `IM`-tombstone (D на паузе, ambiguous reissue, позднее применение) | преемник S6 стартовал после recheck: owner прочитан, `epoch`/`mount` созданы | преемник owner не трогает → `IM(e_owner)` совпадает → tombstone ложится над живым процессом; отчёт `slot_removed = true` | живой сервер с retired owner; следующий его рестарт → `CORRUPTED_DATA` (`throwIfOwnerRetired`), нужна ручная правка; оператор считает слот снятым | **ничего**: межобъектный TOCTOU; код признаёт окно | V код; I вред |
| S12 | Z: процесс прошёл `claimMount`, встал перед `startRenewer`; GC зафенсил слот, D забрал его (fenced → reclaim), прошёл хвост | `mount` и `epoch` удалены | `MountLeaseRenewer::claim`: GET → нет → `INM` mint тела `(uuid, Ev)`; renewer `Active` — Z пишет в снятый корень | до recheck: D выходит «owner kept», слот Z с `epoch` отсутствующим → нормальный старт: `CORRUPTED_DATA` (NormalMount, mount есть, epoch нет); после recheck: tombstone под живым Z; Z пишет в старые жизни удалённых namespaces (debris) | ничего на базе (MT5). Серия: renewer стартует от тела claim, слот не создаёт (`56d876f`) | V код; I сценарий |
| S13 (E2) | D: `DecommissionRecovery` решает «terminal» по `expires_at_ms > now_ms` со сдвигом часов | живой член с `mount`, без `epoch` | выдаёт `surviving.writer_epoch + 1` (отличная эпоха), затем `claimMount` NoWait без `proven_dead`/`unsafe` → reclaim только при `gc_fenced`/clean → отказ, `ABORTED` | нет (сожжена одна эпоха; `epoch` создан — это даже снимает wedge S12) | store + сильный гейт `claimMount` — часы не свидетель захвата (codex OVERCLAIM подтверждён) | V |
| S14 (O1) | B: `INM` owner с другим `uuid` | A создал owner | 412 / resolve: байты другие → `Conflict` → foreign → `CORRUPTED_DATA` | нет | store: `INM` + сравнение `uuid` | V |
| S14b | двойники с одним `uuid` | — | байты owner идентичны → оба принимают; безвредно само по себе, но это вход в S1 | — | — | V |
| S15 | Z: поздний `INM` owner (`uuid_old`) | оператор вручную удалил owner по инструкции из сообщения об ошибке, новый `uuid` ещё не стартовал | создаёт owner старого `uuid` | новый сервер получает foreign owner → fail-closed (wedge) | `INM` | I; пауза + ручная операция |
| S16 | D: tombstone `Unresolved` (бюджет 90 с вышел), запрос применяется позже | преемник стартовал | то же, что S11 | как S11 | ничего | I |
| S17 | D: поздний DELETE `mount`/`epoch` | преемник: reclaim `mount` (новые `seq`, `write_attempt_id`) или bump `epoch` | 412 → `Mismatch` → хвост стоп | нет | store: ETag меняет преемник; ABA у `mount` нет (`write_attempt_id` случайный); у `epoch` — только через сброс (S4) | V |
| S18 | второй экземпляр, тот же `srid`, другой `uuid` | — | `claimOwnerOrThrow` → foreign → отказ до любой записи | нет | store (`owner`) | V |
| S19 | второй запуск D (Z) держит `e0` owner до первого tombstone | оператор вручную снял tombstone побайтно в исходное тело | MD5-ETag снова `e0` → повторный tombstone | лишний wedge | ничего | I; пренебрежимо |

Невозможные расписания:
- Поздний `IM` над `epoch` при монотонном счётчике (S3/S5): каждое применение меняет значение, а значение задаёт байты → токен старого значения не вернётся без сброса (V).
- Поздний DELETE `owner`: в коде нет DELETE owner (V).
- Захват `srid` другим `uuid` через `epoch`: `allocateWriterEpoch` вызывается только после `claimOwnerOrThrow` (V: `mountWritable`, self-remount).

## 4. Нужная строгость {#strictness}

| Класс | Худший вред | Вероятность в проде | Достаточно «минимизировать + обнаружить»? |
|---|---|---|---|
| S1/S1c (T6) | на базе: два держателя одной `(uuid, epoch)` до ≤ `P` = 10 с → удаление in-flight манифеста двойника, потерянные обновления `_files` | нужен двойник того же `uuid` (ошибка развёртывания, ровно то, от чего защищает слот) + одновременный старт (k8s дубль-под, systemd + ручной старт стартуют в одну секунду) + один таймаут/сброс соединения в окне ≥ attempt timeout. Пауза процесса не нужна | на серии — да: второй claim отказывается (`same_epoch_occupied`), два писателя невозможны, ошибка видна. На базе — нет. Сама выдача должна быть уникальной, фикс дешёвый (§6 B) |
| S6 (T12) | повтор эпох → ломается посылка монотонности (`prefixEligibleUnder`, `chassert` remount, `EpochSeal`); прямой вред только через S7–S9 | окно = от DELETE `epoch` до recheck (сотни мс), но вход не требует паузы: достаточно, чтобы сервер жертвы не был остановлен и перезапускался в цикле | нет: монотонность эпохи — опорное допущение. Должно быть невозможно |
| S7, S8, S9 | потеря данных новой эры | нужна задержка GC-воркера или зомби, перекрывающая хвост декомиссии и рестарт (секунды–минуты SIGSTOP) | закрывается вместе с S6 (без сброса эти расписания безвредны по скоупу ключа) |
| S11/S16 (T13) | живой сервер с retired owner; fail-closed на следующем рестарте, ручная правка | окно recheck→apply tombstone; рестарт-цикл жертвы или поздний apply | допустимо «обнаружить», но фикс S6 закрывает и это бесплатно |
| S12 | зомби пишет в снятый корень, wedge следующего старта | пауза между `claimMount` и `startRenewer` ≥ порога фенса GC + вся декомиссия | закрыто серией (MT5) |
| S4, S15, S19 | дубль эпохи / лишний wedge | долгая пауза + сброс/ручная операция | закрывается §6 A+B |

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| GET `epoch`/`owner`, `probeSentinel(mount)`, LIST префиксов в `serverRootSubtreeEmpty`, чтение каталога | `Retry::standard` 90 с (проверка перед попыткой); на попытку — Poco send/receive timeout на каждую операцию сокета, connect `C`, адаптивный первый таймаут (`isFirstAttemptFuseTimeout`) (V движок; I конфиг) | DNS, получение credentials, LIST-страницы: каждая отдельная попытка ограничена только socket-таймаутами на операцию; аренды нет |
| `IM`/`INM` `epoch`, `INM` owner, `IM` tombstone | то же + single-attempt SDK; reissue движка до 90 с; резерв `reservedFor(0,2)` проверяется против `Fence::open` → всегда `Ok` | момент применения хранилищем после последнего байта; пауза между `gate` и `backend->write` |
| DELETE `mount`/`epoch` декомиссии | то же | то же |

Перенос дедлайна аренды в транспорт (`lease_valid_until − M` перед каждой операцией сокета): **не применим к этому объекту**. Все запросы идут по плоскости без аренды: при старте аренды ещё нет (её устанавливает `claimMount` после выдачи эпохи), в хвосте декомиссии её уже нет (`admin.reset()` до удалений) (V). Ни один сценарий §3 не временной: S1 — неразличимость байтов, S6/S11 — межобъектный TOCTOU, S4/S9/S19 — ABA. Единственное, что транспортный дедлайн мог бы ограничить, — задержанные GC-воркеры S7/S9 (если GC-запросы привязать к аренде лидера), но разрыв «проверка → `send`» и позднее применение после последнего байта остаются.

## 6. Перенос гарантии на уровень хранилища {#store-level}

### A. Не удалять `epoch`: снимать слот одной `IM`-записью `epoch` в retired-тело (рекомендовано) {#a-ne-udalyat-epoch-snimat-slot-odnoy-im-zapisyu-epoch-v-reti}

- Хвост: capture `epoch` под захватом (как сейчас) → `admin.reset()` → `IM(claimed_etag)` `epoch := {next_writer_epoch: K, retired_at_ms}` — **точка линеаризации** → tombstone `owner` (для старых бинарников) → DELETE `mount` по farewell-токену (или оставить farewell).
- `allocateWriterEpoch`: retired-тело → `CORRUPTED_DATA` с текстом `throwIfOwnerRetired`.
- Почему закрывает: каждый старт пишет `epoch` (V) → преемник до точки меняет ETag → retire получает 412 → D выходит; преемник после точки видит retired → отказ. Счётчик не исчезает → нет повторной выдачи epoch 1 (S6), нет ABA (S4), S7–S9 безвредны по скоупу ключа, окно tombstone (S11/S16) исчезает: owner-tombstone становится подсказкой, решение принимает `epoch`.
- Формат: новое необязательное поле `retired_at_ms` в `ServerEpoch`. Старые читатели не ломаются (`skipUnknown`, V), но **старый бинарник проигнорирует retire** и выдаст эпоху — поэтому tombstone `owner` остаётся (его старые бинарники соблюдают, V `throwIfOwnerRetired`). Миграция не нужна. Повторный запуск декомиссии: retired `epoch` = «уже снят», идемпотентно.
- Остаток: S12 (зомби-renewer mint) — закрыт серией, не A. DELETE `mount` оставляет `mount` отсутствующим над retired `epoch`; `probeSentinel` в ветке «нет счётчика» больше не нужен для снятых корней.
- Объём: ~40–60 строк `Tools/CasDecommission.cpp` (хвост становится короче: уходят recheck и «ACCEPTED RESIDUAL WINDOW»), ~15 строк `Formats/CasServerRootFormats.*`, ~10 строк `allocateWriterEpoch`; 3–4 gtest (retire vs bump до/после, повторный запуск, старый формат без поля). Риск низкий. TLA+: см. §7.

### B. Нонс выдачи в теле `epoch` {#b-nons-vydachi-v-tele-epoch}

- `{next_writer_epoch: N+1, alloc_id: <random128>}`. Resolve по байтам в `writeLoop` отличает свою запись от записи двойника → S1/S1c невозможны; тело больше не функция `N` → ETag-ABA S4 невозможен и без A.
- Формат: аддитивное поле; старые читатели пропускают (V). Смешанные версии: старый писатель пишет без нонса — байты нового писателя с ним никогда не совпадут → новый не присвоит чужое. Старый писатель против нового двойника сохраняет баг (только на время апгрейда).
- Объём: ~15 строк (формат + генерация в `decide`). Риск минимальный. Тот же класс «байты — функция прочитанного значения» стоит проверить у других `RMW`-объектов (вне этого аудита).

### C. Перевыпуск выше потолка (отклонено) {#c-perevypusk-vyshe-potolka-otkloneno}

- Хранить потолок эпох в `owner` и при отсутствии `epoch` выдавать `ceiling+1` (модельный follow-up `followup-remintepoch-distinct-mint` в `CaCasMountCore_RESULTS.md`). Требует второго объекта в одной транзакции и не закрывает S11. A проще.

### D. Оставить как есть + контроль {#d-ostavit-kak-est-kontrol}

- `cas-fsck`: `owner` есть, не retired, `epoch` отсутствует → алерт «сброс эпохи»; `mount.writer_epoch` меньше максимальной эпохи среди манифестов/`_ckpt` корня → «регрессия эпохи».
- Событие `cas_log` + ProfileEvent на `allocateWriterEpoch`, разрешённую чтением (`Committed.resolved_by_read`), и на ветку `INM` при отсутствующем счётчике с неизвестным потолком.
- Серия уже даёт `same_epoch_occupied` (S1 становится видимым отказом старта).
- Обнаружение S11: при старте `owner.retired_at_ms` есть, а `epoch`/`mount` моложе tombstone — показать оба времени в ошибке.

## 7. TLA+ {#tla}

| Сценарий | Модель / оператор | Статус |
|---|---|---|
| S1/S1c | `CaCasMountCore`: `AllocAttempt`, `AllocResolve`, `AllocSettle` (resolve по значению — ровно правило кода); `W_SameBytesCollision` (`_witness_samebytes`, нарушен — расписание достижимо); `_sab_refreshownpair_immalloc` → `ClaimNeverRegressesEpoch` нарушен (= база с MT2); `_twins_immalloc` зелёный при строгом claim серии. RESULTS: «exclusivity is `ClaimMount`'s strict monotonicity, not the counter's» | покрыто |
| S3/S5 | `AllocAttempt` (ветка `epoch # allocPend[a]`) | покрыто |
| S6 | `WipeEpoch` + `RemintEpoch` (литерал 1); `_sab_epochreset` → `WriterEpochMonotoneUnique`; RESULTS: `FenceCostsEpoch` нарушается повторной выдачей 1 поверх ранее зафенсенной пары | частично: `WipeEpoch` — абстрактная потеря, не хвост декомиссии; twin-конфиги идут с ограничением `NoEpochWipe` — сброс × двойники/зомби не проверялся |
| S13 | `RemintEpochDecom` (терминальность с `Drift`) + гейт `ClaimMount` | покрыто |
| S12 | `AdoptRead`/`AdoptWrite`, `_witness_delayedstart` | частично: удаления слота декомиссией в модели нет (`ClearExpiredMount` — удаление по часам наблюдателя) |
| S11, S16, S19 | — | не моделируется: `owner` без tombstone |
| S7, S8, S9 | — | не моделируется: в `CaCasMountCore` нет манифестов и пола |
| S4 | — | не моделируется: токены `epoch` не отделены от значения |

Минимальное дополнение:
1. В `CaCasMountCore`: `owner.retired`; разбить декомиссию на `DecomCapture`, `DecomDeleteMount`, `DecomDeleteEpoch`, `DecomRecheck`, `DecomTombstone` с захваченными токенами; история `usedEpochs[uuid]`. Инварианты `EpochNeverReused` (`localEpoch` нового инкарнейшна ∉ `usedEpochs`) и `RetiredOwnerHasNoLiveMount`. Ожидание: красный на базе (S6, S11). Вариант A — одно действие `DecomRetireEpoch`; ожидание: зелёный. Снять `NoEpochWipe` с twin-конфига для этого прогона.
2. Для B: в `AllocSettle` разрешать по паре (значение, нонс); `_witness_samebytes` должен стать недостижимым.
3. S7: в модели orphan sweep (`CaGcRootLocalPartManifestCore` или новый маленький) — чтение пола отдельным шагом от удаления + эпоха, которая может уменьшиться. Нужен только чтобы показать, что после A посылка монотонности снова верна.

## 8. Вывод {#verdict}

- T6 (S1): **небезопасно на базе** — неоднозначная запись + идентичные байты двойника дают двум процессам одну эпоху, MT2 пускает обоих в слот; пауза не нужна. На серии — обнаруживается отказом claim. Фикс: нонс в `epoch` (B), ~15 строк, формат аддитивный.
- T12/T13 (S6, S11): **небезопасно**, защиты нет ни в хранилище, ни по времени: удаление `epoch` перевыпускает epoch 1 тому же `uuid`, tombstone ложится над живым преемником. Прямая потеря данных — только через задержанные GC-воркеры/зомби (S7–S9). Фикс: не удалять `epoch`, снимать слот `IM`-записью retired-тела (A), ~70–90 строк, формат аддитивный, без миграции.
- `DecommissionRecovery` по часам (S13): безопасно, часы не авторизуют захват. Owner bootstrap (S14/S18): безопасно.
- Рекомендация: A + B одним PR, плюс расширение `CaCasMountCore` (§7 п. 1–2). Итог: **unsafe → store-enforced**, ~1 день кода + ~1 день модели.
