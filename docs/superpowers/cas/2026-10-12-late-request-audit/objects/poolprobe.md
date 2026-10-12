# Аудит позднего запроса: `POOL/_pool_meta` и `POOL/_probe/<uid>/token` (+ `GetBucketVersioning`) {#poolprobe}

База `d76b59f9295`. `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`. **V** — прослежено в коде базы, **I** — вывод.
Сводка: 17 сценариев; безопасность — **безопасно по ключам/токенам** (store-механизм), вред в худшем случае — мусор или отказ старта; одна фактическая дыра в прозе инвентаря (читатели `_pool_meta` недоучтены) и один неограниченный по времени запрос (`GetBucketVersioning`).

## 1. Объект {#obj}

| | `POOL/_pool_meta` | `POOL/_probe/<u128hex>/token` |
|---|---|---|
| Ключ | `Layout::poolMetaKey()` = `<prefix>/_pool_meta` (`CA/Formats/CasLayout.h`) **V** | `config.pool_prefix + "/_probe/" + u128ToHex(probe_uid) + "/token"`; `probe_uid` — два draw `thread_local_rng` на каждый `Pool::open` (`Pool/CasPool.cpp` ~501, `Backend/CasProbe.cpp:runCapabilityProbe`) **V**. Инвентарь пишет `_probe/<uid>` — ключ на уровень глубже. |
| Тело | текст: заголовок + JSON `{pool_id(32hex), blob_header_len, gc_shards, min_reader_generation, algos_used[]}` (`Formats/CasPoolMetaFormat.h`) **V** | литералы `probe-v1`, `probe-v2`, `should-not-land`, `clobbered` **V** |
| Что меняет ETag/generation | единственный не-мутирующий случай — отсутствие записи; запись только (а) create с новым случайным `pool_id` (`mintPoolId`), (б) `admitOrValidate`: union `algos_used` + `min_reader_generation = G_BUILD`, пишется только если algo ещё не член (`CasPoolMeta.cpp:admitOrValidate`) → тело всегда отличается от прочитанного **V** | каждая запись в пределах батареи: `probe-v1`→`probe-v2` меняет токен; повторный create того же `probe-v1` на S3 даёт тот же MD5-ETag (не важно: ключ одноразовый) **I** |
| `pool_id` | также `domain_id` конверта блоба (комментарий в `CasPoolMetaFormat.h`) → замена `_pool_meta` = смена идентичности пула **V** | — |
| Владелец / срок | создаётся один раз при бутстрапе пула (`Pool::open`, writable, `allow_mint=true`), живёт всё время пула. **Ни один продовый код его не удаляет**: `poolMetaKey()` встречается только в `CasPoolMeta.cpp`, `CasPool.cpp` (чтения 123, 271), `ContentAddressedMetadataStorage.cpp:1317`, `CasSentinelProbe.cpp`; `CasDecommission.cpp` и `FORGET` ключ не трогают (FORGET — node-local Vanished, ничего не стирает). Удаляет только оператор (`rm`/erase) **V** | один `Pool::open` на один `uid`; удаляется самой батареей (шаг 7) и `cleanup` (HEAD + `remove(etag)`); крах оставляет мусор. Никто не чистит `_probe/` потом **V** |

## 2. Операции {#ops}

Профиль: условные записи Native — `SingleAttempt` на уровне бэкенда (`ObjectStorageBackend::conditionalWriteSettings`, `s3_max_unexpected_write_error_retries_override=1`) **V**; control-plane чтения — `single_attempt_control_plane` на записываемом Native-монтировании (инвентарь, `ContentAddressedMetadataStorage.cpp:822`) **V из инвентаря**. Над ними цикл движка `CasOperation::writeLoop` (`Backend/CasRequests.cpp`): переотправка тех же байт под тем же условием после неоднозначности, окно `Retry::standard()` = 90 с (`CasRetry.h`). **Весь бутстрап идёт на `Fence::open()`** (`Pool/CasPool.cpp:398`): допуска по остатку аренды нет вообще **V**.

| id | Объект | Актор: функция | Запрос | Профиль / цикл |
|---|---|---|---|---|
| PM-a | `_pool_meta` | `Pool::open` → `probePoolBootstrapResidual` (`CasSentinelProbe.cpp`): точное `read`, при ошибке LIST `page_limit=32` | GET / LIST | `Retry::standard()`; ошибка read → LIST; ошибка LIST → `Indeterminate` (fail-closed) |
| PM-b | `_pool_meta` | `Pool::open` → `PoolMeta::createOrValidate` (`CasPoolMeta.cpp`): `op.read` | GET | `standard()` |
| PM-c | `_pool_meta` | то же: `op.create(key, encodePoolMeta(pm))` при отсутствии и `allow_mint` | условный create `If-None-Match:*` | `writeLoop`, `standard()`; Conflict → `admitOrValidate` |
| PM-d | `_pool_meta` | `admitOrValidate` → `op.readModifyWrite(key, decide, standard())` | GET → `IM(etag)` replace; на конфликт `decide` заново по свежему телу | `writeLoop` под `readModifyWrite` |
| PM-e | `_pool_meta` | `Pool::refreshAdmittedAlgos` (`CasPool.cpp:265`), из `Gc/CasGc.cpp:1471` (fold, при промахе `isAlgoAdmitted`) | GET, `gc_requests.admit()` | `standard()`; монотонный union в `admitted_algos` |
| PM-f | `_pool_meta` | **не в инвентаре PM3**: `probePoolLifecycleGate` (`CasPool.cpp:123`, шаг 0 `tryRemountOnce`) | `probeSentinel` (GET, классификация 404/NoSuchBucket/403/прочее) | `Retry::once()`; решает `Recover` / `Replaced` / `IdentityLost` / `StayTransient` |
| PM-g | `_pool_meta` | **не в инвентаре**: `confirmPoolIdentityForEmptyEnumeration` (`ContentAddressedMetadataStorage.cpp:1317`) | `probeSentinel` | `once`; разрешает «пустой» ответ `listDirectory` |
| PM-h | `_pool_meta` | `Pool::openForDecommission` / read-only open (`CasPool.cpp:935,952`) | GET (`createOrValidate`, `allow_mint=false`) | `standard()`, при отсутствии `INVALID_STATE` |
| PR-a | `_probe/<uid>/token` | `runCapabilityProbe` шаги 1–7 (`CasProbe.cpp`) | create `probe-v1` (INM); read; create dup `should-not-land` (INM, ждём Conflict); replace `probe-v2` `IM(t1)`; read; replace `clobbered` `IM(t1)` stale (ждём Conflict); `remove(t1)` stale (ждём Mismatch); read; LIST; `remove(t2)` (ждём Removed); read; LIST | все `standard()`, на `Fence::open()` |
| PR-b | `_probe/<uid>/token` | `cleanup` в `noexcept` лямбде | HEAD → `IM`-delete | `standard()`, ошибки глотаются |
| PR-c | `_probe/` | `isProbeSubtreeDebris` в `probePoolBootstrapResidual` | LIST (чтение) | игнорирует всё под `<prefix>/_probe/` |
| GV | bucket | `ObjectStorageBackend::checkPoolPreconditions` → `S3ObjectStorage::isBucketVersioningEnabled` (`S3ObjectStorage.cpp:815`) | `GetBucketVersioning` через `client.get()` (обычный клиент, **не** single-attempt) | `S3::Client::doRequest` по умолчанию: `DEFAULT_RETRY_ATTEMPTS=500`, `request_timeout 30 s`, `connect 1 s` (`IO/S3Defines.h`). Не через `CasRequests` |
| C1 | `cas/ref_catalog` | `CasRefCatalog::initializeEmptyForNewPool` (`CasRefCatalog.cpp:72`) INM, `standard()`; Conflict → точное чтение, не-канонически-пустой каталог → `CORRUPTED_DATA` | как PM-c | входит в бутстрап: создаётся **до** `_pool_meta` |
| O1 | `owner` | `claimOwnerOrThrow` — вне зоны (см. `owner.md`); влияет только порядком: после `_pool_meta` | — | — |

Порядок бутстрапа (V, `Pool::open`): residual-проверка (0 записей) → `checkPoolPreconditions` + `checkConditionalWriteSingleAttemptSupport` → `runCapabilityProbe` → каталог INM → `_pool_meta` INM/union → `process_epoch` → claim.

## 3. Сценарии {#scenarios}

«Безопасность сегодня»: S — store (ключ/токен), T — время, ∅ — ничего.

| id | Опаздывает / зомби | Что успели другие | Что делает поздний запрос при применении | Вред | На чём держится | V/I |
|---|---|---|---|---|---|---|
| PM-1 | **Поздний create** `_pool_meta` от бутстрапа A (запрос завис в сети, окно 90 с истекло, A упал/пауза), преемник B уже создал `_pool_meta` | B: residual→каталог→create→mount | INM → 412 | нет | S: INM; `pool_id` случаен 128 бит, байты не совпадут | V |
| PM-2 | То же, но **оператор стёр префикс** (rm -rf) и пул пересоздаёт B, A применяется в пустой префикс до create B | B: пустой префикс, каталог создан, до `_pool_meta` | A создаёт `_pool_meta` со своим `pool_id`; B: INM → Conflict → `admitOrValidate` усыновляет `pool_id` A, `gc_shards`/`blob_header_len` из тела A (`config.gc_shards = meta.gc_shards`) | нет данных потеряно; B открывает пул с константами A (если конфиги различались — молча). Старые серверы со старым `pool_id` получат `Replaced` на ремаунте (штатно) | S: INM, «пул авторитетен» | V (код усыновления), I (расписание) |
| PM-3 | **Поздний create поверх уже существующего/населённого пула**: A прошёл residual (пусто), заснул; пул создан и заполнен B; затем оператор по ошибке удалил только `_pool_meta` | B: каталог непуст | A: каталог INM → Conflict, тело не канонически-пустое → `CORRUPTED_DATA`, `_pool_meta` не пишется. Если каталог тоже удалён — A создаёт новый `pool_id` поверх данных | порча идентичности только при двойной оператор-ошибке + паузе | S: INM на каталоге (V `CasRefCatalog.cpp:initializeEmptyForNewPool`), residual-проверка до записи; второй барьер отсутствует | V (первое), I (второе) |
| PM-4 | **Поздний replace** `admitOrValidate` `IM(e)` (зомби-админ, algo X) | другой узел уже union'ил Y (тело M1, e1≠e) | `IM(e)` → 412; `readModifyWrite` пересчитывает `decide` по свежему телу | нет | S: `IM(etag)`; множество только растёт | V |
| PM-5 | **ETag ABA**: S3-ETag = MD5 тела; тело вернулось к прочитанному `M0` (после M1) только через **восстановление из бэкапа/ручную запись** `M0` | зомби A прочитал `M0`, B union'ил Y → `M1`, оператор вернул байты `M0` | `IM(etag(M0))` проходит, пишет `M0 ∪ {X}` без Y | `algos_used` потерял Y при наличии блобов Y → fold падает `CORRUPTED_DATA "algo not admitted"` (`CasGc.cpp:1471`) после `refresh`; GC wedge до ручного исправления | S ломается только внешним восстановлением; на GCS (generation) невозможно | I |
| PM-6 | **Поздний delete**: отсутствует у CAS; только оператор | — | — | — | продовой записи нет (V grep) | V |
| PM-7 | **Replace по удалённому ключу**: `admitOrValidate`, `_pool_meta` стёрт оператором между GET и PUT | оператор erase | `IM` → 404 → в движке схлопывается в 412 → resolve-read даёт ProvenAbsent → `decide(nullopt)` бросает **`LOGICAL_ERROR` "vanished mid-admission"** | отказ старта; в debug/sanitizer-сборках `LOGICAL_ERROR` — abort | S: нет записи в пустое; но способ отказа — исключение класса логических ошибок на входной (операторской) ситуации | V (код), I (расписание) |
| PM-8 | **Повтор SDK** того же create/replace | — | `SingleAttempt` + `override=1` → SDK не повторяет; повторяет только движок и только после чтения-разрешения. Create: сравнение байт после «ушедшего» токена доказательно (случайный `pool_id`). Replace: `preconditionStillSatisfiable` + `bytes==` | нет | S + рандом `pool_id` | V |
| PM-9 | **Второй экземпляр того же `srid`** бутстрапится параллельно | оба прошли residual | оба пробуют каталог INM (победитель, проигравший читает канон. пустой — проходит), оба пробуют `_pool_meta` INM; проигравший усыновляет. `probe_uid` разные → батареи не пересекаются | нет | S: INM, случайный `uid` (комментарий в `CasPool.cpp` ~493) | V |
| PM-10 | **Устаревшее чтение для решения — ремаунт**: `probePoolLifecycleGate` (PM-f), `once`; ответ `Present(pool_id=X)` пришёл с задержкой, пока пул стёрт и пересоздан (Y) | оператор: erase+recreate | `Recover` по старому `X`; далее fresh-incarnation recovery (claim слота) в чужом пуле. `Replaced` не ставится | старый `pool_id` как `domain_id` в новых блобах: нечитаемые блобы в Y (последствия за пределами объекта) | T: окно = один `Retry::once` запрос (≤ `attempt_timeout`+); окно erase+recreate+старт нового пула короче вряд ли | I |
| PM-11 | **Устаревшее чтение — empty-proof** `confirmPoolIdentityForEmptyEnumeration` (PM-g): положительный ответ задержан, пул стёрт | оператор erase | возвращает «пусто — правда» для `listDirectory` пустого каталога таблицы | пустой список там, где backing стёрт → ClickHouse видит таблицу без частей вместо ошибки (тихая потеря видимости) | T/∅: только свежесть самого GET. Ответ нельзя «закешировать» (UNCACHED), окно = один запрос | I |
| PM-12 | **Stale `admitted_algos`** (`refreshAdmittedAlgos`, GC fold) | другой узел union'ил новый algo | кеш монотонный, при промахе обновляется перед отказом; отказ только на фактически не-члене | нет (нет ложного отказа; ложное принятие невозможно: union-only) | S: монотонность множества | V |
| PM-13 | **Оператор стёр `_pool_meta`, живой писатель**: Live-сервер продолжает писать | erase `_pool_meta` | все его запросы идут в пул без идентичности; обнаружение — через потерю аренды → ремаунт → `IdentityLost` (оба sentinel отсутствуют) | ограничено; внеконтрактно | T: аренда (обнаружение только на ремаунте), не запись объекта | V (`CasPool.cpp:probePoolLifecycleGate`), I |
| PR-1 | **Поздний create** `probe-v1` (шаг 1) или `should-not-land` (шаг 2) применяется после шага 7 | батарея завершена, ключ удалён | ключ пуст → INM успешен → остаётся объект 8–16 байт под `_probe/<uid>/` | мусор навсегда (никто не чистит `_probe/`, V grep) | S: уникальный `uid`; игнорируется `isProbeSubtreeDebris` | V (игнор), I (расписание) |
| PR-2 | **Поздний replace/remove** (шаги 3/4/5/7) после удаления | ключ удалён | `IM` по отсутствующему → 404→412 / `Mismatch`; ничего не записывается | нет | S | V |
| PR-3 | **Неверный вердикт батареи из-за позднего запроса**: поздний первый attempt шага 1 применяется между шагами 2 и 4 | шаг 2 в процессе | на store, **не** исполняющем INM, шаг 2 вернул бы Committed и сразу упал бы; ложный «pass» возможен только на store, возвращающем 412 при реально неисполняемом условии — не воспроизводится одним поздним запросом | нет практического | S + порядок шагов | I |
| PR-4 | **Зомби-бутстрап A после рестарта**: старая батарея пишет в свой `uid` пока B стартует | — | чужой `uid` не пересекается с B; residual B либо видит `_probe/` (игнор), либо LIST-страницы 32 ключа забиты мусором → `page_limit` 32 и LIST сканирует все debris (длинный проход) | задержка старта при большом числе debris | S: уникальные ключи | V |
| GV-1 | **Устаревшее/позднее решение** `GetBucketVersioning` | bucket позже включён | решение один раз на `Pool::open`; ремаунт не перепроверяет | `Enabled` после старта → GC перестаёт освобождать место (token-DELETE архивирует версию); `nullopt` (нет права `storage.buckets.get`) → **mount продолжается с WARNING** (явное fail-open, `CasObjectStorageBackend.cpp:checkPoolPreconditions`) | T/∅: только на старте; на ETag-диалекте (S3) проверка вообще пропущена (`native_token_type != Generation`), покрывает батарея (шаг 7: delete marker → `CAS_DELETE_MARKER`) | V |

Невозможные расписания: поздний delete `_pool_meta` — в коде нет такого запроса (PM-6). SDK-повтор — исключён `SingleAttempt` (PM-8). Для probe «ABA» не применим: ключ одноразовый.

## 4. Нужная строгость {#strictness}

| Объект | Худший вред | Вероятность | Нужно |
|---|---|---|---|
| `_pool_meta` | подмена идентичности (`pool_id`) под населённым пулом; потеря члена `algos_used` (PM-5) | PM-3/PM-5 требуют двойной ошибки оператора + длинной паузы/restore. PM-1/PM-4/PM-8 — обычные сетевые расписания, закрыты `INM`/`IM` | допустимо «минимизировано + обнаружено». Невозможность не нужна: запись редкая (раз на создание + раз на новый algo), оператор-ошибки вне контракта |
| `_probe/` | нет (только мусор) | любой сетевой сбой в батарее | нужна только изоляция ключей — есть |
| `GetBucketVersioning` | GC не освобождает место на версионном GCS | редко, но тихо (нет права → WARNING, а не отказ) | достаточно детектора; жёсткости не требует |

Что реально стоит закрыть: PM-7 (LOGICAL_ERROR на ситуации операторского erase — заменить на штатное `INVALID_STATE`; один `throw`, `CasPoolMeta.cpp`), и недоучёт читателей PM-f/PM-g (PM-10/PM-11 — единственные сценарии «решение по устаревшему чтению»).

## 5. Таймауты {#timeouts}

| Запрос | Чем ограничен сегодня | Не ограничено |
|---|---|---|
| PM-a/b/e/h (reads, `standard()`) | окно 90 с на вызов; attempt и connect — `attempt_timeout_ms`/`connect_timeout_cap_ms` (`controlRequest`); `Fence::open` → без аренды | одно окно 90 с × число последовательных вызовов бутстрапа (residual + battery ~15 запросов + каталог + meta) — суммарная длительность `Pool::open` не ограничена, но и не нужна (аренды ещё нет) |
| PM-c/d (writes) | резерв `reservedFor(0,2)` под `fits(bound)`; attempt timeout из `conditionalWriteSettings`; `100-continue` и Poco per-socket-op как везде | DNS/credentials/STS вне `attempt_timeout`; серверное применение после последнего байта |
| PM-f/g (`once` probes) | один attempt в рамках `attempt_timeout_ms` | задержка применения на сервере (PM-10/11) |
| PR-a/b | как PM-c/d | `cleanup` глотает всё |
| GV `GetBucketVersioning` | только дефолтный клиент: 30 с на attempt, до 500 повторов (бэк-офф SDK) | **ничем не ограничено по сумме**: недоступный endpoint может блокировать `Pool::open` на десятки минут; в `CasRequests` не проходит, нет окна 90 с. Только GCS-диалект |

Проталкивание дедлайна аренды в транспорт: для этих объектов **нечего закрывать** — все сценарии с `_pool_meta`/`_probe` не завязаны на аренду (запись происходит до claim, аренды нет), а зависят от случайных ключей/`INM`/`IM`. Остаётся открытым при любом транспортном дедлайне: пауза между проверкой и `send` и применение на сервере после последнего байта (PM-10/11, PR-1). Для ремаунт-проб (`once`) дедлайн по сути уже есть: один attempt.

## 6. Перенос на уровень хранилища {#store-level}

| Вариант | Формат (FROZEN 26.6.4) | Миграция | Размер | Риск | TLA+ |
|---|---|---|---|---|---|
| A. «Оставить + контроль»: счётчик/лог на `Replaced`/`IdentityLost`, тест `PM-7` (заменить `LOGICAL_ERROR` на `INVALID_STATE`), fsck-проверка: `_pool_meta.pool_id` совпадает с `domain_id` образцов блобов; `algos_used` ⊇ множества алгоритмов в ref-state | не меняет | нет | ~30–60 строк, 2–3 файла | низкий | нет |
| B. Убрать PM-10/PM-11 «по устаревшему чтению»: сверять `pool_id` из `_pool_meta` с `domain_id` в первом же читаемом конверте перед `Recover`/«пусто — правда» | не меняет | нет | ~40 строк в `CasPool.cpp`, `ContentAddressedMetadataStorage.cpp` | средний (лишнее чтение на ремаунте) | нет |
| C. Ревизия в теле `_pool_meta` (монотонный `revision`) для защиты от ABA PM-5 | **ломает** старых читателей, если не терпят неизвестные ключи: `decodePoolMeta` терпит (комментарий: «Unknown JSON keys are tolerated»), значит аддитивно | не нужна для чтения; писатели без поля создают тело без revision | ~20 строк `CasPoolMetaFormat.cpp` + `CasPoolMeta.cpp` | низкий, но покрывает только внеконтрактный restore | малая модель union-only + restore |

Рекомендация: A. B и C не оправданы вероятностью (PM-5, PM-10 требуют внешнего вмешательства оператора).

## 7. TLA+ {#tla}

| Модель | Что покрывает |
|---|---|
| `CaDiskLifecycle.tla` (+`_RESULTS.md`) | переходы состояния пула `IdentityLost` / `VanishedReplaced` / `Closed` после ремаунт-пробы; `witness_racedreplaced`; **не** модель самого объекта `_pool_meta` и его поздних записей |
| `CaErasureProof.tla` | `_pool_meta` + owner схлопнуты в один sentinel `meta`; rev.7 erasure-proof (в rev.8 доказательство стирания удалено); ERASER/RESTORE как среда; полезно для PM-13 и restore-сценария, но не для PM-1…PM-9 |
| `CaRefCatalogCore` (C1) | bootstrap каталога INM |
| не покрыто | PM-1..PM-9 (INM/IM на `_pool_meta`), PM-10/PM-11 (устаревшее чтение для решения), PR-*, GV-1 |

Минимальное дополнение: ~40 строк в `CaDiskLifecycle` — действие `StaleMetaProbe` (ответ `Present(X)` применяется после `ErasePool; RecreatePool(Y)`), инвариант «`Recover` только если pool_id текущий». Отдельная модель для `_pool_meta` не нужна: INM/IM-семантика тривиальна и покрыта общим протоколом.

## 8. Вывод {#verdict}

- `_pool_meta`: **безопасен по store-механизму** (INM на create, `IM` + union-only на admit, случайный `pool_id`, SingleAttempt); сценарии с вредом требуют операторской ошибки (PM-3/PM-5/PM-7) либо окна erase+recreate под одним запросом `once` (PM-10/PM-11, I).
- `_probe/`: безопасен; вред — мусор навсегда (никто не чистит `_probe/`).
- Исправить: PM-7 (`LOGICAL_ERROR` → `INVALID_STATE`, ~3 строки); ограничить по времени `GetBucketVersioning` (single-attempt/окно, ~10 строк, только GCS); дополнить инвентарь строками PM-f/PM-g (читатели `probePoolLifecycleGate`, `confirmPoolIdentityForEmptyEnumeration`) и поправить ключ пробы на `_probe/<uid>/token`.
- Усилия: ≤ 1 день; TLA+ не требуется.
