# Аудит объекта `POOL/cas/ref_catalog`: поздние запросы {#catalog-audit}

База `cas-janitor-budget` @ `d76b59f9295`. `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
**V** — прослежено в коде базы (функция названа); **I** — вывод. Строки инвентаря: C1–C8, L6, K3, P4; темы T4, T5; Z7.

## 1. Объект {#object}

| Что | Значение | V/I |
|---|---|---|
| Ключ | `POOL/cas/ref_catalog` (`Layout::refCatalogKey`), один на пул | V |
| Тело | текст `cas_ref_catalog`: заголовок, записи `entry` в каноническом порядке по `ns`, трейлер-счётчик (`encodeRefCatalog`) | V |
| Поля строки | `ns`, `state` (`Creating`/`Live`/`Removing`), `incarnation` (случайные 128 бит, `mintFreshIncarnation`), `creator` = `(server_root_id, writer_epoch, fence_generation)` только при `Creating`, `removal_started_round` только при `Removing` | V `CatalogEntry` |
| Ревизия / nonce | **нет**. Байты — чистая функция множества строк | V `encodeRefCatalog`, `RefCatalog` |
| Что меняет токен | на ETag-сторе (S3, MinIO): ETag = MD5 тела одночастной записи → **токен = функция байтов**; одинаковые байты дают тот же ETag | V диалект `Dialect::Generation` vs ETag в `CasObjectStorageBackend.cpp`; MD5-семантика — I (свойство S3, кроме SSE-KMS) |
| | на generation-сторе (GCS): каждая запись — новая generation → ABA невозможна | V `conditionalWriteSettings` (`x-goog-if-generation-match`) |
| Строгость декодера | неизвестный `kind`, мусор после записи, неизвестное слово состояния → `CORRUPTED_DATA` | V `decodeRefCatalog`, `nsStateFromWord` |
| Владелец | общий для пула: пишут все монтирования (жизненный цикл namespace) и GC-лидер (стирание `Removing`) | V |
| Время жизни | создаётся при bootstrap пула, никогда не удаляется | V `initializeEmptyForNewPool` |
| Индекс жизней | `CatalogLifeIndex` включает строки **всех** состояний, в том числе `Creating` | V `CatalogLifeIndex::CatalogLifeIndex` |

Ключевое наблюдение. `decide` у всех мутаций — функция байтов каталога. На ETag-сторе `IM(e)` ≡ «тело байт-в-байт равно X».
Поэтому поздний `IM`-запрос, применённый над тем же X, делает то же, что сделал бы вовремя.
Вред возможен, только если смысл записи зависит от фактов **вне** байтов каталога:
- аренда (`op.admitted()`, T1);
- сертификат смерти создателя;
- история жизни: «этот `incarnation` уже объявлен мёртвым, janitor удаляет его объекты».

## 2. Операции {#operations}

Транспорт всех условных записей: `ObjectStorageBackend::write` → `conditionalWriteSettings`: `SingleAttempt` (`max_retries = 0`), `s3_max_unexpected_write_error_retries_override = 1`, `attempt_timeout_ms` = T, connect ≤ C (V). Повтор делает только движок: `CasOperation::writeLoop`. После неясной попытки он читает ключ. Если предусловие ещё выполнимо, он **шлёт те же байты с тем же `IM`** (V `writeLoop`: ветки `pauseAndReissue`, `reissueAtOnce`, `pauseFlat` по connect-hint — последняя без чтения).

| id | Операция | Актор, функция | Запрос | Повторы над HTTP | V/I |
|---|---|---|---|---|---|
| C1 | bootstrap пустого каталога | `CasRefCatalog::initializeEmptyForNewPool` из `Pool::open` | `INM`, `Retry::standard` | `writeLoop` | V |
| C2 | шаг 1 создания: вставка `Creating` | `createNamespace` → `createNamespaceStep1` → `casUpdateImpl` → `CasHotKeys::submit` | RMW: `IM` от тела из lane-кеша или чтения. `mutate` бросает `CatalogEntryAlreadyPresentMarker`, если строка есть | `writeLoop` + цикл `casUpdateImpl` на `Conflict` (до 100 попыток, общий замороженный дедлайн) | V |
| C3 | `Creating → Live` | `completeCreation`: сначала `publishCkpt` (шаг 2), потом RMW | RMW: `op.admitted()` + строка `== observed` по значению | то же | V |
| C4 | перехват создателя | `reconcileStaleCreator` из `CasRefLedger::resolveNamespaceLife` | RMW: `op.admitted()`, строка `== observed`, `isCreatorFenceTerminal` (GET слота `mount`) внутри `mutate` | то же | V |
| C5 | отмена `Creating` | `cancelStalledCreating` из `dropNamespaceImpl` | RMW, удаляет строку; те же три проверки | то же | V |
| C6 | `Live → Removing` | `beginRemoving` из `dropNamespaceImpl` | RMW: `op.admitted()` + строка `== observed` | то же | V |
| C6b | разрешение неясного C6 | `dropNamespaceImpl`, `catch (...)`: свежий `read`. `Removing` → успех; `Live == observed` → **lane открывается снова** | `GET` | — | V |
| C7 | стирание завершённого `Removing` | `deleteCompletedRemovingAtSnapshot` из `CatalogLifecycleReconciler` | `replace` с `IM(e_snapshot)` + `refresh_authority` + `op.admitted()` + `cleanup_evidence` из fold seal, затем обязательное чтение | `writeLoop` в цикле до 100 | V |
| C8 | чтения | `CasRefCatalog::read` (GC fold, janitor, drop, cold admission, `resolveNamespaceLife`, терминальная проверка перед append) | `GET`, `Retry::standard` | engine read loop | V |
| L6/K3/P4 | удаление объектов мёртвой жизни | `NamespaceJanitor::runOnePage` | LIST `cas/ns/` → `CasRefCatalog::read` (срез **после** LIST) → `HEAD` при отсутствии etag → `op.remove(key, etag)` = `IM`-delete | `Retry::standard`; `liveness` проверяется перед каждым запросом | V |
| — | курсор janitor | `casGcMaintenanceState` | `IM(progress.etag)` | — | V |

## 3. Сценарии {#scenarios}

Обозначения вреда: **loss** — потеря данных, **res** — воскрешение, **wedge** — таблица/строка застревает, **debris** — мусор, **none**.

| id | Кто опаздывает | Что успели другие | Что делает поздний запрос | Вред | Защита сегодня | V/I |
|---|---|---|---|---|---|---|
| A1 | C1 поздний bootstrap `INM` (второй opener) | каталог создан | 412. Разрешение сверяет байты с каноническим пустым | none | store `INM`; каталог не удаляется | V |
| A2 | C2 шаг 1, зомби-пауза между `gate` и отправкой | ничего с каталогом. Создатель A уже fenced | ложится, только если тело == X: вставляет `Creating(e, creator=A)` со свежим `e` | none: у `e` нет объектов, строку перехватит следующий opener (`SupersededEpoch`/`GcFenced`) | store (`IM` = равенство байтов) + свежесть `e` | V механизм, I вред |
| A3 | C2, **ABA через повтор движка** (codex). Попытка 1 принята хранилищем, применяется поздно; resolve-чтение видит X; попытка 2 (те же байты, `IM(e_X)`) ложится | A умер до шагов 2/3 (crash/farewell). Чей-то `DROP` → `cancelStalledCreating` → тело снова X, ETag снова `e_X`. Janitor считает `e` мёртвой | попытка 1 ложится: `Creating(e, creator=A)` **воскрешена** | **res**. Следующий opener перехватывает и рождает `e` заново. Страница janitor, взявшая срез до воскрешения, удаляет `_ckpt` жизни `e` по листинговому etag. Если `publishCkpt` нового создателя дал identical-skip, etag не изменился → `_ckpt` у `Live` пропадает → recovery: `CORRUPTED_DATA` (`chooseRecoveryGrounding`) → **wedge**. Данных пользователя в `e` к этому моменту нет | **ничего** на ETag-сторе. На GCS — store (generation). Время: хранилище должно применить попытку 1 позже смерти A и отмены — секунды–десятки секунд | V механизм (`writeLoop` повторяет те же байты; `encodeRefCatalog` без ревизии), I вред |
| A4 | C2, дубль того же PUT прокси/балансировщиком (PUT идемпотентен по HTTP) | как A3 | как A3, без участия движка | как A3 | ничего; SDK не повторяет (`SingleAttempt`), прокси вне контроля | I |
| A5 | C2, попытка шага 1 применяется после **всего** цикла жизни `e` (Live → Removing → терминал → стирание C7 вернуло X) | жизнь `e` прожита и стёрта, её лог/`_files` — debris | воскрешает `Creating(e)` поверх следа мёртвой жизни. Перерождённая жизнь читает старый лог с `RemoveNamespace` (aliasing), janitor параллельно удаляет | **res** + aliasing + wedge | ничего, кроме того, что хранилище не держит запрос минуты | I; на практике невозможно |
| A6 | C3 поздний go-live зомби A | никто не перехватил | ложится над `Creating(e, A)` → `Live(e)` с `_ckpt` от A | none: преемник A' открывает `Live`, recovery запечатывает epoch A (L1/L2) | store | I |
| A7 | C3 поздний go-live после `reconcileStaleCreator` | B переписал `creator` | 412 → `Superseded` | none | store: строка изменилась | V |
| A8 | C4 поздний reconcile зомби-реконсилера B (пауза после `gate`) | никто | ставит `creator=B(epoch_B)`; B уже fenced | none: следующий opener перехватит ещё раз (`SupersededEpoch`) | store + монотонность терминальности | V механизм, I вред |
| A9 | C4/C5: «терминальность» прочитана в `mutate`, применена позже | слот `mount` создателя ожил: тот же `(uuid, epoch)` (T7) или декомиссия + epoch 1 заново (U13) | перехват/отмена живого создателя → сирота, janitor съест его `_ckpt`, поздние записи — debris | loss `_ckpt` + wedge (C5 инвентаря) | **T3**, плюс монотонность epoch | I; см. T7/T8/U13 |
| A10 | C5 поздняя отмена зомби D | никто | ложится над `Creating(e, A)`: отмена с A терминальным — корректна | none | store + монотонная терминальность | I |
| A11 | **C6, Z7/T4**: зомби A прошёл `gate`, пауза ≥ аренды | B забрал тот же `srid`, пишет в ту же жизнь. Строка `Live(e)` не меняется: takeover каталог не трогает | `beginRemoving` ложится: `Removing(e)` над живой таблицей B. Терминальный append A: отказ `refuseUnlessAdmitted`, иначе 412 от печати B | **wedge**: следующий positive append B видит не-`Live` и навсегда закрывает lane; после рестарта `resolveNamespaceLife` → «is Removing, retry later». Обратного перехода `Removing → Live` нет. Данные целы: без терминала нет `cleanup_evidence` | **T1** (резерв `2E+M` в `gate`); store не защищает | V механизм (`beginRemoving` сравнивает по значению; positive-append-проверка; `CasGc` ставит `cleanup_evidence` только по сложенному терминалу), I вред |
| A12 | C6 + поздний терминал A (вторая пауза после `refuseUnlessAdmitted`), B ещё не трогал `ns` (печати нет) | — | терминал ложится → GC выдаёт `cleanup_evidence` → C7 стирает строку → janitor удаляет жизнь | **loss** с точки зрения клиента: `DROP` вернул ошибку, но выполнен целиком. Намерение было `DROP` | T1 дважды | I |
| A13 | C6 без зомби (поздний запрос живого процесса): `GaveUp(Deadline/Unresolved)` при живой попытке | resolve-чтение `dropNamespaceImpl` увидело `Live == observed` и **снова открыло lane** | попытка ложится позже → `Removing`; вставки отказывают на следующем positive append | wedge до повторного `DROP`; `DROP` сообщил ошибку | ничего; повтор `DROP` пользователем | V (`dropNamespaceImpl` catch), I |
| A14 | C6 + ABA: занятый пул | между `gate` и применением другие `ns` прошли «добавление + отмена», тело вернулось к X | как A11 — ABA расширяет A11 на пулы с DDL-активностью | wedge | T1 | I |
| A15 | C7 поздний erase свергнутого GC-лидера | новый лидер сложил новые раунды | ложится только над тем же телом. `cleanup_evidence` монотонна (терминал не отменяется) → erase корректен в любой момент | none | store + монотонность доказательства | V (`deleteCompletedRemovingAtSnapshot`) |
| A16 | L6/K3/P4: поздний delete janitor (свергнутый лидер, задержанный поток страницы) по мёртвой жизни | — | `IM(listed etag)`: удаляет объект мёртвой жизни | none | store: смерть необратима; LIST идёт до среза каталога, строка `Creating` защищает новорождённого (индекс включает `Creating`) | V порядок, I вред |
| A17 | L6 + воскрешение A3/A4 | `e` снова в каталоге | удаляет листинговые объекты `e`, etag которых не изменился (на деле `_ckpt` от A) | wedge (как A3) | ничего | I |
| A18 | поздняя запись зомби в мёртвую жизнь (`_ckpt`, лог, `_files` через устаревший `lifeIfCataloged`) | строка стёрта | создаёт объект без владельца | debris; удалит следующий проход janitor | store: жизнь вне каталога | V |
| A19 | поздняя публикация курсора janitor | новый лидер продвинул курсор | `IM(progress.etag)`; при совпадении (ABA на обороте курсора) — откат или пропуск диапазона | debris до следующего оборота | store | I |
| A20 | устаревшее чтение: cold admission / `resolveNamespaceLife` берёт `Live` из старого среза | строка уже `Removing` или стёрта | positive append перечитывает каталог перед выдачей id и отказывает | none | повторное чтение (V positive-append-проверка в `CasRefLedger.cpp`) | V |
| A21 | устаревший кеш hot-key lane | каталог изменился | `IM` со старым etag → 412 → `decide` по свежему телу | none | store | V `casUpdateImpl` |
| A22 | повтор SDK | — | условные записи и чтения каталога идут `SingleAttempt`; `WriteBufferFromS3`-цикл ограничен до 1 | none; единственный «повтор тела» — повтор движка (A3) | V `conditionalWriteSettings`, `getSingleAttemptClient` |
| A23 | второй экземпляр того же `srid`/epoch | оба создают один `ns` | второй видит строку на свежем чтении → `Superseded`; оба дропают → `AlreadyRemoving` | none | store + перепроверка в `mutate` | V |

Проверка утверждения codex (T5):
- «Нет поля ревизии; добавление + удаление восстанавливает байты и ETag» — **V** (`encodeRefCatalog`) для ETag-сторов. На GCS неверно.
- «Две одинаковые попытки вставки обходят отмену» — **V**: `writeLoop` повторяет те же байты под тем же `IM`, пока resolve-чтение видит старый токен.
- «Janitor удаляет объекты перерождённой жизни» — **I**, но уже: удаляются только объекты, перечисленные до воскрешения с неизменным etag. На практике это `_ckpt`, а не данные пользователя.
- Новые жизни, рождённые после LIST, страница не видит. Следующая страница видит `e` в каталоге.

Исправления инвентаря:
- Заголовок раздела «любая запись меняет ETag» ложен на ETag-сторе.
- C2 «над неизменённым каталогом» точнее читать как «над байт-идентичным каталогом».
- C5 цитирует `CaRefCatalogCore:EntryDelete`, но это стирание `Removing`. Аналог отмены в модели — `ReconcileCreating` (он удаляет строку).

## 4. Нужная строгость {#strictness}

| Класс | Худший вред | Что нужно для расписания | Вердикт |
|---|---|---|---|
| A3/A4/A17 (ABA шага 1) | wedge перерождённой таблицы, без потери данных пользователя | хранилище применяет полностью принятый PUT позже: смерти создателя (crash/farewell или `gc_fenced` ≥ `L·1.05+P` ≈ 41.5 s), `DROP` застрявшей `Creating`-таблицы и чтения-разрешения. На AWS S3 — практически нет. На S3-совместимых сторах при brown-out — редко. Ещё нужен `DROP` именно `Creating`-строки | «минимизировать + обнаруживать» приемлемо. Дешёвый фикс есть (6.2), берём его |
| A5 | aliasing | задержка запроса на весь цикл жизни (минуты+) | невозможно практически; закрывается тем же фиксом 6.1 |
| A11/A14 (Z7) | wedge живой таблицы преемника; ручной выход — повторить `DROP` (данные удаляются) или правка каталога | SIGSTOP/GC-пауза дольше остатка аренды (≥ 16 s после `gate`) плюс takeover; либо задержка применения на хранилище в десятки секунд. Тихой потери нет | «минимизировать + обнаруживать»: wedge громкий, вред ограничен таблицей, у которой уже был запрошен `DROP` |
| A12 | `DROP` выполнен, хотя клиент получил ошибку | две паузы подряд | приемлемо: семантика неясного DDL |
| A13 | wedge до повторного `DROP` | задержка в пределах одного вызова (сетевой сбой) | приемлемо, но исправляется дешёво (6.4) |
| A9 | потеря `_ckpt` живого создателя | неверная терминальность | должно быть невозможно; закрывается в T7/T8/U13, не здесь |

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает | Что не ограничено |
|---|---|---|
| условный PUT каталога (C1–C7) | допуск `gate(reservedFor(0,2))` до каждой попытки (V `writeLoop`); `requestTimeoutMs = T` как Poco send/receive **на одну socket-операцию** (V `getSingleAttemptClient`, `PocoHTTPClient`); connect ≤ C; `Expect: 100-continue` только при теле ≥ 1 MiB (каталог меньше); замороженный `Retry::standard` на весь `casUpdateImpl` плюс одна пауза ≤ 5 s | длительность попытки при медленной отдаче байтов; DNS и получение credentials (I); **применение хранилищем после последнего байта**; пауза процесса между `gate` и `send` |
| `GET` каталога, `GET` слота `mount` в `isCreatorFenceTerminal` | то же; чтение не меняет состояние | устаревание прочитанного к моменту применения записи (A9) |
| janitor LIST/HEAD/`IM`-DELETE | `liveness` перед каждым запросом (V `NamespaceJanitor` doc); T на socket-операцию | применение DELETE после потери лидерства — безвредно (A16), кроме A17 |
| `casUpdateImpl` в целом | один замороженный дедлайн, до 100 попыток | — |

Перенос дедлайна аренды в транспорт (проверка `lease_valid_until − M` перед каждой socket-операцией, таймаут = min(настроенный, остаток)):
- Сужает A11/A14 до паузы между последней проверкой и `writev`. Окно сжимается с «`gate` → send» до микросекунд, но SIGSTOP в нём остаётся.
- Убирает A11 при медленной отдаче тела и при долгом connect/DNS.
- Не закрывает применение хранилищем после последнего байта (A3, A4, A11 со стороны стора, A13).
- Не закрывает A3/A4 вообще. Это ABA, а не время: поздняя попытка жила внутри аренды, вред — от отмены, случившейся позже.
- A9 к транспорту не относится (решение по устаревшему чтению).

## 6. Перенос гарантии на уровень хранилища {#store-level}

### 6.1 Ревизия каталога {#opt-revision}

Добавить `revision: u64` в заголовок или отдельную запись. `casUpdateImpl` ставит `revision = cur.revision + 1` в каждом кандидате.
- Закрывает A3, A4, A5, A14, A19-аналог для каталога: тело никогда не повторяется, ETag тоже.
- Формат: **меняется**. `decodeRefCatalog` строгий (неизвестный `kind` / мусор → `CORRUPTED_DATA`). Старые бинарники после первой записи нового формата отказывают громко (fail-closed), без тихой порчи.
- Нужны: новая версия `FormatId::RefCatalog` в заголовке; правило «пишем новую версию, только когда все монтирования пула её понимают» (флаг в `_pool_meta` или ручной апгрейд); отката нет.
- Код: ~60–100 строк в `Formats/CasRefCatalogFormat.cpp`, `Pool/CasRefCatalog.cpp` + гейт версии; 3–4 файла + тесты.
- Риск: средний (миграция FROZEN-формата).
- TLA+: заменить `obsStale`/`creatorStale` на счётчик ревизии, свойство «ни один `inc` не возвращается после ухода из каталога».

### 6.2 Новая incarnation на каждую попытку шага 1 (формат не меняется) {#opt-remint}

`createNamespaceStep1` идёт `Retry::once` и при `GaveUp(Unresolved)` / неясности перерешает со свежим `mintFreshIncarnation`. Не переотправляет те же байты.
- Закрывает A3: поздняя попытка несёт `incarnation`, который ни разу не был в каталоге. Значит, его никто не объявлял мёртвым и у него нет объектов — это форма A2, безвредна.
- Не закрывает A4 (дубль одного запроса) и A14.
- Побочный эффект: поздняя попытка `e1` над X создаёт лишнюю `Creating`-строку, которую перехватит следующий opener. Это уже покрыто A2.
- Код: ~30–40 строк в `Pool/CasRefCatalog.cpp`, один файл + gtest с hook `create_namespace_step1_pre_read_hook_for_test`. Риск низкий.
- TLA+: в `CaRefCatalogCore` `Create` разбить на `SendStep1(inc)` / `LandStep1(m)`.

### 6.3 Generation-стор {#opt-generation}

На GCS (`Dialect::Generation`) A3/A4/A5/A14 невозможны уже сегодня (V).
- Ничего не стоит.
- Задокументировать как свойство диалекта.

### 6.4 C6: закрыть повторное открытие lane и сделать `Removing` объяснимым {#opt-c6}

- (a) В `dropNamespaceImpl` при неясном `beginRemoving` **не** открывать lane снова на основании одного чтения `Live`. Lane остаётся закрытым (fail-close), пока повтор `DROP` не разрешит исход. Закрывает A13. ~10 строк, формат не меняется. Цена — отказ вставок до повтора `DROP` после сетевого сбоя.
- (b) Записывать в строку `Removing` автора: `removal_by = (server_root_id, writer_epoch)`. Формат меняется (новое поле → старые читатели `CORRUPTED_DATA`); вместе с 6.1 — одна миграция. Тогда преемник, увидев `Removing` от superseded epoch без терминала в логе, выдаёт точную диагностику. Оператор решает сам: доделать `DROP` или вернуть `Live` инструментом. Автоматическое завершение `DROP` преемником — деструктивное действие на запасном пути, **не делать**.
- Store-уровня для A11 без смены протокола нет. S3 не умеет условие на чужой объект (`_ckpt` меняется печатью преемника, но каталог нельзя условить на его etag).
- Варианты «IM на цель, которую меняет преемник» требуют записи в каталог при каждом takeover каждой таблицы. Это горячий общий ключ — отклонено.

### 6.5 Оставить как есть + контроль {#opt-detect}

| Вред | Детектор |
|---|---|
| A11/A13 wedge | GC fold: строки `Removing` без сложенного терминала старше N раундов (`removal_started_round` уже есть, V) → anomaly + метрика `CASCatalogStuckRemoving`. Writer: счётчик отказов «is Removing» по таблице, у которой локальный lane был открыт |
| A3/A17 воскрешение | `completeCreation`/reconcile: если у `Creating`-строки до шага 2 уже есть `_ckpt` с чужим `life_epoch` **и** creator терминален — anomaly «reborn incarnation» (I, эвристика). Fsck: `Live`-жизнь без `_ckpt` (recovery уже бросает `CORRUPTED_DATA`; добавить fsck-проверку и счётчик) |
| A18/A19 debris | `NamespaceJanitorResult.deleted`/`leaked` уже есть (V) |

## 7. TLA+ {#tla}

| Сценарий | Модель / оператор | Покрытие |
|---|---|---|
| A2, A6, A7 | `CaRefCatalogCore`: `Create`, `GoLive`, `ZombieGoLive` (sabotage), `creatorStale` | частично: токен моделируется как «писали после выборки». ABA непредставима |
| A3, A4, A5, A14, A17 | `CaRefNsCleanupStaleLeaderCore`: `StaleJanitorDelete` + `SabotageNoIncarnation` (same-id rebirth → `deletedLiveData`) | вред показан **только как sabotage**. Честного пути к same-id rebirth нет: `Create` атомарен, отложенной попытки нет |
| A8, A10 | `CaRefCatalogCore`: `ReconcileObserve`, `ReconcileCreating` (удаляет строку = отмена C5), `OrphanWrite`, `Janitor` | только честный своевременный путь; поздний реконсилер — нет |
| A9 | `ReconcileCreating` + `SabotageReconcileLiveCreator` | терминальность — булев флаг `creatorAlive`, монотонный по построению. Оживление слота (T7/U13) не моделируется |
| A11–A14 (Z7) | `CaRefCatalogCore:Drop`, `ObserveRemoving`, `AdoptTerminalEvidence` | `Drop` честный, без зомби и без поздней доставки. Wedge не проверяется: нет свойства «`Removing` ⇒ ◇ терминал» |
| A15 | `CaRefPreFoldDrainCore`: `DeleteSuccess`, `DeleteUnknownLanded`, `AdoptHeldOverDeposedRequest`; `CaRefPreFoldDrainAllRowsCore:ExactCatalogCAS` | покрыто |
| A16, A19 | `CaRefNsCleanupStaleLeaderCore:StaleJanitorDelete`; `CaNamespaceJanitorCursorCore:AuthorizedFoldPage` | покрыто для необратимой смерти |

Минимальные добавления:
1. В `CaRefCatalogCore` хранить значение каталога (`catVal`), а не флаг устаревания. Сообщения в полёте: `inflight ⊆ [expect: Val, write: Val]`. Действие `LateApply(m)` разрешено при `catVal = m.expect`. Создание разбить на `SendStep1` / `LandStep1`, разрешить дубль. Свойство `INV_NO_REBIRTH`: `inc`, ушедший из каталога, туда не возвращается. Ожидается нарушение на базе и зелёный прогон с ревизией (6.1) или re-mint (6.2; с дублем — нарушение остаётся).
2. В той же модели — `ZombieDrop`: разрешён при `~ownerAlive`, строка не изменилась, owner-преемник жив. Свойство: «`Removing` без терминала ⇒ владелец-преемник не жив» как safety-ghost `wedgedLive`. Ожидается нарушение (A11). Это фиксирует текущий риск, а не доказывает фикс.
3. `CaRefNsCleanupStaleLeaderCore`: заменить `SabotageNoIncarnation` честным действием `ReplayStep1`, достижимым из (1).

## 8. Вывод {#verdict}

- Каталог безопасен по хранилищу, кроме двух мест. (1) C6 `beginRemoving` (Z7/T4) держится только на времени (T1): поздний `DROP` зомби даёт wedge таблицы преемника, без тихой потери. (2) ABA на ETag-сторе (T5) подтверждена: повтор движка или дубль запроса воскрешает отменённую `Creating`-жизнь. Вред — потеря `_ckpt` перерождённой жизни и wedge, не потеря данных пользователя. На GCS ABA невозможна.
- C3/C4/C5/C7 и janitor безопасны по хранилищу при монотонной терминальности. Немонотонность — в T7/T8/U13.
- Рекомендация, сейчас: 6.2 (re-mint шага 1, ~40 строк, без формата) и 6.4a (не открывать lane после неясного `beginRemoving`, ~10 строк). Плюс детектор «`Removing` без терминала старше N раундов» (6.5).
- Рекомендация, со следующей миграцией формата: ревизия каталога (6.1) вместе с `removal_by` (6.4b).
- TLA+: честный `LateApply` по значению каталога в `CaRefCatalogCore` и `ZombieDrop`.
