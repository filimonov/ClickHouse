# Аудит `files`: файлы таблицы вне частей `POOL/cas/ns/state/<life>/_files/<name>` {#files}

База `d76b59f9295` (`cas-janitor-budget`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
Строки инвентаря: P1–P4, тема T3, Z5. **V** — прослежено в коде базы, **I** — вывод.
Зомби A — старый процесс того же `srid`; B — преемник.

## 1. Объект {#object}

| Поле | Значение | V/I |
|---|---|---|
| Ключ | `<prefix>/cas/ns/state/<life>/_files/<tail>`; `tail` — путь ниже каталога таблицы, с подкаталогом (`deduplication_logs/deduplication_log_7.txt`) | V `Formats/CasLayout.h:namespaceFileKey`, `Parts/PartPathParser.cpp:parseTableFilePath` |
| `life` | из каталога по пространству `<srid>/store/<u3>/<uuid>@cas@` (`liveNamespace`). Меняется только при DROP/RENAME/пересоздании. Рестарт `srid` и новый writer epoch её **не** меняют → ключ общий у A и B | V `ContentAddressedMetadataStorage.cpp:liveNamespace`, `ContentAddressedTransaction.cpp:writeFile` (life берётся при открытии буфера) |
| Тело | сырые байты MergeTree, без конверта. Поля epoch/nonce класть некуда | V `Pool/CasPlainObjects.h` |
| Токен | S3: ETag = MD5 тела (одночастный PUT) → **одинаковые байты = одинаковый токен (ABA)**. GCS: generation, меняется на каждую запись, ABA нет | V `Backend/CasObjectStorageBackend.cpp:conditionalWriteSettings` (`Dialect::Generation`); ETag=MD5 — I (поведение S3) |
| Владелец | сервер `srid`, через MergeTree; одна таблица — один пишущий процесс (контракт, не проверка) | V комментарий `moveFile` «SINGLE-WRITER CONTRACT» |
| Время жизни | от первой записи (CREATE TABLE) до DROP/RENAME; после — мёртвая жизнь, удаляет janitor | V `Gc/CasNamespaceJanitor.cpp:runOnePage` |
| Видимость для B | MergeTree читает `_files` **только при загрузке таблицы**; в работе — свои структуры в памяти. Призрачный файл, положенный зомби после старта B, проявится на **следующем** рестарте B | V `StorageMergeTree.cpp:loadMutations`, `MergeTreeData.cpp` (ctor, `format_version.txt`), `MergeTreeDeduplicationLog::load` |

### Какие файлы MergeTree кладёт сюда и что делает с ними при старте {#file-kinds}

| Файл | Кто пишет | Что делает старт | V/I |
|---|---|---|---|
| `format_version.txt` | ctor `MergeTreeData`, при `!attach` или отсутствии файла; тело — `1` (или `0` для старого синтаксиса) | читает; отсутствует → пишет `min_format_version` заново. Удаление по одному файлу в продакшне нет (только `dropNamespace`) | V `MergeTreeData.cpp:MergeTreeData` |
| `tmp_mutation_<k>.txt` | `MergeTreeMutationEntry` ctor; `k` = `insert_increment` — счётчик процесса, начинается с 0 → **у A и B одни имена** | удаляет все `tmp_mutation_*` | V `MergeTreeMutationEntry.cpp`, `MergeTreeData.h:insert_increment`, `loadMutations` |
| `mutation_<N>.txt` | `commit`: `moveFile(tmp → mutation_N)` = get → put → remove. `N` из `increment` | загружает каждую; `tid` без `csn`: CSN известен → `writeCSN`, иначе удаляет. `increment ≥ max N` | V `MergeTreeMutationEntry::commit`, `loadMutations` |
| `tmp_mutation_csn_<N>.txt` → `mutation_<N>.txt` | `writeCSN`: полная перезапись через tmp + `replaceFile` (не Append) | см. выше | V `MergeTreeMutationEntry::writeCSN` |
| `deduplication_logs/deduplication_log_<M>.txt` | `MergeTreeDeduplicationLog` (только при `non_replicated_deduplication_window > 0`). CA не поддерживает Append (`supportWritingWithAppend` = false) → `rotate` на **каждом** `addPart`: файл M финализируется (один PUT), открывается M+1. `dropOutdatedLogs` удаляет старые | грузит все логи в `deduplication_map`; номер следующего = max+1 по LIST | V `MergeTreeDeduplicationLog.cpp:rotateAndDropIfNeeded`, `IMetadataStorage.h:supportWritingWithAppend` (CA не переопределяет) |
| Прочие движки (`Log`, `StripeLog`) на CA-диске | `WriteMode::Append` → RMW с префиксом, прочитанным при открытии буфера | — | I: допускает ли CA-диск эти движки, не проверено |

`ReplicatedMergeTree` здесь кладёт только `format_version.txt` (мутации и дедупликация в Keeper) — I.

**Находка (проза, V):** комментарии `CasPlainObjects::casPutObject` и `ContentAddressedTransaction::writeFile` называют
«единственным аппендером» CSN-append записи мутации. На базе `writeCSN` пишет tmp + `replaceFile`, а лог
дедупликации на CA Append не использует. Append-путь у MergeTree на CA недостижим (I); остаются `Log`/`StripeLog`.

## 2. Операции {#ops}

Все идут через `CasRequests` с `Retry::standard()` (движок повторяет). Каждая попытка — `gate` (T1): запись
резервирует `reservedFor(0,2)` = 2E, чтение/удаление — E (V `Backend/CasRequests.cpp:writeLoop`, `removeCurrent`).
HTTP: условная запись `SingleAttempt`, `s3_max_unexpected_write_error_retries_override = 1`, attempt timeout T,
connect cap C (V `conditionalWriteSettings`); удаление/HEAD/GET/LIST — `controlRequest`, `SingleAttempt` на записываемом
Native (V `ContentAddressedMetadataStorage.cpp` `single_attempt_control_plane_`).

| id | Операция | Актор / функция | Запрос | V/I |
|---|---|---|---|---|
| O1 | create/replace | `writeFile` (finalize `CaInlineWriteBuffer` → `putNamespaceFile`), `moveFile` (put dst), `moveDirectory` (RENAME TABLE: put в новую жизнь), `replaceFile` → `moveFile` | `casPutObject` = `readModifyWriteOnPresence`: HEAD → `decide` (всегда те же байты) → PUT `If-None-Match:*` при отсутствии / `If-Match(e_HEAD)`; на `Conflict` новый HEAD и тот же PUT. **По смыслу last-writer-wins** | V `Pool/CasPlainObjects.cpp:casPutObject`, `CasRequests.cpp:readModifyWriteOnPresence` |
| O2 | delete | `removeFile`/`unlinkFile` (`clearOldMutations`, `killMutation`, `loadMutations`, `dropOutdatedLogs`, `MergeTreeMutationEntry::removeFile`), `moveFile` (remove src), `removeRecursive` подкаталога | `casRemoveObject` = `removeCurrent`: HEAD → DELETE `If-Match(e_HEAD)` → на mismatch новый HEAD. **По смыслу слепое** | V `CasRequests.cpp:removeCurrent`, `ContentAddressedTransaction.cpp:removeFile`, `removeRecursive` |
| O3 | read | `getNamespaceFile` (GET), `listNamespaceFiles` (LIST), `existsFile`; Append-префикс при открытии буфера; `moveFile` читает src | GET/LIST | V `CasPlainObjects.cpp` |
| O4 | delete мёртвой жизни | janitor `runOnePage`: LIST `cas/ns/` → жизнь вне каталога → `remove(key, listed_etag)` | точный `If-Match` | V `Gc/CasNamespaceJanitor.cpp:runOnePage` |
| O5 | copy | нет (RENAME = get + put) | — | V |

Порядок шагов составных операций (V): `moveFile` = GET src → PUT dst → DELETE src, три отдельных допуска.
`MergeTreeSink::commitPart`: `deduplication_log->addPart` (PUT лога) **до** `renameTempPartAndAdd` + `transaction.commit`
(append ref-log части) (V `MergeTreeSink.cpp:commitPart`).

## 3. Сценарии {#scenarios}

Окно зомби: A прошёл `gate`, затем пауза/задержка дольше остатка аренды + ожидания наблюдателя; B стартовал,
прочитал `_files`, работает. Ключевое свойство (V): **B при старте не переписывает существующие файлы**, поэтому
`If-Match` зомби на файл, который B только прочитал, всё ещё выполним. Защищает только запись B того же ключа
(тогда 412/404).

| id | Кто опаздывает | Что успел B | Что делает опоздавший | Вред | Держится на | V/I |
|---|---|---|---|---|---|---|
| F1 | A: `moveFile` PUT `mutation_N.txt` (`If-None-Match`), ALTER A не подтверждён клиенту | стартовал; `N` у A не сохранён ни в одной части → `increment` B < N; B вставляет части `≥ max+1` | PUT ложится (ключ пуст) | призрачная мутация: на **следующем** рестарте B грузит `mutation_N` и выполняет ALTER DELETE/UPDATE, который клиент считал упавшим; применяется к частям с версией < N, т.е. и к данным, вставленным B с номерами < N | только T1 | механизм V, вред I |
| F2 | как F1, но B выделил тот же `N` своему ALTER и уже записал `mutation_N` | `mutation_N` = B | `If-None-Match` → 412 → `Conflict` → новый HEAD под `gate` → отказ (аренды нет) | нет | store: ключ занят B | V |
| F3 | как F1, PUT A лёг раньше записи B | — | B: `moveFile` → RMWP HEAD видит A → `If-Match(e_A)` → перезаписывает | нет: B побеждает. Но `tmp`-путь B — см. F5 | store | V |
| F4 | A: `writeCSN` `replaceFile` PUT `mutation_N` `If-Match(e_old)` | B загрузил `mutation_N`, CSN нет: удалил (txn не закоммичен) или дописал тот же CSN | удалён → 404/412, безвредно. Дописан → у B те же байты (те же поля + тот же CSN) | нет | store / одинаковые байты | V механизм, I содержимое |
| F5 | A: `moveFile` разорван паузой между GET `tmp_mutation_k` и PUT/DELETE | B (свой `insert_increment` с 0) пишет **тот же** `tmp_mutation_k` своего ALTER | (а) если GET A после паузы: читает tmp B → PUT `mutation_N_A` с командами B; (б) DELETE `tmp_mutation_k` A (HEAD после паузы) удаляет tmp B → `moveFile` B: `FILE_DOESNT_EXIST` | ALTER B падает у клиента, а его команды (а) применятся на следующем рестарте под номером A — воскрешение упавшего ALTER | только T1 (каждый шаг) | V механизм, I вред |
| F6 | A: `killMutation` / `clearOldMutations` DELETE `mutation_N` (HEAD до паузы, DELETE поздно) | B загрузил `mutation_N`, файл не трогал | `If-Match(e)` совпал → удалён | kill: B выполняет мутацию до рестарта, после — она исчезает частично применённой; это совпадает с намерением KILL. clearOld: мутация уже выполнена | T1; вред мал | V механизм, I вред |
| F7 | A: `dropOutdatedLogs` DELETE старого лога дедупликации | B загрузил его в память | удалён | после рестарта B окно короче → повтор вставки из удалённого лога не дедуплицируется → дубль | T1 | I |
| F8 | A: `addPart` PUT `deduplication_log_M` лёг (в аренде), затем `commit` части отвергнут (печать B / аренда) | B стартовал до PUT A | лог с токеном A лежит, части нет | на следующем рестарте B токен в `deduplication_map` → повтор клиентом той же вставки **молча отброшен** → потеря вставки. Тот же класс без зомби: крах между `addPart` и `commit` (порядок upstream) | ничего (порядок операций MergeTree) | V порядок `MergeTreeSink::commitPart`, I вред |
| F9 | A: PUT `deduplication_log_M` (`If-None-Match`), M совпал с номером, который выбрал B (оба max+1 по LIST) | B ещё не финализировал свой M | ложится; позже B: HEAD → `If-Match(e_A)` → перезаписывает | записи A потеряны → если часть A закоммичена, её повтор не дедуплицируется (дубль). Если B уже записал M — A получает 412 | store частично | V механизм, I вред |
| F10 | A: `removeCurrent` с паузой **между `gate` и HEAD** | B переписал файл (новый токен) | HEAD видит токен B → DELETE удаляет версию B | потеря файла B (`mutation_N` B → мутация B пропадает после рестарта) | только T1 | V механизм |
| F11 | A: RMWP с паузой между HEAD и PUT | B переписал файл | PUT `If-Match(e_old)` → 412 → новый HEAD под `gate` → отказ | нет | store: токен меняет B | V |
| F12 | SDK-повтор | — | условный PUT/DELETE — одна HTTP-попытка (`SingleAttempt`, `override = 1`); движок повторяет те же байты с тем же условием, двойное применение упирается в условие. Неоднозначная попытка + чужие одинаковые байты → `postCommit` «свой» | нет (одинаковые байты) | store | V `writeLoop`, `conditionalWriteSettings` |
| F13 | ETag ABA: B переписал те же байты, A шлёт поздний `If-Match(e)` | — | на S3 условие выполнимо | вредных экземпляров не найдено: одинаковые байты пишет только `format_version.txt` (по одному файлу не удаляется) и повторный `writeCSN` (те же байты). На GCS ABA нет | случайность содержимого | I |
| F14 | Второй живой экземпляр того же `srid` | оба держат таблицу | номера `N`, `k`, `M` совпадают; каждый PUT — last-writer-wins | потеря мутаций/логов, F1–F10 без всякой паузы | только исключение плоскости монтирования (MT2/T7) | I |
| F15 | Устаревшее чтение: LIST/GET B при старте | — | S3 LIST/GET строго согласованы | нет; опоздание A после LIST B = F1 | store | I |
| F16 | Устаревший Append-префикс (`Log`/`StripeLog`) | B дописал файл | PUT A `carried + bytes` → `Conflict` → тот же PUT поверх B | потерянное обновление строк Log-таблицы | T1 | V механизм `writeFile`, I применимость |
| F17 | A: PUT в жизнь, которую B уже удалил (DROP/RENAME) | `dropNamespace`, жизнь вне каталога | ложится в мёртвую жизнь | мусор до следующего прохода janitor | store: ключ по жизни | V `namespaceFileKey`, `runOnePage` |
| F18 | janitor: поздний `remove(key, listed_etag)` | — | удаляет только неизменённый объект мёртвой жизни | нет, пока жизнь не воскрешена (ABA каталога — T5) | store + необратимость удаления жизни | V механизм; T5 вне объекта |
| F19 | A: RENAME TABLE (`moveDirectory`) поздно | — | put в новую жизнь + `dropNamespace` старой | файлы — как F17; решение о `dropNamespace` — каталог (T4/T5) | — | I |

Невозможные расписания: поздний `If-Match`-PUT/DELETE поверх **изменённого** B файла (F11, F2) — условие
проверяется при применении, токен сменил B (V `ObjectStorageBackend::write`, `removeUnder`).
Запись зомби в чужую жизнь — ключ несёт жизнь, захваченную при открытии буфера (V).

## 4. Нужная строгость {#strength}

- **Худший вред:** F1/F5 — ALTER DELETE/UPDATE, о котором клиент знает как об упавшем, выполняется на следующем
  рестарте; F8 — молча отброшенная вставка. Целостность пула (блобы, манифесты, ссылки) не страдает. Вред тихий и
  отложенный: B его не видит до рестарта, логов нет.
- **Вероятность:** допуск требует остатка ≥ 2E+M = 16 с; B пишет не раньше `L·1.05+P` = 41,5 с после последнего
  продления, которое видел наблюдатель. Значит, запрос A должен застрять после `gate` дольше ~16 с плюс старт B.
  Сетевой сбой этого не даёт: каждая socket-операция ≤ T = 5 с, connect ≤ C = 1 с. Нужна пауза процесса
  (SIGSTOP, своп, долгий GC), зависание DNS/учётных данных или позднее применение самим хранилищем. Плюс B за это
  время должен создать конфликтующее имя (F5, F9, F10) или не тронуть ключ (F1, F6–F8).
- **F8 частично не про зомби:** крах между `addPart` и `commit` даёт тот же результат на любом диске (порядок
  upstream). Чинить его в CAS не нужно; зомби лишь добавляет ещё один путь к нему.
- **Вердикт:** «минимизировано + обнаружено» приемлемо для F6, F7, F9, F17. Для F1/F5/F10 сегодня нет ни
  невозможности, ни детектора. Минимум — детектор (вариант D ниже). Сделать невозможным — вариант A, без новых
  полей в формате.

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| HEAD/GET/LIST (`controlRequest`) | `gate(E)`, `SingleAttempt`, `requestTimeoutMs` = T на **каждую** операцию send/receive Poco, connect ≤ C | медленная выдача по байтам (каждый recv < T); DNS; получение учётных данных (IMDS/STS); пауза между `gate` и `send` |
| Условный PUT (тело ≪ 1 MiB) | `gate(2E)`, одна HTTP-попытка, T на socket-операцию, C; `100-continue` не используется (порог 1 MiB) | то же + задержка применения на стороне хранилища после последнего байта |
| DELETE `If-Match` | `gate(E)` на HEAD и на reissue (`reservedFor(pause,2)`), `SingleAttempt` | то же |
| Составные шаги (`moveFile`, RMWP-цикл) | каждый шаг гейтится заново | пауза между шагами: каждый шаг сам по себе допущен |

V: `S3ObjectStorage.cpp:getSingleAttemptClient`, `PocoHTTPClient.cpp:getTimeoutsFromConfiguration`,
`CasRequests.cpp:gate`, `reservedFor`. Не ограничено — I (по устройству Poco/SDK).

**Дедлайн аренды, спущенный в транспорт** (`lease_valid_until − M` перед каждой socket-операцией, таймаут =
min(настройка, остаток)) закрывает зависания DNS/connect/медленной выдачи и учётных данных. Открытыми остаются:
пауза процесса между проверкой и `send` (F1, F5, F10), позднее применение хранилищем после последнего байта (F1, F8, F9).
Для `_files` именно эти два пути дают вред, так что транспортный дедлайн сужает окно, но не убирает класс.

## 6. Гарантия на уровне хранилища {#store-options}

| Вариант | Суть | Формат | Миграция | Объём | Риск | TLA+ |
|---|---|---|---|---|---|---|
| **A. Файлы как ссылки пространства** (предложение codex) | Каждый файл — ref `_files/<tail>` в той же ref-таблице, манифест из одной `Inline`-записи. Запись = PUT манифеста (`If-None-Match`, уникальный ключ) + append ref-log; удаление = drop ref; `moveFile` = одна порция ref-log (drop src + set dst, **атомарно**). Поздний append зомби: до печати B → упорядочен до состояния B (как будто A успел до краха); после → 412, ячейка чужая. B читает файлы только через recovery, а recovery ставит печать → после первого чтения B поздняя запись A невозможна | **Новых полей нет**: `ManifestEntry` уже несёт `inline_bytes` (V `CasPartManifestFormat.h`), имя ref допускает `/` (V `isCanonicalRefName`), есть прецедент префиксов `detached/`, `moving/` (V `detachedRefNames`). Меняется семантика: класс служебных ref | Старый читатель 26.6.4 файлов не увидит (потеряет мутации и логи дедупликации при даунгрейде) и увидит лишний «каталог» `_files` (MergeTree, вероятно, пропустит как не-часть — I). Нужен однонаправленный флаг пула/минимальная версия читателя и импорт сырых `_files/*` одной порцией при первом открытии | 400–700 строк, 6–8 файлов: новый `CasNamespaceFiles` вместо `CasPlainObjects` для `_files`, `ContentAddressedTransaction` (`writeFile`, `moveFile`, `removeFile`, `removeRecursive`, `moveDirectory`), листинги и `exists` в `ContentAddressedMetadataStorage`, фильтр служебных ref в листинге частей, fsck, BACKUP/FREEZE | средний: все потребители `listRefs` считают ref частью; +1 запрос на каждую запись файла (≈ +1 PUT на вставку с дедупликацией) | фенсинг наследуется от `CaRefTableSnapshotLogCore:FrontierOldWriterLosesToSeal`; новая маленькая модель — только для импорта (сырой ключ vs ref) |
| B. Ключи по epoch `_files/<tail>@<E>` + чтение max E | поздняя запись A ложится в свой ключ `@E_A` | меняет грамматику ключа → 26.6.4 не читает | да | 300+ строк | не закрывает F1: имя, которое B не писал, видно с `@E_A`. Чтобы закрыть, B должен при захвате переписать все имена и вести tombstone — это вариант A заново | — | не рекомендован |
| C. «Касание» при захвате: B переписывает каждый файл, меняя токен | поздние `If-Match` зомби получают 412 | на S3 тот же байт → тот же ETag; менять байты нельзя (`assertEOF` в `MergeTreeMutationEntry`) → нужен конверт = смена формата. На GCS работает без смены формата, но F1 (`If-None-Match` нового имени) не закрывает | — | ~80 строк | частичный | — | отвергнут |
| **D. Оставить + контроль** | Детектор «чужая запись после захвата»: B запоминает время хранилища своего захвата (LastModified слота `mount` — часы хранилища, не узла) и множество своих ETag; на старте таблицы и периодически LIST `_files/` живых жизней; ключ с LastModified > захвата и чужим ETag → `CASForeignFileWriteAfterTakeover` + ERROR в лог. Fsck: `mutation_N` с N, совпадающим с min-блоком части, — признак F1. Уже есть `CASRequestFenceLostPostWrite` (запись легла, после неё аренда потеряна — V `CasRequests.cpp:postCommit`) | нет | нет | 100–150 строк | ложных срабатываний мало (B знает свои ETag); не ловит запись, легшую до LIST и не изменившую имя | — |

Рекомендация: D сразу (дёшево, закрывает «тихо»), A — как плановое изменение вместе с ближайшим подъёмом минимальной
версии читателя. A также чинит неатомарный `moveFile` (F5) без всякой связи с зомби.

## 7. TLA+ {#tla}

| Сценарий | Модель / оператор | Статус |
|---|---|---|
| F17, F18 | `CaRefNsCleanupStaleLeaderCore:StaleJanitorDelete`, `CaNamespaceJanitorCursorCore:AuthorizedFoldPage`; `CaRefCatalogCore_RESULTS.md` оставляет остаток `_files` janitor | покрыто |
| F1–F11, F14, F16 | нет. `CaMountObserveCore:Write` делает допуск и запись атомарными (пропускает запрос, стоящий в очереди после допуска); `CaCasMountCoreLateRenew:WriteAfterRenew` — про ref-полосу | не моделировано |
| F8 | нет (порядок MergeTree) | не моделировано |
| Вариант A | `CaRefTableSnapshotLogCore:FrontierOldWriterLosesToSeal`, `FrontierStartLog` | наследуется |

Минимальное дополнение для статус-кво: модель `SharedKeyLateApply` — ключ `k`; держатель H с допуском по остатку и
недетерминированной задержкой δ до применения; преемник S стартует после ожидания наблюдателя, читает `k`, пишет
INM/IM/RC; операции H — INM/IM/RC по токену, прочитанному до или после паузы. Инвариант «никакое применение H после
первого чтения `k` преемником». Ожидаемо: нарушен при неограниченном δ (F1, F10), держится при δ ≤ резерва.
Для варианта A — модель импорта: сырой ключ и ref сосуществуют, поздний PUT зомби в сырой ключ после импорта не
должен стать видимым.

## 8. Вывод {#verdict}

- Сегодня **safe-by-time**: F1, F5, F10 (призрачная или потерянная мутация) и F7–F9 (дедупликация) держатся только на T1.
  Механизм V, вред MergeTree I; пул не страдает.
- Худшее — ALTER, известный клиенту как упавший, выполняется на следующем рестарте; вероятность низкая (нужна пауза
  > ~16 с после допуска).
- F8 (молча отброшенная вставка) — порядок upstream `addPart` → `commit`, существует и без зомби.
- Действие: D (детектор, 100–150 строк) сейчас; A (файлы как ref, без новых полей, 400–700 строк, флаг пула +
  импорт) — закрывает класс через печать ref-log. Попутно исправить устаревшие комментарии об «единственном
  аппендере» (`casPutObject`, `writeFile`).
