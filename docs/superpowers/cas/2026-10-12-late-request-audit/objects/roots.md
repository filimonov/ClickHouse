# Аудит `roots`: loose-объекты `POOL/roots/<srid>/<path>` {#roots-audit}

База `d76b59f9295` (`cas-janitor-budget`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
Строки инвентаря: R1–R4 (U4), тема T3, класс Z5. **V** — прослежено в коде базы; **I** — вывод.

## 1. Объект {#object}

| Поле | Значение | V/I |
|---|---|---|
| Ключ | `<prefix>/roots/<srid>/<path>`; `<path>` — путь диска как есть, без namespace и без `_files/` | V `CA/Formats/CasLayout.h:mountpointObjectKey`, `serverRootDataPrefix` |
| Тело | сырые байты файла, не интерпретируются; полей версии нет | V `CA/Formats/README.md` (`roots/…` raw passthrough) |
| Токен | S3: ETag = MD5 тела (single PUT) или MD5-of-parts (multipart) — **зависит только от байтов**; GCS: generation, новая на каждую запись | V `CA/Backend/CasObjectStorageBackend.cpp:conditionalWriteSettings` (Dialect::Generation), I для формулы ETag |
| Владелец | один `server_root_id`; ключ общий у всех процессов с этим `srid` (зомби и преемник) | V `ContentAddressedTransaction.cpp:writeFile` (`serverRootId() + "/" + path`) |
| Жизнь | пока файл не удалён `unlinkFile`/`moveFile`; GC `roots/` не сканирует; fsck не сканирует; декомиссия чистит префикс один раз | V `CA/Pool/CasPool.h` (коммент у `putMountpointObject`), `CA/Tools/CasDecommission.cpp:deleteListedPrefix` |

### Что ClickHouse пишет в `roots/` {#what-lands-here}

Маршрут (V, `CA/Parts/PartPathParser.cpp`, `ContentAddressedTransaction.cpp:writeFile`): путь не part-файл → `parseTableFilePath`.
Не-Atomic ветка `parseTableFilePath` забирает **любой** путь из ≥2 компонент без part-компоненты (таблица = всё до последнего компонента), и `namespaceLife` рождает такой namespace (`CA/Pool/CasRefLedger.cpp:namespaceLife`). Поэтому в `roots/` попадает только:

| Вид пути | Кто пишет | Тело | Жизнь | V/I |
|---|---|---|---|---|
| `clickhouse_access_check_<ServerUUID>` | `IDisk::checkAccess` → `checkAccessImpl` при старте диска (если не `skip_access_check`): write → read → read со смещением → `existsFile(UPPER)` → `removeFile` | константа `test` (4 байта) | секунды, при каждом старте | V `src/Disks/IDisk.cpp:startup`, `checkAccessImpl` |
| `clickhouse_case_sensitivity_check_<random v4>` | `IDisk::isCaseInsensitive`, только если `checkAccess` не выставил флаг | пусто | секунды; ключ уникален | V `src/Disks/IDisk.cpp:isCaseInsensitive`, I что `createFile` доходит до `roots/` |
| Одно-компонентный файл в корне диска, напр. `BACKUP … TO Disk('<cas>', 'b.zip')` → `b.zip`, `b.zip.lock` | `BackupWriterDisk` | архив целиком (буфер `CaInlineWriteBuffer` в памяти, без лимита); `.lock` = UUID бэкапа | до удаления пользователем | V `BackupImpl.cpp` (`lock_file_name = archive_name + ".lock"`), `CaInlineWriteBuffer`; I что CAS-диск допускается как `allowed_disk` |
| Не-Atomic путь ≥2 компонент, последний компонент похож на part (`x/all_1_1_0` как файл) | неизвестно | — | — | V парсер (`findPartDirComponent` есть, `isPartFilePath` ложно, `parseTableFilePath` = nullopt); I писателей нет |

Не в `roots/` (уходят в namespace `_files/`, это P1/P2): `format_version.txt`, `mutation_*.txt`, `deduplication_logs/*`, `shadow/increment.txt`, `metadata/*.sql`, `<backup>/.backup`, `tmp*/…`. `detached/`, `moving/`, `shadow/` part-файлы — refs. V `PartPathParser.cpp:parseTableFilePath`, `ContentAddressedMetadataStorage.cpp:route`.

**Итог:** в эксплуатации `roots/<srid>/` — практически только временные пробы старта диска с **константным телом**; долгоживущие файлы — только при бэкапе-архиве в корень CAS-диска (I).

## 2. Операции {#operations}

Все через `Pool` → `CasPlainObjects` (`CA/Pool/CasPlainObjects.cpp`), каждая — новый `requests.admit()` (mount-плоскость), `Retry::standard()` = окно 90 с.

| Операция | Актор / функция | Запрос | Профиль | V/I |
|---|---|---|---|---|
| put | `writeFile` (не-таблица) → `CaInlineWriteBuffer` finalize → `putMountpointObject` → `casPutObject` | `readModifyWriteOnPresence`: HEAD → `INM` (absent) / `IM(e)` (present); `decide` **всегда те же байты** | write: `SingleAttempt`, `s3_max_unexpected_write_error_retries_override=1`, attempt timeout `T`, connect cap `C`, GCS — single PUT; S3 — multipart разрешён для больших тел. Движок: `writeLoop` перед каждой попыткой `gate(reservedFor(0,2))` | V `CasRequests.cpp:readModifyWriteOnPresence`, `writeLoop`; `CasObjectStorageBackend.cpp:conditionalWriteSettings` |
| put (Append) | `writeFile` Append: prefix читается `getMountpointObject` при открытии, заморожен | как put, тело `carried + bytes` | как put | V `ContentAddressedTransaction.cpp:writeFile`; I писателей нет |
| exists | `existsFile`, `getStorageObjects` → `mountpointObjectExists` | HEAD | control, single attempt на writable Native, `gate(reservedFor(0,1))` | V `ContentAddressedMetadataStorage.cpp:existsFile`, `getStorageObjects` |
| size | `getFileSize` → `getMountpointObject` | GET тела целиком | как HEAD | V `ContentAddressedMetadataStorage.cpp:getFileSize` |
| read body | `DiskObjectStorage::readFile` по `StoredObject(physicalKey(pool_key))` | GET мимо `CasRequests` | профиль `IObjectStorage` по умолчанию (SDK-повторы), **без допуска** | V `getStorageObjects` возвращает физический ключ; I профиль |
| delete | `unlinkFile` / `removeFile` → GET (наличие) → `removeMountpointObject` → `casRemoveObject` | `removeCurrent`: HEAD → `IM(e)`-DELETE → на `Mismatch` повтор HEAD (по смыслу слепое) | control single attempt; повтор `removeCurrent` гейтится `reservedFor(pause,2)` | V `ContentAddressedTransaction.cpp:unlinkFile`, `CasRequests.cpp:removeCurrent` |
| move | `moveFile` (обе стороны не таблица, или одна) → get(src) → [get(dst) при отсутствии src] → put(dst) → delete(src) | RMWP + RC | как выше | V `ContentAddressedTransaction.cpp:moveFile` |
| list | `listLiveTreeChildren`/`liveTreeDirHasChildren` → `Pool::listMirroredChildren` | LIST `roots/<srid>/<scope>` | control | V `CasPool.cpp:listMirroredChildren` |
| list (решение) | `claimOwnerOrThrow`, `allocateWriterEpoch` → `serverRootSubtreeEmpty` → `prefixHasAnyKey(roots/<srid>/)` | LIST limit 1 | control | V `CA/Pool/CasServerRoot.cpp:serverRootSubtreeEmpty` |
| prefix delete | декомиссия: `deleteListedPrefix(roots/<victim>/)` | LIST → (HEAD) → `IM(listed etag)`-DELETE, ошибки в warnings | `Retry::standard`, один проход, без повторного LIST | V `CA/Tools/CasDecommission.cpp:deleteListedPrefix` |
| — | `removeRecursive`, `removeDirectory`, GC, fsck | `roots/` не трогают | — | V `ContentAddressedTransaction.cpp:removeRecursive`; `CasGc.cpp` LIST только `casRefsPrefix` |

## 3. Сценарии {#scenarios}

Общая механика (V, `CasRequests.cpp:writeLoop`, `CasMountRuntime.cpp:admit`): допуск проверяется **до** каждой отправки по локальному boot-clock дедлайну; после `gate → send` перепроверки нет. Запрос, уже прошедший `gate`, применяется хранилищем независимо от аренды. `INM` ложится только в отсутствующий ключ; `IM(e)` — только если текущий токен = `e`. На S3 токен — функция байтов, поэтому для файла с константным телом `IM` по смыслу слепой.

| id | Кто опаздывает | Что успели другие | Что делает опоздавший | Вред | На чём держится сегодня | V/I |
|---|---|---|---|---|---|---|
| RS1 | `INM`-put пробы `clickhouse_access_check_<uuid>`; зомби = старый процесс того же хоста (тот же `ServerUUID`), SIGSTOP после `gate` | преемник заявил mount, прошёл свой `checkAccess` (put, read, remove) — ключ снова отсутствует | `INM` ложится | debris `test` в `roots/<srid>/`; виден в `listDirectory("")`; делает `serverRootSubtreeEmpty` = false навсегда | ничего (ключ тот же, условие выполнено) | V механизм, I расписание |
| RS2 | то же `INM`, ложится, пока файл преемника ещё есть | преемник записал пробу | 412 → `observePresence` → `Conflict` → новый `decide` → `gate` отказывает (FenceLost/NoBudget) | нет | store: `INM` + время (повтор гейтится) | V |
| RS3 | `IM(e_test)`-put (зомби видел свой старый файл-пробу) | преемник переписал пробу | S3: токен совпадает (те же байты) → ложится; GCS: generation другая → 412 | нет: байты идентичны | ABA безвреден, т.к. тело константно | V механизм, I ETag=MD5 |
| RS4 | `IM(e_test)`-DELETE из `removeFile` пробы зомби | преемник записал пробу и ещё не прочёл её | S3: токен совпадает → **удаляет файл преемника**; GCS: `Mismatch` → повтор `removeCurrent` гейтится → отказ | S3: `checkAccessImpl` преемника падает `CANNOT_READ_ALL_DATA`/`FILE_DOESNT_EXIST` → `IDisk::startup` бросает → старт диска/сервера неуспешен; повторный старт проходит | S3: ничего (ETag ABA); GCS: generation | V механизм (`IDisk::startup` rethrow), I расписание |
| RS5 | тот же DELETE, ложится после `removeFile` преемника | — | `Gone` | нет | — | V |
| RS6 | SDK-повтор того же запроса | — | conditional PUT: `SingleAttempt` + `max_unexpected_write_error_retries=1`; DELETE/HEAD/LIST: single attempt на writable Native. Повтор SDK не возникает | нет | профиль запроса | V `conditionalWriteSettings`, `ContentAddressedMetadataStorage.cpp` (`single_attempt_control_plane_`) |
| RS7 | повтор движка после неоднозначной попытки (`any_ambiguous`) | — | resolve-read; при сдвинутом предусловии байты = наши → `postCommit` (успех), иначе reissue через `gate` | собственный ABA: при константном теле «наши байты» могут быть байтами преемника → зомби считает запись своей; вреда нет (байты те же) | `gate` перед reissue | V `writeLoop` |
| RS8 | ABA для файла с переменным телом (`b.zip.lock` = UUID бэкапа, архив) | преемник/другой бэкап записал свои байты | токен другой → 412 / `Mismatch` | нет | store: токен меняется с байтами | V механизм, I использование |
| RS9 | `INM`-put архива бэкапа `b.zip` (поток BACKUP в старом процессе, пауза в finalize) | пользователь удалил `b.zip` через преемника | `INM` ложится | **воскрешение** удалённого бэкапа (полный, но устаревший архив) + `.lock`-debris | ничего | I (зависит от допуска CAS-диска для BACKUP) |
| RS10 | второй экземпляр того же `srid`, другой `ServerUUID` | — | `claimOwnerOrThrow` отказывает (`CORRUPTED_DATA`); ключи пробы разные | нет | store: `owner` | V `CasServerRoot.cpp:claimOwnerOrThrow` |
| RS11 | второй живой экземпляр того же `srid` и `ServerUUID` (клон каталога) | оба стартуют | взаимные put/delete пробы (RS4 вживую) | один из стартов падает — побочно полезно; корень — U8 (MT2) | U8 | I |
| RS12 | устаревшее чтение для решения: `unlinkFile` GET видит отсутствие (файл удалён зомби, RS4) | — | `if_exists=false` → `FILE_DOESNT_EXIST` | как RS4 | — | V `unlinkFile` |
| RS13 | устаревшее чтение для решения: `moveFile` re-drive — src нет, dst есть → «уже перенесено» | зомби положил dst поздним `INM` | преемник пропускает перенос | потерянный rename; писателей `moveFile` по `roots/` не найдено | ничего | V код, I использование |
| RS14 | поздний `INM`-put жертвы декомиссии после `deleteListedPrefix(roots/<victim>/)` | инструмент: claim, drain, retirement tail (`mount`/`epoch` удалены, `owner` → retired) | ложится в ретайрнутый `srid` | leak; повторного LIST после drain нет, warnings пусты → слот ретайрится | ничего (claim фенсит только `gate`-нутые запросы) | V механизм, I расписание |
| RS15 | поздний `IM`-DELETE жертвы во время drain | инструмент листит и удаляет | drain получает `Gone`/`Mismatch` → warning → слот сохраняется | нет; оператор повторяет команду | warnings | V `deleteListedPrefix` |
| RS16 | debris из RS1/RS14 и решение по пустоте (`serverRootSubtreeEmpty`) | оператор удалил `owner` (или окно U13: `mount`/`epoch` удалены, `owner` ещё не tombstone) | — | owner/epoch re-mint отказывает fail-closed («identity lost over existing data» / «writer_epoch reset hazard»); в окне U13 это **защищает** | fail-closed | V `serverRootSubtreeEmpty`, `allocateWriterEpoch`; I расписание |
| RS17 | устаревший LIST в `serverRootSubtreeEmpty` (in-flight put зомби ещё не лёг) | преемник счёл корень пустым и перевыпустил `epoch` | потом put ложится | для самого `roots/` — debris; вред перевыпуска — U13, не этот объект | — | I |
| RS18 | чтение тела loose-файла зомби (`readFile` мимо допуска) | преемник переписал/удалил | зомби читает байты преемника или 404 | нет: решения зомби после чтения снова гейтятся; тело пробы константно | — | V путь, I вред |
| RS19 | `IM(e)`-put Append (`carried + bytes`) | преемник дописал | токен другой → 412 | нет; Append-писателей в `roots/` нет | store | V механизм, I использование |

Сценариев: **19**. Невозможны по построению: RS2, RS6, RS8, RS10 (store или профиль). Безвредны: RS3, RS5, RS7, RS15, RS18, RS19.

## 4. Нужная строгость {#required-strength}

- **Худший вред сегодня:** RS4 — неуспешный старт преемника (повторяемый), RS1/RS14 — debris в несколько байт, RS16 — fail-closed отказ перевыпуска после ручного удаления `owner`. Потери данных нет. RS9 (воскрешение удалённого архива бэкапа) — I, только при бэкапе в корень CAS-диска.
- **Вероятность:** нужен зомби ровно внутри своего `checkAccess` (≈5 запросов по 4 байта, доли секунды при старте) с паузой ≳ 27 с: `gate` требует остатка ≥ `2E+M` = 16 с, преемник заявляет mount не раньше `L·1.05 + P` = 41.5 с после последнего renew зомби → 41.5 − (30 − 16) ≈ 27.5 с (I). Плюс преемник того же `ServerUUID` должен успеть дойти до своего `checkAccess`. Сетевой блип не хватает: запросы гейтятся до отправки, а поздний apply хранилищем через ≥ 27 с — только TCP-буфер с ретрансмиссией по живому соединению (I).
- **Вывод:** «минимизировано + обнаружено» достаточно. Невозможность не требуется: вред ограничен отказом старта и debris, оба видны.

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| HEAD / GET через `CasRequests` | `gate(reservedFor(0,1))` = остаток > `E+M`; одна попытка; Poco connect ≤ `C`, send/receive ≤ `T` **на socket-операцию**; окно `Retry::standard` 90 с | DNS, получение кредов (IMDS/STS); пауза после `gate` |
| PUT 4 байта (проба) | `gate(reservedFor(0,2))` = остаток > `2E+M`; один HTTP-запрос; одна send-операция; `100-continue` на 4 байтах не нужен (I) | пауза между `gate` и `send`; применение хранилищем после последнего байта |
| PUT большого тела (архив, I) | то же на попытку; `s3_allow_parallel_part_upload=false`; на S3 multipart — каждая part отдельным запросом | **сумма** part-запросов: N × `T` на socket-операцию, без общего дедлайна; медленный send укладывается в per-op таймаут бесконечно долго (I, `base/poco/Net` per-op) |
| DELETE `IM` | `gate(reservedFor(0,1))`; single attempt; reissue на `Mismatch` гейтится `reservedFor(pause,2)` | пауза после `gate` |
| LIST | как HEAD | — |
| raw body read `readFile` | профиль `IObjectStorage` по умолчанию, SDK-повторы | без допуска совсем; только чтение — вреда нет |

**Перенос дедлайна аренды в транспорт** (`lease_valid_until − M` перед каждой socket-операцией, socket timeout = min(configured, remaining)):
- закрывает: длинный multipart/медленный send большого тела (RS9 частично), DNS/креды после истечения аренды, если проверка стоит и перед ними;
- **не закрывает** RS1, RS4, RS14: тело в 4 байта уходит одной операцией; остаются пауза между последней проверкой и `send` (SIGSTOP где угодно) и поздний apply хранилищем после последнего байта. Это store-side сценарии, временем их не убрать.

## 6. Перенос гарантии на уровень хранилища {#store-level}

| Вариант | Суть | Формат (FROZEN 26.6.4) | Миграция | Код | Риск | TLA+ |
|---|---|---|---|---|---|---|
| **A. Ключ пробы по epoch** | в `ContentAddressedTransaction::writeFile`/`unlinkFile`/`existsFile`/`getStorageObjects`/`getFileSize` для имён `clickhouse_access_check_*` и `clickhouse_case_sensitivity_check_*` ключ = `roots/<srid>/<path>@w<writer_epoch>` (epoch растёт на каждый claim). Поздний put/delete зомби бьёт только в свой ключ | меняется только имя временного объекта; старые читатели этих ключей не читают (проба живёт секунды). Persistent loose-файлы не затронуты | нет; старый debris без суффикса остаётся как был | ~25 строк, 2 файла (`ContentAddressedTransaction.cpp`, `ContentAddressedMetadataStorage.cpp`) + gtest | низкий; special-case по имени — хрупко, если upstream переименует пробу | не нужен; при желании — расширение из §7 с ключом по epoch |
| B. Обойти upstream-пробу | CA-диск выставляет `skip_access_check` для `IDisk::checkAccess`; доступ уже проверяет `runCapabilityProbe` под `_probe/<random>` (ключ случайный, PR1) | нет | нет | ~5 строк | средний: теряется проверка read-with-offset пути `IDisk::checkAccessImpl`; на GCS writable `skip_access_check=true` сейчас запрещён (`checkSkipAccessCheckSupport`) — потребует развести «upstream-проба» и «CAS-проба» | нет |
| C. Общий фенс для persistent loose-файлов | версия/nonce в теле невозможна (тело verbatim); метаданные объекта не меняют ETag на S3; multi-object условия нет; перенос в namespace `_files/` даёт тот же общий ключ жизни (U3) | — | — | — | отклонено: нет механизма без смены семантики пути | — |
| **D. Оставить + контроль** | (1) после claim mount: LIST `roots/<srid>/clickhouse_access_check_*` до своей пробы, лог + ProfileEvent на чужой/остаточный объект; (2) в `checkAccessImpl` CA-диска: `FILE_DOESNT_EXIST` на read сразу после своего put — отдельный код ошибки «проба удалена другим процессом этого srid»; (3) декомиссия: повторный LIST `roots/<victim>/` после retirement cut, непустой → warning; (4) `cas-fsck`: секция `roots/` — объекты под ретайрнутыми `srid` и остаточные пробы | нет | нет | ~40–60 строк, 3 файла | нет | нет |

## 7. TLA+ {#tla}

| Модель / оператор | Что покрывает | Сценарии |
|---|---|---|
| `CaCasMountCore:Write` | абстрактная запись держателя, атомарна с допуском (`CanWrite`: `clock < fenceUntil`), выставляет `rootEmpty = FALSE` | ни один поздний сценарий: нет зазора `gate → apply` |
| `CaCasMountCore:ClaimOwnerEmpty`, `AllocEpoch`, `RemintEpoch`, `RemintEpochDecom` | предусловие `rootEmpty` ≈ `serverRootSubtreeEmpty` | RS16 частично (fail-closed по непустоте) |
| `CaCasMountCore:StragglerLands` | поздний apply хранилищем без проверки часов — но для слота recovery-lane, не для `roots/` | шаблон для RS1/RS4/RS14 |
| `CaMountObserveCore:Write`, `NoWriteAfterFence` | запись держателя до фенса, атомарно | не покрывает поздний apply (как у codex для X3) |

Не моделировано: пер-ключевые loose-объекты, общий ключ зомби/преемника, ETag-ABA на константном теле, поздний `INM` в освободившийся ключ, drain декомиссии и поздняя запись после него.

Минимальное добавление (~60 строк в `CaCasMountCore` или отдельный `CaRootsLooseCore`): переменная `probe ∈ {Absent} ∪ [by: Incs, tok: Tokens]`, константа `SameBytesSameToken` (TRUE = S3, FALSE = GCS); действия `ProbeAdmit(a)` (под `CanWrite`, кладёт запрос в `inflight[a]`), `ProbeLand(a)` и `ProbeDeleteLand(a)` без проверки часов (как `StragglerLands`), применяющие `INM`/`IM` по токену; инвариант `NoForeignProbeLoss == ∀ a: holder(a) ∧ probe записан a ⇒ ¬(удалён запросом b ≠ a)`. Ожидание: нарушение при `SameBytesSameToken = TRUE`, выполнение при FALSE и при варианте A (ключ по epoch).

## 8. Вывод {#verdict}

- Безопасность сегодня: **unsafe по механизму, безвредно по последствиям** — общий ключ, `INM`/`IM` без фенса, на S3 `IM` по константному телу слепой; держится на времени (`gate`) и на том, что в `roots/` живут почти только пробы старта.
- Худший реалистичный вред: повторяемый отказ старта преемника (RS4) и debris (RS1, RS14); воскрешение архива бэкапа (RS9) — I.
- Рекомендация: **D (контроль) сейчас**, A (ключ пробы по epoch) — если RS4 увидим хоть раз; C отклонён.
- Объём: D ~50 строк / 3 файла; A ~25 строк / 2 файла; формат не меняется ни там, ни там.
