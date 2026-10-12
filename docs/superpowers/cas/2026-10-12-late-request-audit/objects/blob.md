# Аудит: тело блоба `POOL/blobs/<algo>/<S>/<hex>` (id `blob`) {#blob-audit}

База `d76b59f9295` (`cas-janitor-budget`). `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
**V** — прослежено в коде базы; **I** — вывод. Код не исполнялся, TLC не запускался.

Главное:

- **B9 (T1) подтверждён и сужен.** Повтор SDK того же `PUT`/`CopyObject` воспроизводит старый content-ETag. Вред (висячая ссылка) требует ВТОРОГО позднего запроса: точного `DELETE If-Match(t_A)`, который ляжет после повтора поверх чужой свежей публикации. Один повтор без позднего удаления безвреден: payload тот же.
- **Публикация не ограничена арендой вообще.** Допуск проверяется один раз до вызова. Дальше — клиент по умолчанию: до 501 попытки, пауза до 5 с, 30 с на операцию сокета (V). Источник «позднего повтора» — это не только SIGSTOP, а любой затяжной сбой сети у писателя.
- **НОВОЕ, без зомби: гонка `HEAD` тела → `GET` маркера (S13).** Писатель видит тело, затем маркер отсутствует, потому что GC успел удалить тело и маркер. Писатель дописывает `Clean` и принимает удалённое тело → висячая ссылка. Обе модели TLA+ склеивают эти два чтения в один шаг и гонку не видят.
- `NoSuchUpload`-как-успех на AWS почти безопасен (повтор `Complete` объект не пересоздаёт). Остаток — «чужое тело по ключу = мой успех» при upload, прерванном не нами (S7, I).
- GCS (generation): повтор, копия и compose дают новый generation → ABA невозможна (V диалект, I семантика GCS).

## 1. Объект {#object}

| Поле | Значение |
|---|---|
| Ключ | `Layout::blobKey(ref)` = `POOL/blobs/<algo>/<S>/<hex>`; hex = хеш payload, ключ общий у всех писателей одного содержимого (V) |
| Тело | `[envelope header][payload]`. Header: `incarnation_tag` = `mintU128()` на каждый вызов `buildHeader`, `build_id`, `provenance{now_ms, server_id, version, op}`, `intended_ref` (V `Pool/CasPartWriteTxn.cpp:ensureBlobPresent::buildHeader`) |
| Что меняет токен | S3/RustFS: ETag = MD5 тела (multipart: MD5 от MD5 частей + `-N`); меняется только при других байтах, то есть при новом header. Те же байты → тот же ETag (I для RustFS). GCS: generation меняется при КАЖДОЙ записи, даже байт-в-байт (I семантика) |
| Копия | `VerbatimStagedBlobPublication`: байты staging-объекта `staging/<srid>/<random32>.tmp` вместе с его header. ETag копии = ETag staging-объекта (I), повтор копии даёт тот же ETag |
| Владелец | никто: общий ключ. Живость — in-degree рёбер манифестов (precommit + committed), который считает GC (V `Gc::foldManifestEdges`) |
| Маркер | `.meta` рядом (`Clean`/`Condemned{r}`) — отдельный объект, отчёт `meta`. Тело само по себе не несёт состояния осуждения |
| Жизнь | публикация (из `Absent` или поверх `Condemned`) → in-degree 0 → condemn `HEAD`→токен `t` (раунд r) → graduation (r+1, маркер подтверждён) → `delete_pending` → точный `DELETE If-Match(t)` (r+2). `gc_interval_sec` = 60 (V `ContentAddressedSettings.cpp`) → не меньше ~2 мин от нулевого in-degree до удаления |

## 2. Операции {#operations}

| # | Актор / функция | Запрос | Профиль | Внешний цикл |
|---|---|---|---|---|
| O1 | писатель `ensureBlobPresent` → `op.head(key)` | `HEAD` | `SingleAttempt`, попытка `T`, connect `C` (V `controlRequest`, `ContentAddressedMetadataStorage.cpp` `single_attempt_control_plane`) | `readLoop`, `Retry::standard` = 90 с, замороженный дедлайн цикла; gate `E+M` перед каждой попыткой (V) |
| O2 | писатель → `loadMeta` (маркер, не тело) | `GET .meta` | то же | то же |
| O3 | писатель → `op.publish(StreamingBlobPublication)` → `ObjectStorageBackend::publish` → `writeObject(..., WriteSettings{})` | слепой `PUT` или multipart (Create / UploadPart×N / Complete; Abort при `cancel`) | **по умолчанию**: `client.get()`, `retry_attempts` = 500, пауза 25 мс…5 с, `request_timeout` 30 с, `max_unexpected_write_error_retries` = 4, части параллельно (V `CasObjectStorageBackend.cpp:publish`, `S3Defines.h`) | CAS-движок publish НЕ повторяет (`policy.asSingleAttempt()`, V); цикл `ensureBlobPresent` — до 8 итераций, каждая со свежим header (V) |
| O4 | писатель → `VerbatimStagedBlobPublication` → `copyObject(NativeOnly)` → `copyS3File` | `HEAD` источника + `CopyObject` (или multipart `UploadPartCopy` выше `max_single_operation_copy_size`) | по умолчанию: `client.get()` захардкожен в `S3ObjectStorage::copyObject`; свой цикл `NO_SUCH_KEY` (V) | только первая публикация `BlobSource` после `Absent` (`beginPublication`, V); `uploadPendingBlobs` один раз на staging (`st.published`, V) |
| O5 | `Client::CompleteMultipartUpload` при `NO_SUCH_UPLOAD` | `HEAD` ключа; есть объект → результат подменяется на успех | по умолчанию | — (V `src/IO/S3/Client.cpp`) |
| O6 | `Client::CompleteMultipartUpload`, GCS + `gcs_issue_compose_request` | `ComposeObject(key ← key)`, без условий | по умолчанию; флаг по умолчанию `false` (V `S3AuthSettings.cpp`) | ошибка игнорируется |
| O7 | `WriteBufferFromS3::finalizeImpl` / `copyS3File` при `s3_check_objects_after_upload` | `HEAD` + сверка размера | по умолчанию; флаг по умолчанию `false` (V `Settings.cpp`) | ошибка → исключение → следующая итерация O3 |
| O8 | GC condemn `head_blob` (read-ahead `GcReadAhead::takeHead`) | `HEAD` → токен в retired-запись | open-плоскость `gc_requests` (отказ только при teardown, V `CasPool.cpp`); `SingleAttempt` | `Retry::standard` |
| O9 | GC supersede `peek_head` | `HEAD` | то же | то же |
| O10 | GC `performRedeleteIo` | `HEAD`; при `entry.token.matches` → `DELETE If-Match(t)` (V) | то же; `removeObjectIfTokenMatches` | `readLoop` `Retry::standard`: **переотправляет тот же `If-Match(t)` до 90 с** (V) |
| O11 | `scheduleConfirmedMetaDelete` после `Removed`/`Gone` | маркер, асинхронно в пуле `GcMetaWriter` | open | — |
| O12 | fsck, `previewDeletes`, `rebuildBaseline` | `HEAD` | — | только чтение |
| O13 | чтения данных (`readObject`, read pipeline) | `GET` | мимо `CasRequests` | вне темы (T11) |

Нет: conditional create, `If-Match` replace и list для тела. Тело пишется только слепо, удаляется только по точному токену (V).

## 3. Сценарии {#scenarios}

`W1` — опаздывающий писатель; `W2` — свежий писатель того же содержимого; `G` — живой лидер GC; `G'` — свергнутый лидер.

| id | Кто опаздывает | Что успели другие | Что делает поздний запрос | Вред | На чём держится сегодня | V/I |
|---|---|---|---|---|---|---|
| S1 | поздний create: первая (единственная) доставка `PUT t_A` W1, применённая поздно (пауза, сеть, store) | W2 опубликовал `t_B`; G мог осудить `t_B` | `t_A` ложится поверх `t_B`, payload тот же | нет; удаление `If-Match(t_B)` даёт mismatch → `Replaced` → повторное осуждение текущего токена | store: новый header на каждую физическую публикацию → `t_A` нигде не записан как осуждённый | V код GC (`applyRedeleteOutcome`, `merge.replaced`), I расписание |
| S2 | поздний create поверх `Condemned`: W1-зомби публикует свежий `t_Z` | G держит `delete_pending(t_old)` | `t_Z` поверх `t_old` | утечка до следующего condemn; поздний `Clean` W1 закрыт T1 (`writeLoop` gate → `FenceLost`) | store: `If-Match(t_old)` ≠ `t_Z` | V |
| **S3** | **повтор SDK** `PUT t_A` (`Client::doRequestWithRetryNetworkErrors` или цикл `NO_SUCH_KEY`): попытка k применилась, ответ потерян; попытка k+j через минуты | precommit W1 снят (S11/S12); G: condemn `t_A` → graduate → delete; W2 опубликовал `t_B` | попытка k+j заново кладёт ТЕ ЖЕ байты: тело снова `t_A`, поверх `t_B` | сам по себе — нет (payload тот же, `t_A` никем не ожидается). Вред — только в S3a–S3d | ничего на ETag-диалекте. Время: никакое, бюджет SDK арендой не ограничен | V профиль, I расписание |
| S3a | + собственный реиссью G: `DELETE If-Match(t_A)`, попытка 1 применилась, ответ потерян (`T`=5 с) | в окне паузы `readLoop` (до 90 с): W2 публикует `t_B` (`Absent`), затем ложится повтор W1 `t_A` | попытка 2 `If-Match(t_A)` совпадает → удаляет живое тело W2 | **висячая ссылка** | ничего (open-плоскость, точный токен совпал) | V механизм, I расписание (совпадение трёх событий за ≤ 90 с) |
| S3b | + перенос G: удаление исчерпало 90 с, но применилось; запись остаётся `delete_pending` | следующий раунд (+60 с): W2 сделал precommit ПОСЛЕ среза раунда и опубликовал `t_B`; повтор W1 `t_A` лёг до `HEAD` G | `performRedeleteIo`: `HEAD`=`t_A` совпадает → `DELETE` | **висячая ссылка** | спасает, только если precommit W2 попал в срез (тогда `spared`) | V (`reportRedeleteFailure` оставляет `delete_pending`), I |
| S3c | + `G'` замер между `HEAD` и `DELETE` в `performRedeleteIo` | G удалил `t_A`; W2 → `t_B`; повтор W1 → `t_A` | `G'` отправляет `If-Match(t_A)` → удаляет | **висячая ссылка** | ничего: PRE-CAS удаления «безопасны при любой устарелости лидера» только при уникальном токене | V (комментарий у `redeleteBlobs`), I |
| S3d | повтор W1, и G в фазе удаления раунда | `delete_pending(t_A)`; W2 (precommit после среза) видит маркер `Condemned` → перепубликует `t_B`; повтор W1 `t_A` ложится до `HEAD` G | G: `HEAD`=`t_A` → удаляет | **висячая ссылка** | ничего | I |
| S4 | повтор SDK / поздняя копия на GCS | как S3 | новый generation `g'` | нет: `If-Match(g_A)` не совпадает ни с одной новой записью | store: generation на каждую запись | V диалект (`Dialect::Generation`), I семантика GCS |
| S5 | поздняя / повторная `CopyObject` (O4) W1 | как S3 | копирует staging-объект ещё раз → тот же ETag `t_S` | как S3: вред только с S3a–S3d | время: staging-объект удаляется в конце транзакции (`ContentAddressedTransaction.cpp:199`) или `sweepOwnMountStaging` преемника → поздняя копия падает `NoSuchKey` | V профиль, I |
| S6 | повтор `CompleteMultipartUpload` (SDK) после применённого | — | `NoSuchUpload` → `HEAD` → «успех» | нет: на AWS израсходованный `UploadId` объект не пересоздаёт | store: `UploadId` одноразовый | V код, I семантика (AWS API). RustFS/MinIO — I |
| S7 | синтетический успех: `Complete` НЕ применился (upload прерван не нами: lifecycle `AbortIncompleteMultipartUpload`, поведение store), по ключу лежит старое `t_old` | W1 публикует потому, что маркер `Condemned{r}`; G уже опубликовал `delete_pending(t_old)` | `HEAD` находит `t_old` → W1 считает, что опубликовал свежий header; `reconcileMetaClean` `IM(e_Condemned)` → `Clean`; W1 возвращает `Materialized` | **висячая ссылка**, если precommit W1 после среза раунда удаления. До graduation безвредно: гейт видит `Clean` → перенос → W1 в следующем срезе | ничего: `HEAD` не сверяет ETag с ожидаемым multipart ETag | V код, I достижимость |
| S8 | поздние `CreateMultipartUpload` / `UploadPart` / `AbortMultipartUpload` | upload завершён или прерван | Create: пустой upload; Part: `NoSuchUpload`; Abort после Complete: `NoSuchUpload` | мусор (незавершённые uploads, части) — стоимость, не потеря | store: `UploadId`; `tryToAbortMultipartUpload` best-effort | V |
| S9 | поздний `DELETE If-Match(t)` от `G'` без ABA | публикация свежего `t'` | 412 mismatch | нет | store: точный токен | V |
| S10 | ABA при перепубликации тех же байтов писателем | — | — | невозможно: каждая итерация зовёт `buildHeader` → новый `incarnation_tag`; копия — один раз на `BlobSource`; `uploadPendingBlobs` один раз на staging | store + код | V |
| S11 | второй экземпляр с тем же `srid` (P1 жив, но отрезан) | P2 дождался T4, сделал claim, `sweepStalePrecommitsNow` снял precommit P1 (старший epoch) | in-flight `PUT`/повтор P1 продолжается | источник S3/S5; `sweepOwnMountStaging` P2 удаляет staging P1 → поздние копии P1 падают | время (T4); после claim ничего не мешает телу P1 лечь | V (`CasRefLedger::sweepStalePrecommitsNow`), I |
| S12 | отложенный поток в живом процессе: аренда потеряна, self-remount поднял epoch и снял precommit; поток W1 всё ещё в цикле SDK | как S11 | повтор ложится; `requireAdmitted` / `requireAlive` ловят это только ПОСЛЕ возврата `publish` | источник S3 | ничего на транспорте | V (`ensureBlobPresent`, `sweepStalePrecommitsNow`), I |
| **S13** | **без зомби**: у W2 разрыв между `HEAD` тела (O1) и `GET` маркера (O2) | delete-раунд G: precommit W2 после среза; `HEAD` W2 видит `t` до `DELETE`; G удаляет тело (`Removed`) и асинхронно маркер (O11) до `GET` W2 | W2: маркера нет → `putMetaIfAbsent(Clean)` → `Observed` → `Materialized` | **висячая ссылка** + лживый `Clean` над пустым ключом | ничего: после `GET` повторного `HEAD` нет (V). Окно — задержка `HEAD`→`GET` у W2 больше, чем «ответ DELETE → GET+DELETE маркера» у G | V код, I расписание |
| S14 | устаревшее чтение `HEAD` писателя (кеш / прокси, нелинеаризуемый store) | тело и маркер удалены | «present» + маркер отсутствует → принятие | висячая ссылка | S3 строго согласован; для RustFS и прокси — допущение | I |
| S15 | устаревший / поздний `HEAD` GC при condemn (O8) | писатель перепубликовал `t'` | осуждается старый `t` → `DELETE` mismatch | нет (задержка освобождения) | store | V |
| S16 | поздний `ComposeObject` (GCS, флаг включён) | G удалил `g1`, W2 записал `g3` | compose текущего `g3` → `g4` с байтами W2; если ключа нет — ошибка | нет; `If-Match(g3)` → mismatch → `Replaced` | store: generation; compose не воскрешает отсутствующее | V код, I семантика |
| S17 | `checkObjectAfterUpload` ложный 404 / размер | — | исключение → итерация O3 со свежим header | лишний `PUT` | — | V (флаг по умолчанию выключен) |
| S18 | поздний `Clean` зомби-писателя после публикации | — | `reconcileMetaClean` идёт через `writeLoop` → gate → отказ | нет (по телу) | время T1 | V |
| S19 | чтения зомби (B7) | GC удалил тело | ошибка чтения | доступность | — | I, тема T11 |

Невозможное: S10 (V). Повтор `Complete` → новое тело (S6) — невозможно на AWS: `UploadId` расходуется первым `Complete` (I, документация AWS).

## 4. Нужная строгость гарантии {#required-strength}

| Сценарий | Худший вред | Насколько вероятно на практике | Вердикт |
|---|---|---|---|
| S3a–S3d | потеря данных части (висячая ссылка на блоб) | Нужны ДВА поздних события на одном хеше: (1) повтор того же тела спустя ≥ 2 раунда GC (≥ 2 мин) после первой доставки — затяжной сбой сети у W1 (SDK 501 попытка, без SIGSTOP), self-remount или второй экземпляр снимают precommit; (2) точный `DELETE` того же токена после повтора — реиссью G (≤ 90 с), перенос `delete_pending` или замерший `G'`; плюс W2 с тем же содержимым в этом окне. Каждое по отдельности бывает; совпадение редкое, но не требует SIGSTOP | «минимизировать + обнаруживать» недостаточно: тихая потеря данных. Нужно сделать невозможным — уникальность HTTP-применения (опция A) дешёвая |
| S13 | потеря данных | Без зомби. Нужны: precommit W2 после среза раунда, `HEAD` W2 за мгновение до `DELETE`, `GET` W2 после асинхронного удаления маркера. Окно — несколько RTT на каждый удаляемый блоб, который в тот же момент переиспользуют. Растёт с dedup-нагрузкой и с перегрузкой store | должно быть невозможно; фикс — 5–10 строк (опция B) |
| S7 | потеря данных | Требует `NoSuchUpload` без применённого `Complete` — на AWS только чужой abort (lifecycle). Для RustFS не проверено | невозможно дёшево (опция C); до фикса — допустимо как I-риск с детектором |
| S8 | мусор, стоимость | частые незавершённые uploads при сбоях | минимизировать + lifecycle-правило |
| S1, S2, S15, S16 | утечка / задержка освобождения | обычны | норма |

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| `PUT` тела (O3, ≤ 32 МиБ) | gate один раз до вызова: остаток аренды > `E + M` = 9 с (`readLoop` `reservedFor(0,1)` + `lease_safety_margin_ms`, V `CasMountRuntime::admit`). Транспорт — клиент по умолчанию: connect диска (1 с), send/receive 30 с на ОПЕРАЦИЮ сокета Poco, адаптивные таймауты только на первой попытке (`ConnectionTimeouts::getAdaptiveTimeouts`), `100-continue` нет (только conditional, V `PocoHTTPClient.cpp`) | число попыток: `retry_attempts` 500 × (≤ 30 с + пауза ≤ 5 с); цикл `NO_SUCH_KEY` ×4 сверху; медленная струйка (каждый `send` укладывается в 30 с) растягивает одну попытку без предела; DNS, получение credentials. `attempt_timeout_ms` и `SingleAttempt` к publish НЕ применяются (`WriteSettings{}`, V) |
| multipart (O3, > 32 МиБ) | то же, gate один раз | каждая часть — свой цикл SDK; `Complete` — цикл SDK × `NO_SUCH_KEY` ×4 + `HEAD` при `NoSuchUpload`; `Abort` в `cancel`/деструкторе — после `safeWaitAll` всех частей |
| `CopyObject` (O4) | gate один раз | `HEAD` источника + `CopyObject` клиентом `client.get()` (500 попыток); multipart-копия — как выше |
| `HEAD`/`GET` писателя (O1, O2) | `SingleAttempt`, `T`=5 с, connect cap `C`; gate `E+M` перед каждой попыткой; общий дедлайн цикла 90 с | пауза между gate и отправкой (I) |
| GC `HEAD`/`DELETE` (O8–O10) | `SingleAttempt`, `T`, `C`; цикл 90 с | аренды нет (open-плоскость): реиссью точного `DELETE` идёт 90 с, перенос `delete_pending` — следующими раундами; длина раунда — сколько угодно (I) |

**Проброс дедлайна аренды в транспорт** (проверка `lease_valid_until − M` перед каждой операцией сокета, таймаут = min(настройка, остаток)):

- Закрывает: источник повторов S3/S5 в S11/S12 и затяжной сбой сети. Повтор, отправленный после потери аренды, просто не уйдёт. ABA требует повтора через ≥ 2 раунда GC после снятия precommit, а снятие идёт только после потери аренды (claim преемника или self-remount с новым epoch). Значит каждый опасный повтор отправляется после дедлайна (I).
- Остаётся открытым:
  - пауза процесса между проверкой и `send` последнего байта (SIGSTOP-класс; окно — один вызов `send`);
  - позднее применение запроса store после последнего байта (секунды, не минуты — I);
  - прокси или балансировщик, который сам повторяет `PUT` (I);
  - все сценарии на стороне GC: S3a–S3c — у GC нет аренды, проброс туда не дотягивается;
  - S13 и S7: зомби там нет.
- Ограничение: проброс ничего не меняет в том, что поздний `PUT` в принципе ложится. Он только сжимает окно до «дедлайн + время применения». Этого хватает, потому что вреду нужны минуты (раунды GC), но это гарантия по времени, а не по store.

## 6. Перенос гарантии на уровень хранилища {#store-level}

| Опция | Что делает | Формат | Миграция | Объём | Риск | TLA+ |
|---|---|---|---|---|---|---|
| **A. Одно HTTP-применение на header** | publish и copy блоба — профиль `SingleAttempt` (клиент без повторов SDK, `s3_max_unexpected_write_error_retries_override = 1`; для copy — `clientForRetryProfile` вместо `client.get()` в `S3ObjectStorage::copyObject`). Ретраи остаются во внешнем цикле `ensureBlobPresent` (8 итераций, каждая — новый header). Таймаут на операцию сокета — отдельный бюджет publish, не `T` на запрос | нет | нет | ~30–50 строк, 3 файла (`CasObjectStorageBackend.cpp`, `S3ObjectStorage.cpp`, `copyS3File` вызов) | больше перепубликаций при плохой сети → больше `Replaced` и мусора; multipart: повтор части с тем же номером остаётся (безвреден: часть не объект). Закрывает S3/S5 механизмом store: два применения одних байтов требуют двух HTTP-запросов, а их больше нет. Остаток — прокси-повторы (I) | добавить `SdkResend(w)` в `CaBlobPublishCore` (повторная посадка уже севшего envelope после `GCDelete`); ждать нарушения `ExactDeleteCannotRemoveFreshIncarnation` без фикса и тишины с ним |
| **B. Повторный `HEAD` при отсутствующем маркере** | в `ensureBlobPresent`, если `loadMeta` → nullopt: ещё раз `op.head(key)`; принимать, только если токен равен `present->etag`, иначе следующая итерация. Вариант дешевле по коду: отсутствующий маркер → перепубликация (цена — лишний `PUT` для тел без маркера) | нет | нет | 5–10 строк, 1 файл | +1 `HEAD` только на пути backfill. Корректность — см. ниже | разделить `WAdopt` в `CaGcCondemnMarkerGate` на `WHead` + `WMetaRead` (и снять guard `body.present` с `ObserveMeta` в `CaBlobPublishCore`); ждать нарушения без фикса |
| **C. `NoSuchUpload` для CAS — неоднозначность, не успех** | флаг на запросе (из `WriteSettings` для publish блоба): не подменять `NoSuchUpload` успехом. Исключение → внешний цикл: `HEAD` + маркер + свежий header. Альтернатива: сверить ETag из `HEAD` с ожидаемым `md5(⊕ md5_i)-N` (только ETag-диалект) | нет | нет | ~15–25 строк, 3 файла (`Client.cpp`, `WriteBufferFromS3.cpp`, `WriteSettings.h`) | лишняя итерация, когда первый `Complete` реально применился | нет модели multipart; минимальное добавление — `CompleteAmbiguous` в `CaBlobPublishCore` |
| D. Перечитать маркер перед удалением (`DeleteReads`) | `performRedeleteIo` перед `DELETE` читает `.meta`; удаляет, только если там `Condemned{≥r}` | нет | нет | ~20 строк | +1 `GET` на удаление. Сужает S3a/S3d: W2 к этому моменту обычно уже записал `Clean`. Не закрывает: удаление между публикацией W2 и его `Clean` | уже есть флаг `DeleteReads` в `CaGcCondemnMarkerGate` |
| E. Второе условие на `DELETE` | `x-amz-if-match-last-modified-time` / `-size` вместе с `If-Match`: повтор получает новый `Last-Modified` | нет | нет | ~30 строк + проба поддержки при монтировании | поддержка AWS для general-purpose bucket и RustFS не проверена (I) | токен = пара (etag, mtime) в `CaIncarnationCore` |
| Оставить + контроль | — | — | — | — | — | — |

Почему B корректна (I, кроме ABA S3). `delete_pending` требует маркер `Condemned{≥r}`, прочитанный после среза раунда graduation (V `confirm_condemned_marker`). Маркер удаляется только после `Removed`/`Gone` этого тела (V `applyRedeleteOutcome` → `deleteConfirmedMeta`). Значит «маркера нет, но тот же токен виден после этого чтения» возможно, только если то же тело воскресло — а это S3, который закрывает A.

Контроль без фикса:

- `blob_storage_log`: два `PutObject` (или `Copy`) на один ключ с одинаковым ETag, разделённые строкой `Delete`, — прямой след S3.
- fsck (`Tools/CasFsck.cpp`, `HEAD` по каждой ссылке) находит висячую ссылку постфактум.
- Нет счётчика «повторов SDK на publish блоба»: стоит добавить, если A не делается.
- Нет события «backfill `Clean` при отсутствующем маркере» в паре с `BlobDelete` того же хеша за последние N секунд — детектор S13 по `cas_log`.

## 7. TLA+ {#tla}

| Сценарий | Модель:оператор | Покрыт? |
|---|---|---|
| S1, S2 | `CaBlobPublishCore:BackendLand`, `LateBackendLand`, `CondemnedNeedsFreshPublication`; `CaIncarnationCore:WOverwrite` | да |
| S9, S10 | `CaBlobPublishCore:GCDelete`, `DeleteLands`, `ExactDeleteCannotRemoveFreshIncarnation`, `MintToken`; `CaIncarnationCore:GRecheckDelete` | да |
| S3, S5 (повтор уже севшего envelope) | `CaBlobPublishCore:LateBackendLand` | **нет**: `latePending = ~attemptLanded` при `ResponseLost` — у попытки одна посадка, вовремя или поздно, никогда две (V текст модели). `queuedDeletes` вечные — это хорошо: стоит добавить одну повторную посадку, и модель увидит S3a–S3d |
| S13 | `CaGcCondemnMarkerGate:WAdopt` (`body.present /\ ~Cond` одним шагом); `CaBlobPublishCore:ObserveMeta` (guard `body.present`) + `ObservationInvalidated` | **нет**: обе модели проверяют присутствие тела в момент чтения маркера, код — нет |
| S7, S6, S8 | — | нет модели multipart / `NoSuchUpload` |
| S4, S16 (generation) | `CaBlobPublishCore` `tokenFamily` | частично (генерационная семья токенов есть, compose нет) |
| S11, S12 | `CaRefWriterCleanupCore:SuccessorCleanupStep` (снятие precommit) | снятие да; продолжение in-flight publish после снятия — нет |
| S3a/S3c (поздний `DELETE` GC) | `CaGcCondemnMarkerGate:GMetaDeleteJob` (без fence, «стоит за свергнутого лидера»); `CaBlobPublishCore` — `GCDelete` разрешён в любой момент | да для тела, но без S3-повтора вреда нет |

Минимальные добавления:

1. `CaBlobPublishCore`: действие `SdkResend(w)` — если тело последней посадки W удалено, посадить тот же `envelope` ещё раз (тот же `token`, новый `serial`). Без фикса A ожидается нарушение `ExactDeleteCannotRemoveFreshIncarnation`. С A это действие запрещено.
2. `CaGcCondemnMarkerGate`: разделить `WAdopt` на `WHead(w)` (запомнить `body.tok`) и `WMetaRead(w)` (без guard на тело), при `SplitMetaDelete = TRUE`. Ожидается нарушение без B и тишина с повторным `HEAD`.

## 8. Вывод {#verdict}

- **Безопасность сегодня — unsafe** на ETag-диалекте (S3/RustFS) по двум независимым путям:
  - S3: повтор SDK + поздний точный `DELETE`. Время не ограничивает: publish живёт вне аренды.
  - S13: гонка `HEAD`→маркер, без зомби.
- GCS — safe по store для S3; S13 там тоже открыт.
- Рекомендация:
  1. **B** — повторный `HEAD` при отсутствующем маркере: 5–10 строк, без формата.
  2. **A** — `SingleAttempt` для publish и copy блоба: ~40 строк, 3 файла, без формата.
  3. **C** — `NoSuchUpload` как неоднозначность: ~20 строк.
- Плюс два действия в TLA+ (`SdkResend`, разделённый `WAdopt`) как failing-first проверка. Миграции нет, формат не меняется.
