# Аудит опоздавших запросов: слот монтирования `gc/server-roots/<srid>/mount` {#mount-audit}

База `d76b59f9295` (`cas-janitor-budget`), сравнение с `3b4037ae265`. `CA/` = `src/Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/`.
**V** — прослежено в коде базы; **I** — вывод. Строки инвентаря: MT1–MT10; темы T7, T8, T14.

**Ответ на главный вопрос.** Да, с двумя оговорками. Каждый актор, который действует по «держатель мёртв», сначала меняет слот условной записью (V):

- фенс GC: `RMW` → `gc_fenced`;
- reclaim: `IM(got->etag)`;
- `cas_unsafe_remount_no_delay`: `IM`;
- декомиссия: `IM`, затем farewell и `IM`-delete;
- `isCreatorFenceTerminal` и `floorForNamespace` только читают. Они верят состояниям, которые сами были условной записью в слот: fence, farewell, другой epoch.

Значит, успешный renew `IM(last_etag)` доказывает: с прошлого commit держателя слот никто не трогал (V). Это доказательство ничего не говорит об интервале «действие актора → следующий renew держателя» (до `P` + дедлайн). Там держатель пишет данные по локальному дедлайну (T1). Безопасность этого интервала держится:

- на времени (T3, T4);
- на полноте drain (farewell);
- ни на чём (T5, same-pair refresh, регресс epoch).

Оговорки:

1. Same-pair refresh (MT2/T7) меняет слот, но без сертификата смерти. Действует сразу, держатель жив.
2. `SupersededEpoch` считает терминальным любое неравенство epoch. Вместе с reclaim «другой, а не больший» (T8) это даёт сертификат по слоту, где держит **старший по номеру** epoch.

## 1. Объект {#object}

| Поле | Значение | V/I |
|---|---|---|
| Ключ | `POOL/gc/server-roots/<srid>/mount` (`Formats/CasLayout.h:mountKey`) | V |
| Тело | `{server_uuid, writer_epoch, hostname, pid, started_at_ms, seq, expires_at_ms, min_active_build_sequence, gc_fenced, write_attempt_id}` (`Formats/CasServerRootFormats.cpp`) | V |
| Что меняет ETag | каждая запись держателя несёт свежий случайный `write_attempt_id` (`newMountWriteAttemptId`, UUIDv4); фенс GC копирует тело + `gc_fenced=true` + `seq+1`. Байты двух разных состояний совпасть не могут → ABA нет | V |
| `seq` | **не монотонен на базе**: `MountLeaseRenewer::start` пишет тело с `seq=1` поверх тела `claimMount` (`seq = existing.seq+1`). Инвентарь («`seq` растёт на каждой записи») неточен; ABA всё равно нет из‑за `write_attempt_id` | V |
| Терминальные состояния | `gc_fenced` (по паре `(uuid, epoch)`), clean farewell `min_active_build_sequence == UINT64_MAX`, слот занят другим epoch | V |
| Владелец | процесс‑держатель `(uuid, epoch)`; пишут также GC‑лидер (фенс), стартер того же uuid (reclaim), декомиссия | V |
| Жизнь | mint при первом старте → renew каждые `P` → farewell / фенс → reclaim следующим epoch → удаление только декомиссией | V |

## 2. Операции {#operations}

Все записи — `CasRequests`, `conditionalWriteSettings` (single-attempt, SDK не повторяет). Чтения — `SingleAttempt` на записываемом Native (`ContentAddressedMetadataStorage.cpp:822`, по инвентарю, I). Повторы — цикл движка `writeLoop` с политикой `Retry`.

| # | Актор | Функция | Запрос | Политика / план | V/I |
|---|---|---|---|---|---|
| O1 | стартер | `claimMount` (absent) | `GET` → `INM` | `Retry::standard` (90 с), `gc_requests` (fence open) | V |
| O2 | стартер | `claimMount` same-pair refresh | `GET` → `IM(e)` | то же | V |
| O3 | стартер | `claimMount` reclaim (fenced / clean / `proven_dead` / `unsafe`), epoch `≠` | `GET` → `IM(e)` | то же | V |
| O4 | стартер / self-remount | `claimMountAwaitingExpiry` | цикл O1–O3 + `GET`, сон `P/2`, порог `L + L/20 + P/2` | `kMaxObservationRestarts` | V |
| O5 | renewer | `MountLeaseRenewer::claim` из `start` | `GET` → `INM` (absent) / `IM` adopt same-pair | `Retry::standard`, `open_requests` | V |
| O6 | renewer | `renewOn` | `IM(last_etag)` | `Retry::untilLeaseSafe(confirmed_deadline, M)` | V |
| O7 | renewer | `terminate` (farewell) | `IM(last_etag)`, `min_active=MAX` | `untilLeaseSafe(..., max(10 с, 2E+2 с))`, `open_requests` | V |
| O8 | GC-лидер | `computeHeartbeatFloor` | LIST + `RMW` → `gc_fenced`, `seq+1` | `Retry::standard`; порог `L + L/20 + P` по mono GC | V |
| O9 | декомиссия | `Pool::openForDecommission` → `mountWritable(NoWait)` | O1/O3 без observation | `Retry::standard` | V |
| O10 | декомиссия | `deleteSlotObject` (`Tools/CasDecommission.cpp:99`) | `IM`-delete по захваченному farewell | `throwIfClaimLost` | V |
| O11 | ref-ledger | `isCreatorFenceTerminal` → `classifyFenceCertificate` | `GET` | политика вызывающего | V |
| O12 | orphan sweep | `floorForNamespace` / `prefixEligibleUnder` | `GET` | `Retry::standard` | V |
| O13 | bootstrap пула | `probeNonTerminalMountSlots` (`Pool/CasPool.cpp:453`) | LIST + `GET` | `Retry::standard` | V |
| O14 | `system.cas_mounts` | `listMounts` | LIST + `GET`, `expired` по wall-clock | только отображение | V |
| O15 | старт / декомиссия | `allocateWriterEpoch` → `probeSentinel(mount)`; `DecommissionRecovery`: `!gc_fenced && expires_at_ms > now_ms` | `HEAD`/`GET` | решает только mint epoch | V |

## 3. Сценарии {#scenarios}

Обозначения: H — держатель, S — преемник, Z — опоздавший.

| id | Кто опаздывает | Что успели другие | Что делает опоздавший при применении | Вред | На чём держится сегодня | V/I |
|---|---|---|---|---|---|---|
| **M1** | renew H (O6): ответ потерян / пакет задержан / запрос применён поздно | GC фенсит, или S reclaim | `IM(old)` → 412 | нет | store: ETag сменён фенсом/reclaim | V |
| M2 | renew H применён поздно, **до** любого действия | GC видит новый токен | слот продлён, H об этом не знает (дедлайн H считается от `attempt_start` последнего **известного** commit) | только задержка фенса/reclaim | store + дедлайн H консервативен | V |
| M3 | фенс GC (O8): лидер на паузе между `decide` и `IM`; или старый лидер‑зомби | H успел renew | `IM(observed)` → 412, `decide` заново | нет | store | V |
| M4 | фенс GC применён поздно, токен не менялся | — | ложится; H уже вне дедлайна: `first_seen ≥ apply(commit H) ≥ attempt_start H`; позднее = безопаснее | нет для слота | store + арифметика T3 | V |
| M5 | reclaim S (O3) применён поздно | слот не менялся | ложится; сертификат был ровно про этот ETag | нет для слота | store (`IM(got->etag)`) | V |
| M6 | farewell H (O7) применён поздно | — | ложится только над своим телом; посылается после drain, значит позднее ≠ хуже | нет для слота | store | V |
| **M7** | **данные** H (`_files/`, `roots/`, каталог, блоб): допуск до дедлайна, отправка/применение после | farewell H записан при неполном drain → S reclaim **сразу** (clean marker) | запись старого epoch поверх объектов S | потеря обновления / воскрешение (U3, U4, C*); ref-log защищён печатью | **ничего по времени**: только полнота drain; drain не гасит ушедшие запросы (codex MT7) | I |
| **M8** | данные H (SIGSTOP / своп / сетевой разрыв) после истечения аренды | GC: фенс через `≥ L·1.05 + P` от first_seen; затем `isCreatorFenceTerminal` / reclaim S / декомиссия | как M7 | как M7; плюс перехват строки `Creating` живого создателя (C4, C5) | **время T3**: запас ≈ резерв `2E+M` (16 с) + `P` (10 с) + `0.05L` (1.5 с) ≈ 27 с от последнего допуска | V механизм, I вред |
| **M9** | данные H после истечения аренды | рестарт того же uuid: observation `L·1.05 + P/2` → `proven_dead` reclaim | как M7 | как M7 | **время T4**: запас ≈ 16 + 5 + 1.5 ≈ 22 с | V механизм, I вред |
| **M10** | данные живого H | рестарт с `cas_unsafe_remount_no_delay`: reclaim **сразу**, в том числе над живым H (O3 `unsafe_authorized`) | как M7, окно до `L` | как M7 | ничего (T5, решение оператора) | V |
| **M11** | — (второй экземпляр, та же пара `(uuid, E)`: копия uuid + T6, неоднозначная выдача epoch) | B: `claimMount` same-pair refresh (`seq+1`, без сертификата) → renewer adopt `IM` | A: следующий renew → 412 → `same_epoch_state_uncertain` → terminal → self-remount с новым epoch (на базе) | два писателя одной пары до `L`: коллизии `(E,seq)` ref-log → fault (L9); `_files/` потерянное обновление; orphan sweep берёт `min_active` B и может счесть in-flight манифесты A eligible (I); clean farewell пары E перестаёт быть терминальным | **ничего** (U8/T7) | V код, I вред |
| **M12** | стартер A (epoch E, выделен раньше) на паузе между `allocateWriterEpoch` и `claimMount` | B (E+1) смонтирован, затем fenced / farewell | A: `claimMount` видит `E+1 ≠ E` + сертификат → `IM` reclaim → слот = `(U, E)`, **регресс** | печать recovery A закрывает только epoch `< E`; поздние записи E+1 не запечатаны; `SupersededEpoch` для строк E+1 «терминален» по слоту младшего epoch; orphan sweep: префиксы E+1 > слота → retain (безопасно) | время: пауза A ≥ жизнь B до фенса (T3); при `unsafe` — ничего, и B может быть жив (T8, CAS-178) | V код, I вред |
| M13 | renewer start (O5) на паузе после `claimMount` | GC фенсит (пауза ≥ T3), декомиссия NoWait: reclaim → drain → farewell → `IM`-delete mount и epoch → tombstone owner | `claim`: `GET` absent → `INM` **mint** слота; fence армируется от свежего anchor; записи в выведенный root | воскрешение слота и записей выведенного члена (U13); owner retired не проверяется | время (T3 + длительность декомиссии) | V код, I вред |
| M14 | `claimMount` (O1) поздний `INM` | декомиссия удалила слот | mint над пустым ключом | как M13 | `claimOwnerOrThrow` → `throwIfOwnerRetired` до claim, но окно «delete mount → tombstone owner» открыто (U13) | V код, I вред |
| M15 | `IM`-delete декомиссии (O10) применён поздно | S reclaim после farewell | 412 | нет | store (ETag farewell) | V |
| M16 | повтор SDK / движка того же запроса | — | conditional — single-attempt (SDK не повторяет); повтор движка шлёт те же байты → 412 → resolve-чтение узнаёт свои байты по `write_attempt_id` | нет | store + nonce в теле | V (профиль SDK — I по инвентарю) |
| M17 | два GC-лидера фенсят один токен | — | одинаковые байты фенса; второй получает 412 → resolve видит «свои» байты → Committed | нет (идемпотентно) | store | V |
| M18 | ETag ABA (те же байты переписаны) | — | невозможно: каждое тело держателя с новым 128‑бит `write_attempt_id`; фенс отличается `gc_fenced` | — | формат тела | V |
| M19 | устаревшее чтение `isCreatorFenceTerminal` (O11) | после чтения терминального состояния слот снова «живой» для той же пары | reconciler перехватывает строку | на базе возможно только через M11: clean farewell пары E → same-pair refresh E. `gc_fenced` поглощающий (`FencedSelf`). Другой epoch на базе возвращается только через M12 | на базе ничего при T7; на `3b4037` поглощающее | V |
| M20 | устаревшее чтение orphan sweep (O12) | — | только `retain` или тот же вывод | задержка (кроме M11) | RULE 1 (protection view) главнее | V |
| M21 | `DecommissionRecovery` по wall-clock (O15) | — | ложный «не live» → mint epoch; затем `claimMount(NoWait)` без observation отказывает живому | сгоревший номер epoch | store + строгий gate `claimMount` | V |
| M22 | `listMounts` `expired` по wall-clock | — | только отображение `system.cas_mounts` | нет | — | V |

Итог по объекту.

- Сам слот store-safe во всех схемах M1–M6, M15–M18 (V).
- Опасны M7–M14: сертификат «держатель мёртв» и его потребители. Плюс M19 как следствие M11.

## 4. Нужная строгость {#strictness}

| Сценарий | Худший вред | Насколько вероятно | Требование |
|---|---|---|---|
| M11 (T7) | два писателя одной пары до `L`: fault ref-log, потерянные `_files/`, возможные висячие манифесты | нужен дубль uuid **и** T6 (таймаут на записи epoch + гонка) либо ручная копия; T6 — обычный сетевой сбой, не пауза | **невозможно**: прямой фикс, дёшево |
| M12 (T8) | незапечатанный старший epoch, ложный `SupersededEpoch` | два процесса одного uuid + долгий старт одного из них (разрыв сети на старте). С `unsafe` хватает простого перезапуска | **невозможно**: прямой фикс, дёшево |
| M13/M14 (U13) | записи в выведенный root | пауза ≥ T3 + время декомиссии; редкий оператор | невозможно (это T12); на `3b4037` M13 закрыт |
| M8/M9 (T3/T4) | потеря `_files/`/`roots/`, перехват `Creating` | нужен запрос, применённый > 22–27 с после допуска: SIGSTOP/своп/VM‑пауза **или** разрыв сети. Закрытый по таймауту сокет без `SO_LINGER` (в `src/IO` нет `setLinger`, V) оставляет байты в ядре; при заживлении разрыва в пределах `tcp_retries2` (≈15 мин) и SigV4 (15 мин) запрос доставится и применится (I) | «минимизировано + обнаружено» допустимо **для слота**. Для целевых объектов без печати — нет; гарантию переносить на целевые ключи (T3) |
| M7 (farewell) | как M8, но **без временного запаса** | любой брошенный по таймауту не‑ref запрос перед shutdown | минимум: не писать farewell при неясных запросах (тогда M9, запас 22 с) |
| M10 (T5) | как M8, окно до `L` | гарантированно при дубле процесса | оставить, это явный риск оператора; плюс отказ при `epoch` слота `>` своего (иначе M12) |

## 5. Таймауты {#timeouts}

| Запрос | Что ограничивает сегодня | Что не ограничено |
|---|---|---|
| `GET`/LIST (O1, O8, O11–O14) | `Retry::standard` 90 с — проверка **между** попытками. Попытка: connect `C`, Poco recv/send **на одну операцию сокета** (`T`). Тело слота ~300 Б → 1–2 операции → ≈ `E = T + 2C` | DNS (`getaddrinfo`), получение кредов (свои таймауты), медленный поток байтов (каждый recv сбрасывает таймер) — I |
| renew `IM` (O6) | `untilLeaseSafe`: последняя попытка начинается не позже `deadline − M − 2E` (`reservedFor(0,2)`) | применение хранилищем после последнего байта; для слота безвредно (M1, M2) |
| farewell (O7) | `untilLeaseSafe` + окно `max(10 с, 2E+2 с)`; на `3b4037` — `Retry::within`, без дедлайна аренды | то же |
| claim/reclaim (O1–O4) | `Retry::standard` на каждую операцию; `claimMountAwaitingExpiry` ≈ `(kMaxObservationRestarts+1) × порог` | план `gc_requests` — `Fence::open()`, аренда не участвует (её ещё нет) |
| фенс GC (O8) | `Retry::standard` | безвредно (M3, M4) |
| `IM`-delete (O10) | `Retry::standard` + `throwIfClaimLost` между шагами | — |
| **данные H** (источник M7–M10) | T1: `admit` при остатке ≥ `2E+M`; дальше только транспорт | пауза «допуск → `send`», многочастные циклы, повторы `WriteBufferFromS3`/`copyS3File` для `publish`, байты в ядре после close, обработка хранилищем |

**Дедлайн аренды в транспорте** (`lease_valid_until − M` перед каждой операцией сокета, таймаут = `min(настройка, остаток)`):

- Для слота ничего не меняет: его поздние запросы уже безвредны.
- Для M8/M9 сужает окно. Клиентская активность H кончается к `deadline − M`; действие S наступает через `≥ deadline + P/2 + 0.05L`.
- Остаются открыты:
  - пауза между последней проверкой и `send()`: SIGSTOP не ограничен;
  - байты, уже ушедшие в ядро: без abortive close (`SO_LINGER{1,0}`) при отказе от запроса ядро их дошлёт после заживления разрыва (I);
  - задержка применения на стороне хранилища после последнего байта;
  - повторы SDK у `publish` блоба (профиль по умолчанию, не слот).
- M7 не закрывается: farewell не ждёт ничего. M10 и M11 тоже: там время не участвует вовсе.

## 6. Перенос гарантии на хранилище {#store-options}

| Вариант | Что делает | Формат | Миграция | Размер | Риск | TLA+ |
|---|---|---|---|---|---|---|
| **A. T7: отказ same-pair** | `claimMount`: тело своей пары в слоте → `LiveDoubleStart`/`FencedSelf`, не refresh; renewer стартует от тела claim (`MountClaimed`), без `GET`+adopt и без mint над absent. Это `3b4037` (`17c7f28`, `56d876f`) | нет | нет | ~60–100 строк, 3 файла (`CasServerRoot.{h,cpp}`, `CasPool.cpp`) + тесты | низкий; плюс отдельно T6, иначе legit-рестарт после неоднозначной выдачи ловит отказ (доступность) | уже есть: `CaCasMountCore` `_sab_refreshownpair` (RED на базе), `_twins`/`_twins_v9` (GREEN) |
| **B. T8: монотонность** | reclaim только при `existing.writer_epoch < our_epoch`, иначе `ABORTED` (`3b4037` так делает); `classifyFenceCertificate`: `SupersededEpoch` только при `lease.writer_epoch > fence_writer_epoch`. При `<` → `None` (блок) | нет | нет. `<` в слоте легитимен только после сброса epoch декомиссией (T12); там блок = доступность, лечится T12 | ~10–20 строк, 1 файл + тесты | низкий | reclaim: `ClaimNeverRegressesEpoch`, `_sab_claimregress`. Чтение `SupersededEpoch` не моделировано (§7) |
| **C. Farewell только при пустом наборе неясных запросов** | `CasRequests` считает брошенные запросы с `sent_any` (не‑ref планы); при `> 0` farewell не пишется → S идёт через T4 (M7 → M9) | нет | нет | ~40–80 строк, 3–4 файла | средний: ложные «неясные» → медленный рестарт | `CaDiskLifecycle`: обобщить `NoFarewellAfterUnclearRenewal` на любые запросы |
| **D. Сертификат в целевом ключе** (настоящий перенос) | все мутируемые объекты H — в ключах `…/<epoch>/…` или с `If-Match` на версию, которую меняет S при reclaim (copy-forward в свой epoch). Поздняя запись H ложится в мёртвое пространство имён. Это тема T3 | **да** (`_files/`, `roots/` под epoch) — читатели старого формата не увидят новые ключи; нужна двойная схема чтения | да: однократный copy-forward при первом mount нового формата | 500–1500 строк, 10+ файлов | высокий | новый модуль |
| **E. Оставить + контроль** | детекторы ниже | метаданные объекта — аддитивно, старые читатели игнорируют | нет | 50–150 строк | низкий | — |

Детекторы для варианта E:

- `CASMountExclusivityViolation`;
- строки `cas_log` `MountConflict` с ветками `same_epoch_state_uncertain` / `superseded` / `newer_epoch` (`3b4037`);
- счёт отклонённых печатью straggler'ов при recovery (`W_SealRejectedRetry`): поздние ref-записи, доказательство M8/M9 в поле;
- новое: штамп `x-amz-meta-cas-writer = (epoch, write_attempt_id)` на `_files/`/`roots/` и проверка в fsck: «запись epoch X позже reclaim слота epoch Y > X» → находит M7–M10 постфактум;
- fsck: ref-log epoch `>` epoch слота → находит M12.

## 7. TLA+ {#tla}

| Сценарий | Модель / оператор | Статус |
|---|---|---|
| M1, M2 | `CaMountRenewRetryCore`: `LateDeliveryRefused`, `LateDeliveryCannotOverwriteSuccessor`, `LandGhost`, `ConfirmedDeadlineNeverExtendedByResponse`, `LateSuccessNeverOpensWrites` | покрыто |
| M3, M4 | `CaMountObserveCore`: `ObsDecide`, `ObsFenceWrite`, `ObsReRead`, `NoFenceInsideAuthority`, `FencingWaitCoversDeclaredLease` | покрыто для слота |
| M8, M9 (данные после допуска) | `CaMountObserveCore:Write` атомарен с допуском (`hclock < AuthorityEnd`); `CaCasMountCore:StragglerLands` — только ref-log под печатью | **не моделировано** для мутируемых объектов |
| M11 | `CaCasMountCore:ClaimAllowed` (`refreshOK` при `Twins = {}`), `ClaimEqualOverOther`, `OneWritableIncarnationPerUuid`; `_sab_refreshownpair` = база (RED `ClaimNeverRegressesEpoch`); `CaMountRenewRetryCore:SamePairTwin` | покрыто; база = sabotage |
| M12 | `CaCasMountCore:ClaimAllowed` `monotoneOK` (`localEpoch > mount.epoch`), `RegressesEpoch`, `ClaimNeverRegressesEpoch`; `_sab_claimregress` = база (RED, CAS-178) | покрыто; честная модель = `3b4037`, не база |
| M10 | `WallClockReclaim` (sabotage, ближайшее) | `unsafe` как отдельный путь не моделирован |
| M7 | `CaDiskLifecycle:I2EarnedFarewell`, `NoLandingAfterFarewell` — только publish/ref-lane | не‑ref запросы не моделированы |
| M13, M14 | `CaCasMountCore:ClearExpiredMount`, `AdoptRead`/`AdoptWrite`, `RemintEpochDecom` | частично; tombstone owner и retire не моделированы |
| M19 (`SupersededEpoch`, clean farewell как сертификат для каталога) | нет (`CaRefCatalogCore` по codex неверно процитирован) | **не моделировано** |
| M16–M18 | `CaMountRenewRetryCore:ExactAttemptOnly`, `DoubleConditionalLanding`, `_sab_ignore_attempt_id`; `CaCasMountCore:W_SameBytesCollision` | покрыто |

Минимальные добавления:

1. `CaMountObserveCore`: разбить `Write` на `Admit` (при `hclock < AuthorityEnd − Reserve`) и `Land` (недетерминированная задержка ≤ `MaxDelay`). Инвариант `NoLandAfterFence`. RED при `MaxDelay > Reserve + P + drift`, GREEN ниже. Документирует временной остаток T14 числом.
2. `CaCasMountCore`: действие `ReadCertificate(r, e)` по правилу `classifyFenceCertificate` и инвариант `CertImpliesNoWriter(e)`: сертификат для `e` ⇒ ни одна инкарнация с epoch `e` не `CanWrite`. Ловит `<` после регресса и clean farewell + same-pair.
3. `CaCasMountCore`: start-ветка reclaim по clean marker (сейчас только `SabReacqClean`) с недетерминированным неполным drain → `StragglerLands` по не‑ref ключу.

## 8. Вывод {#verdict}

Сам слот **безопасен**: все записи условные, в теле nonce, ABA нет, поздние запросы самого слота безвредны (V). Сертификат «держатель мёртв»:

- **safe-by-time** для фенса GC и observation-reclaim: запас ≈ 27 с и ≈ 22 с;
- **unsafe** для same-pair refresh (T7), регресса epoch (T8), `unsafe_remount` (T5) и clean farewell при неполном drain (M7).

Рекомендация:

1. Портировать A и B из `3b4037` (≈100 строк, без формата). B дополнить `>` в `classifyFenceCertificate`: в `3b4037` там всё ещё `≠`.
2. Добавить C (≈60 строк).
3. Временной остаток T3/T4 не чинить на уровне слота: переносить гарантию на целевые ключи (D, тема T3).
4. До D включить детекторы E.

**Отличие `3b4037ae265`** (V, `git diff d76b59f9295 3b4037ae265 -- CA/Pool/CasServerRoot.cpp`):

- `claimMount` отказывает при `existing.writer_epoch > our_epoch` (ABORTED, «newer_epoch») и при своей паре (`same_epoch_occupied` → `LiveDoubleStart`);
- reclaim только над младшим epoch;
- `claimMountAwaitingExpiry` не ждёт свою пару, а при старшем epoch перечитывает и отказывает;
- renewer стартует от тела claim (`start(const MountClaimed &)`), без `GET`, adopt и mint → M13 закрыт;
- renew получил режим `untilDefinitive`;
- farewell — `Retry::within(window)` без дедлайна аренды;
- self-remount удалён;
- `classifyFenceCertificate` **не изменён**: `SupersededEpoch` = любое неравенство.
