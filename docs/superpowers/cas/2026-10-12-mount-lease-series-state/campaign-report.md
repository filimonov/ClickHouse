# Серия mount-lease: итоговый отчёт кампании (2026-10-07 → 2026-10-09) {#seriya-mount-lease-itogovyy-otchet-kampanii-2026-10-07-2026}

Все 16 кодовых PR серии реализованы, отревьюированы и собраны в читаемые коммиты. Три слиты, четыре открыты, девять готовы локально и ждут слияния предшественников. Спека — r8 (`0cb9107ed87`).

## Результат по PR {#by-pr}

| PR | Ветка (squash) | Head | Коммитов | Prod +/− | Статус |
|---|---|---|---|---|---|
| 1 GC: sighting после чтения | `cas-gc-sighting-after-read` | bc7f453aa4a | 1 | — | **слит** #2509 |
| 5 probeSentinel на границе | `cas-probe-sentinel-boundary` | 58debc8b370 | 1 | — | **слит** #2510 |
| 3 memory guard в продлении | `cas-renewal-memory-guard` | 949578b2d73 | 1 | — | **слит** #2511 |
| 7 монотонность claim | `cas-mount-claim-monotonicity` | 88953e60433 | 1 | — | открыт #2512 |
| 4 прогресс продления | `cas-mount-renewal-progress` | 0a8317178b0 | 1 | — | открыт #2513 |
| 2 v2 observers ждут declared lease | `cas-observers-wait-declared-lease` | 732155f15e7 | 1 | — | открыт #2516 (на PR 1) |
| 8 продление до definitive | `cas-mount-renewal-until-definitive` | 2f650d9efe9 | 7 | +348/−90 | открыт #2517 (на 1+4) |
| 10 v2 одна ref-lane fault | `cas-ref-lane-fault-latch-squash` | b9a51702188 | 1 | — | готов (независим) |
| CAS-349 temp roots тестов | `cas-unit-test-temp-roots` | 458b49619ee | 1 | — | готов (независим) |
| 9a system table, `is_local` | `cas-mounts-table-local-path` | 07458806831 | 1 | +284/−308 | готов, после 8 |
| 9b operator budget contract | `cas-operator-budget-contract-7` | 674c618ae18 | 7 | +354/−113 | готов, после 9a (docs-фикс перед открытием) |
| 11a ownership primitives | `cas-ownership-primitives-5` | ef8a1c57bad | 5 | +406/−140 | готов, после 9a |
| 11b lossless claim | `cas-lossless-claim-4` | dabbfabfb28 | 4 | +153/−242 | готов |
| 12 terminal losses + stop | `cas-terminal-losses-stop-10` | a470f914557 | 10 | +803/−122 | готов |
| 13 farewell rules | `cas-farewell-rules-5` | 470e1eb1f6d | 5 | +116/−67 | готов |
| 14 reacquire в нити продления | `cas-reacquire-in-lease-thread-6` | 903e03ae516 | 6 | +851/−43 | готов |
| 15 снос remount-машины | `cas-remove-remount-machine-9` | b81092d3e85 | 9 | +551/−1612 | готов (на merge 14 + PR 3) |
| 16 снос observability-стека | `cas-remove-observability-stack-7` | 3b4037ae265 | 7 | −289 нетто | готов, после 15 |

Отклонено: PR 6 (absence per verb) — чувствителен к формату ответа; заменён LIST-подтверждением в PR 12. Переделаны по решению владельца: PR 2 (v2), PR 10 (v2).

Ветки живут в `cas-95-1` и `cas-janitor-budget` (обе — одни и те же объекты). Гейты на каждом финальном дереве: `CAS*` release + ASan, интеграционные модули там, где план требовал; мутационные/красные доказательства в логах `build*/pr<N>_*.log`.

## Что изменилось в поведении {#behaviour}

- Пауза ≠ потеря: продление повторяется до определённого исхода (одна неизменяемая пара, шаг 0,8–1,2 с); нет deadline-счётчика и второго PUT.
- Observers (GC floor, рестарт того же srid) ждут `max(своя, задекларированная) + 5% + свой шаг`; настройка lease — per-server.
- Потери: таблица R8–R18 с одной `ledger_fault`; внешние потери → LOG_ERROR + latch + graceful SIGTERM (process-directed); сломанные инварианты → LOG_FATAL + `std::terminate` (**предварительно**, условие — тест «shutdown не висит на мёртвом диске»).
- Reacquire после своего `gc_fenced` — в той же нити, через сертификат (`closeOldEpoch → claimNewEpoch`); remount-машина, второй поток, parking, `DriverLease`, состояния `IdentityLost`/`VanishedReplaced` удалены.
- `system.cas_mounts`: жизненный цикл из часов (`live`/`paused(...)`/`stopping`/`closed`), `is_local` без I/O; один `CasLeaseThresholds` для допуска, лог-строки и таблицы; тексты отказов называют причину.
- Абсенс только с подтверждением LIST (`ProvenAbsent`); 404-семантика не привязана к глаголу.
- Farewell после истечения условен по нашему ETag и ограничен по времени.
- Счётчики: 18 удалены, 4 добавлены (`CASMountReacquire*`, `CASMountLeaseExpired`), 2 перенаправлены.

## Баланс кода {#balance}

База → PR 15: production +3537/−2582 (нетто +955); PR 16: −289. Тесты +8574/−3168. Структурно проще (одна нить, один классификатор, одна формула порога, типизированные исходы), но не по строкам — см. открытые вопросы.

## Модели (TLA+/TLC) {#models}

Обновлены до реализации каждого PR: `CaMountObserveCore`, `CaMountRenewRetryCore`, `CaDiskLifecycle`, `CaCasMountCore`, `CaRefLaneCore` (24 коммита в `docs/superpowers/models` с 2026-10-07). Модели трижды поймали артефакты до кода (11a: fence latch от AllocBegin; 11b: latch от ClaimBegin; 12: постоянный stopLatch); соундность `ViewNoHistory` проверена и записана. Правила запуска: `timeout 1200`, 4 воркера, `-Xmx8g`, дисковый guard, малый MaxClock.

## CI {#ci}

Красные на наших PR — все не наши (Iceberg UBSan, Grype, Hybrid stress, Swarms infra, `04605…` = upstream #115986, DNS cache, QueryAnalyzer). Три CAS-кандидата — существующие классы: CAS-353 (медленный renewal PUT / slack 4 с), CAS-354 (404 при Error), CAS-355 (последовательный reclaim). RCA: `tmp/lease-series/rca/ci-rca-2513-2510.md`.

## Backlog {#backlog}

Новые задачи: CAS-348…356 (349 temp roots — готов; 350 deposed gate в окне remount; 351 recovery-seal fault — ветка `cas-ref-lane-recovery-seal-fault` на PR 10 v2, нужен раунд B; 352 LIST-подтверждённый абсенс — закрыт PR 12; 353–355 CI-классы; 356 перенацеливание soak/Grafana — диф сохранён, тест drilldown требует переопределяемого источника имён).

## Ждёт владельца {#owner}

1. Открыть PR 10 v2 и CAS-349 (независимые).
2. После слияния #2517: открыть 9a → 9b → 11a → 11b → 12 → 13 → 14 → 15 → 16 (перекладка на merge-коммиты).
3. Перезапустить упавшие джобы #2513/#2510?
4. Умолчания 60/15 (lease 60 с, период 15 с) — отдельный малый PR после 9b: да/нет/другие числа.
5. Перегруппировка (9a+9b, 11a+11b, 12→12a/12b/12c, 14+15): 5 стопочных + 4 независимых вместо 9 стопочных.
6. «Сжимающий» проход после 16 (−400…−600 строк) или назвать фичи под нож.
7. Пересмотр SIGTERM/terminate (помечено предварительным).
8. Отложенные: 17b/18, чтения при паузе, CAS-149.1/156.1/185/289.4; удаление внешних тестовых префиксов (AWS/GCS); s39 leg B — редизайн, не переименование.
9. Локальный скилл `~/.claude/skills/analyzing-cas-health` читает удалённые имена счётчиков.

## Отложенные правки прозы {#deferred-docs}

`tmp/lease-series/deferred-docs-fixes.md` — 9 пунктов (PR 10 v2 ×3, PR 12 ×2, PR 15 ×2, PR 16 ×1, `InterpreterSystemQuery.cpp`); применяются одним проходом перед открытием соответствующего PR.

## Артефакты {#artifacts}

- Спека: `docs/superpowers/specs/2026-10-07-cas-mount-lease-series-design.md` (r8); r7 — `tmp/lease-series/spec-r7.md`.
- Планы: `docs/superpowers/plans/2026-10-0{8,9}-cas-lease-pr*.md` (20 файлов).
- SDD-журналы: `.superpowers/sdd/<plan>/` (progress, impl-report, review-*, rereview-*).
- Тела PR: `cas-95-1/tmp/pr-body-lease-pr*.md`.
- Журнал кампании: `tmp/lease-series/CAMPAIGN.md`.
