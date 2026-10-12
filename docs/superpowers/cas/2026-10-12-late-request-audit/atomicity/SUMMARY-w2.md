# Атомарность моделей vs код: волна 2 {#atomicity-w2}

21 модель, отчёты `w2-{gc,gclease,ref,misc}.md`. V = проверено в коде codex, I = вывод.

| # | Находка | Модель | V/I | Backlog |
|---|---|---|---|---|
| 1 | Холодный читатель видит `Live`, удаление отставляет жизнь до вставки runtime; отложенная вставка публикует её снова; тёплый `namespaceLife` возвращает отставленную жизнь; `putNamespaceFile` подтверждает байты, которые janitor мёртвой жизни удалит | ref-модели | V | новая |
| 2 | `precommitAdd` прошёл `requireAlive`; отмена/перерождение до того, как append захватил runtime → старое замыкание даёт владение в жизни преемника; у promote та же дыра по epoch | `CaRefLaneCore` | V (потеря I) | новая |
| 3 | Разрешение wedge публикует `Faulted`, отпускает `state_mutex`, потом латчит fault монтирования — в окне другая таблица проходит допуск (lease-серия) | `CaRefLaneCore:FaultHasExit` | V | новая |
| 4 | Окно terminal-латча: открытие прошло проверку, другой поток латчит потерю, arming сбрасывает `lost`, допуск успевает до перепроверки | `CaDiskLifecycle` | V | новая |
| 5 | Воссоздание счётчика `epoch` гоняется с подготовленным claim (отсутствие и mint в модели атомарны) | `CaWriterEpochBackfillCore`/`CaCasMountCore` | V (потеря I) | → CAS-363 |
| 6 | Опубликованный по таймауту snapshot/`_ckpt` переживает drain при shutdown → farewell → запрос ложится; `NoLandingAfterFarewell` не обоснован | `CaDiskLifecycle` | V | → CAS-179 |
| 7 | Отложенный `Clean` писателя переоткрывает адопцию осуждённой инкарнации: retry примирения снимает новый `Condemned` без повторного PUT | `CaRetiredInRun*` | I (шаги V) | → CAS-334 |
| 8 | Lease GC: прогресс heartbeat не отменяет кражу (`NoFalseSteal` ложен); публикацию защищает токен состояния — только живучесть | `CaGcLeaseCore` | V | новая (Low) |
| 9 | `CaGcRoundDeferCore` запрещает удаление через атомарный guard, которого у кода нет: «нет несвёрнутого добавления» → писатель коммитит → `DELETE` | `CaGcRoundDeferCore` | I | новая (research) |
| 10 | Ворота модели гейтят и осуждение, код — только выпуск/удаление; дефолт и доказательство пустоты различаются | `CaGcDestructiveGateCore` | V | → CAS-375 |
| 11 | Abort/timeout стирают промежуточные состояния (pre-CAS delete или артефакт лёг, раунд проиграл) | gc-модели | V | → CAS-375 |
| 12 | Durability лога склеена с подтверждением; актор удаления relink ограничен сильнее кода; heartbeat create по таймауту ломает историческое доказательство `GcLiveGate` | misc, gclease | V/I | → CAS-375 |
