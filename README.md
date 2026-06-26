# Raft KV Store

In-memory key-value хранилище поверх Raft-кластера из 5 процессов. Узлы общаются по TCP в формате JSON Lines, клиентские команды идут через отдельный CLI.

## Архитектура

- `ru.hse.drip.systems.raft.node` - роли Raft, election timeout, RequestVote, AppendEntries, backtrack, majority commit.
- `ru.hse.drip.systems.raft.storage` - persistent state: `currentTerm`, `votedFor`, `log[]`.
- `ru.hse.drip.systems.raft.transport` - короткие TCP request/response соединения, один JSON на строку.
- `ru.hse.drip.systems.raft.cli` - пользовательские команды, fault injection, запуск/убийство процессов, benchmark.
- `ru.hse.drip.systems.raft.protocol` - JSON DTO и доменные сущности.

KV state machine не пишется на диск. Узел применяет только committed-префикс журнала; после рестарта `commitIndex=0`, а актуальный committed-префикс подтягивается от лидера через AppendEntries.

## Persistent State

Для каждого узла файлы лежат в `<data-dir>/<nodeId>/`:

- `meta.json` - `currentTerm`, `votedFor`;
- `log.jsonl` - по одной `LogEntry {term, command}` на строку.

При `--fsync on` storage вызывает `FileChannel.force(true)` до возврата из операций, от которых зависит сетевой ответ:

- term/vote сохраняются до `RequestVoteResponse`;
- follower сохраняет усечение/дозапись лога до `APPEND_ENTRIES_RESP success=true`;
- leader сохраняет новую запись до рассылки AppendEntries.

`--fsync off` оставляет запись в page cache и нужен только для benchmark-сравнения.

## Запуск

Сборка. Gradle 8.8 нужно запускать под Java 21; если системная Java новее и Gradle падает на `Unsupported class file major version`, задайте `JAVA_HOME` явно:

```bash
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew build
```

Пять узлов в отдельных терминалах:

```bash
./gradlew runNode1
./gradlew runNode2
./gradlew runNode3
./gradlew runNode4
./gradlew runNode5
```

CLI:

```bash
./gradlew runCli --console=plain
```

Можно стартовать процессы из CLI:

```text
startCluster
leader
put x 1
get x
status
```

Параметры узла:

```bash
java -cp <classpath> ru.hse.drip.systems.raft.node.RaftNodeMain \
  --id n1 \
  --port 9101 \
  --data-dir data \
  --peers n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103,n4=127.0.0.1:9104,n5=127.0.0.1:9105 \
  --fsync on
```


## Code Quality And Concurrency

Код разделен по слоям, чтобы Raft-логика не зависела напрямую от файловой системы и TCP:

- `DurableState` - интерфейс persistent state; `FileDurableState` является файловой реализацией и может быть заменен in-memory реализацией в тестах.
- `RpcClient` - интерфейс исходящих RPC; `TcpRpcClient` является TCP-адаптером.
- `KeyValueStateMachine` применяет committed команды к KV и не знает про Raft.
- `PendingClientRequests` хранит ожидания клиентских `put` до продвижения `commitIndex`.
- `RaftNode` содержит собственно state machine протокола: роли, term, голосование, replication progress, commit.

Потокобезопасность устроена через один явный monitor `lock` внутри `RaftNode`: под ним меняются `currentTerm`, `votedFor`, `role`, `log`, `commitIndex`, `lastApplied`, replication progress и partition/delay state. Сетевые вызовы не выполняются под этим lock: `sendRpc` только снимает snapshot запроса и отправляет его через отдельный executor. Входящие TCP-соединения и исходящие RPC имеют таймауты, поэтому зависшие peers не блокируют узел навсегда.

Логи узла печатают ключевые события с `nodeId`, `term`, ролью/commitIndex: старт, выборы, переход в leader, append и продвижение commit.

## CLI

- `put <key> <value>` - запись через лидера, ответ после commit.
- `get <key>` - leader-read из примененного KV.
- `leader` - опрос всех узлов.
- `status` - `nodeId, role, currentTerm, commitIndex, log.length`.
- `logDump [--node <id>]` - журнал узла.
- `killNode <nodeId>` - узел вызывает `System.exit`.
- `startNode <nodeId>` - CLI поднимает новый процесс с тем же `--data-dir`.
- `partition <g1Ids> <g2Ids>` - обе группы блокируют исходящие RPC друг к другу.
- `healPartition` - очистка partition.
- `setDelay <nodeId> <ms>` - задержка исходящих RPC узла.
- `setElectionTimeout <minMs> <maxMs>` - настройка election timeout.
- `bench --puts <N> --threads <T> [--read-ratio <r>]` - нагрузка и запись строки в `benchmarks/results.csv`.

Follower на `get` и `put` отвечает `NOT_LEADER`/`NO_LEADER` с `leaderHint`; CLI автоматически ищет лидера и ретраит до таймаута.

## Протокол

Все сообщения идут как JSON Lines:

```json
{"type":"REQUEST_VOTE","term":3,"candidateId":"n4","lastLogIndex":47,"lastLogTerm":2}
{"type":"APPEND_ENTRIES","term":3,"leaderId":"n1","prefixLen":4,"prefixTerm":3,"leaderCommit":4,"suffix":[{"term":3,"command":"put x 8"}]}
{"type":"CLIENT_PUT","requestId":"...","key":"x","value":"1","command":"put x 1"}
```

Commit на лидере выполняется только если запись на границе commit создана в текущем term. Записи старых term фиксируются транзитом вместе с более новой записью текущего term.


## Benchmarks

Полный обязательный набор из 7 прогонов запускается одной командой:

```bash
JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 ./gradlew runRequiredBenchmarks
```

Команда поднимает свежие кластеры, выполняет части A/B/C, перезаписывает `benchmarks/results.csv`, три SVG-графика в `benchmarks/` и `report.md`.