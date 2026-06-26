# Demo Scenario

Старт `./gradlew runCli --console=plain`
## 1. Нормальная работа

```text
startCluster
leader
put x 1
put y 2
get x
status
logDump --node n1
logDump --node n3
```

Ожидаемо: один лидер в одном term; `get x` печатает `1`; у всех живых узлов одинаковые `commitIndex` и `log.length`; журналы `n1` и `n3` совпадают.

## 2. Падение лидера

```text
leader
killNode <leaderId>
leader
put z 9
startNode <leaderId>
status
logDump --node <leaderId>
```

Ожидаемо: после убийства лидера оставшиеся 4 узла выбирают нового лидера с большим term; запись `z=9` коммитится; старый лидер после старта догоняет журнал.

## 3. Разделение кворума

```text
partition n1,n2 n3,n4,n5
leader
put q majority
status
healPartition
status
```

Ожидаемо: группа из 3 узлов выбирает/сохраняет лидера и обслуживает `put`; группа из 2 узлов не может собрать majority. После `healPartition` журналы сходятся.

## 4. Расходящийся журнал

Терминал A:

```text
leader
setDelay <leaderId> 4000
put a 1
```

Пока `put a 1` висит из-за задержки исходящих RPC лидера, в течение этих 4 секунд выполнить в терминале B:

```text
killNode <leaderId>
```

После переизбрания:

```text
leader
logDump
put b 2
status
logDump
startNode <leaderId>
status
logDump
```

Ожидаемо: новый лидер делает backtrack по `sentLength`, follower с конфликтующим хвостом усекает uncommitted записи и дозаписывает suffix лидера. Запись `a 1`, если не успела стать committed, может быть усечена; это и есть демонстрация безопасного удаления незафиксированного хвоста.

## 5. Параллель с Paxos

```text
status
logDump
leader
```