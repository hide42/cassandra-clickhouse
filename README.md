1) Имеется 2 основых способа извлечения данных из кассандры: через cdc или trigger.


 Trigger является depricated, но cdc доступен только с версии 3.8, так что в первую очередь необходимо проверить версию cassandra на прод/конечном сервере.

# Trigger 
Кассандра будет вызывать augment на каждую партицию событий, событие(Mutation).
offtop:(хранимки и триггеры - bad practice из-за доп нагрузки CPU активной СУБД). Так же триггеры синхронные, но можно запустить доп. поток на выполнение, но тогда нельзя отследить выполнение.
Для использования триггеров необходимо имплиментировать интерфейс ITrigger

Для 3.0+
```java
public interface ITrigger {

    public Collection<Mutation> augment(Partition update);
}
```
Для 3.0-
```java
public interface ITrigger {

    public Collection<Mutation> augment(ByteBuffer partitionKey, ColumnFamily update);
}
```
Имплементация должна быть с конктруктором без параметра и stateless.

Поля записи получаются из Partition и состоят из key,clusteringKey(опционально), Row(columns и cells) offtop:извлечение данных из row, то еще удовольствие.
Собранный jar необходимо закинут в папку для триггеров кассандры etc/cassandra/triggers (может быть и другой), а затем подключить его в cassandra : 

Запустить следующую команду на сервере касандры, чтобы триггер подцепился. 
```
nodetool reloadTriggers
```

```
Повторить на всех узлах (для этого написан скрипт mvn package,mv trigger,nodetool reloadTriggers)
```

``` psql
CREATE TRIGGER kafka_trigger ON movies_by_genre USING '<path-to-class-in-project-src>';
```
Так если класс у вас в папке java, то просто пишите имя класса.

#CDC(change data capture):
Cassandra будет писать журналы событий, которые вы можете потом читать через имплиментацию [CommitLogReadHandler](https://github.com/apache/cassandra/blob/trunk/src/java/org/apache/cassandra/db/commitlog/CommitLogReader.java)

```java
public interface CommitLogReadHandler
{
    boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException;

    void handleUnrecoverableError(CommitLogReadException exception) throws IOException;

    void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc);
}
```
В handleMutation можно получить те же partitions, что и были описаны в тригерах
```
for (PartitionUpdate partitionUpdate : m.getPartitionUpdates()) {
            process(partitionUpdate);
}
```

Пример чтение из журнала:
```
CommitLogReader commitLogReader = new CommitLogReader();
commitLogReader.readCommitLogSegment(yourCommitLogReadHander, path.toFile(), false);
```


Чтобы cassandra начала писать в журнал событий, необходимо в конфигах самой кассандры включить cdc и запомнить cdc_raw_directory :
```
cdc_enabled: true
cdc_raw_directory: ???
cdc_free_space_check_interval_ms: #(default 250) 
cdc_total_space_in_mb: #(min 4096)
```
И у необходимой таблицы указать cdc = true при создании или через ALTER TABLE.

#Kafka - Clickhouse
Для этого можете использовать flink-clickhouse-sink от Ivi: https://github.com/ivi-ru/flink-clickhouse-sink
