import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.triggers.ITrigger;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class CassandraKafkaTrigger implements ITrigger {

    private static final Logger logger = LoggerFactory.getLogger(CassandraKafkaTrigger.class);
    private static KafkaProducer<String, String> producer;
    private final ThreadPoolExecutor threadPoolExecutor;
    private String topic_default = "test";
    private String kafka_default = "kafka:9092";
    private boolean log_info;
    private static Map<String, Object> configuration;
    private static final String FILE_PATH = "/etc/cassandra/conf/triggers/KafkaTrigger.yml";

    public CassandraKafkaTrigger() {
        checkLastModified();
        Map<String, Object> kafkaParams = new HashMap<>();
        log_info = (Boolean) getProperty("log.info", configuration,true);
        kafkaParams.put("bootstrap.servers", (String) getProperty("bootstrap.servers", configuration,kafka_default));
        kafkaParams.put("key.serializer", StringSerializer.class);
        kafkaParams.put("value.serializer", StringSerializer.class);
        kafkaParams.put("group.id", "kafka-trigger1");
        producer = new KafkaProducer<>(kafkaParams);
        threadPoolExecutor = new ThreadPoolExecutor(4, 40, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }

    @Override
    public Collection<Mutation> augment(Partition partition) {
        threadPoolExecutor.execute(() -> readPartition(partition));
        return Collections.emptyList();
    }


    private void readPartition(Partition partition) {
        String key = getKey(partition);
        String table = partition.metadata().cfName;
        checkLastModified();
        List<JSONObject> rows = new ArrayList<>();
        if (partitionIsDeleted(partition)) {
            //partition deleted
        } else {
            UnfilteredRowIterator it = partition.unfilteredIterator();
            while (it.hasNext()) {
                Unfiltered un = it.next();
                if (un.isRow()) {
                    JSONObject jsonRow = new JSONObject();
                    Clustering clustering = (Clustering) un.clustering();
                    String clusteringKey = clustering.toCQLString(partition.metadata());
                    //HardCode names of primary key :
                    if (table.contains("TEST_TABLE")) {
                        jsonRow.put("session_id", key); //custom field
                        jsonRow.put("event_id", clusteringKey); //custom field
                    }
                    // more else if
                     else {
                        logger.error("Unknown table {}", partition.metadata().cfName);
                        return;
                    }

                    Row row = partition.getRow(clustering);
                    if (rowIsDeleted(row)) {
                        //row deleted
                    } else {
                        Iterator<Cell> cells = row.cells().iterator();
                        Iterator<ColumnDefinition> columns = row.columns().iterator();
                        while (cells.hasNext() && columns.hasNext()) {
                            ColumnDefinition columnDef = columns.next();
                            Cell cell = cells.next();
                            if (cell.isTombstone()) {
                                //"Cell is tombstone (deleted)"
                            } else {
                                //Check your types
                                    //custom check for map only:
                                if (columnDef.type instanceof MapType) {
                                    boolean first = true;
                                    JSONArray array = new JSONArray();
                                    while (cells.hasNext()) {
                                        if (first) {
                                            first = false;
                                        } else {
                                            cell = cells.next();
                                        }
                                        AbstractType<?> abstractType = cell.column().type;
                                        AbstractType keyType = ((MapType) abstractType).getKeysType();
                                        try {
                                            String keyMap = byteBufferToString(keyType, cell.path().get(0)).left;
                                            array.add(keyMap + "=" + keyType.getString(cell.value()));
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    jsonRow.put(columnDef.name.toString(), array);

                                } else {
                                    //Your types:
                                    String data = columnDef.type.getString(cell.value());
                                    if (columnDef.type instanceof TimestampType | columnDef.type instanceof DateType) {
                                        data = checkTS(data);
                                    }
                                    if (columnDef.type instanceof BooleanType) {
                                        data = data.equals("true") ? "1" : "0";
                                    }
                                    jsonRow.put(columnDef.name.toString(), data);
                                }
                            }
                        }
                    }
                    rows.add(jsonRow);
                } else if (un.isRangeTombstoneMarker()) {
                }
            }
            String topic=(String) getProperty("topic."+table, configuration,topic_default);
            int i = 0; //unique key for multirows
            for (JSONObject row : rows) {
                String value = row.toJSONString();
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key + "-" + i, value);
                producer.send(record);
                if(log_info)
                    logger.info("Record sent: {}", value);
                i++;
            }
        }

    }

    //reload config
    private static long lastModifed=0;
    private static void checkLastModified(){
        long time = new File(FILE_PATH).lastModified();
        if(lastModifed!=time);
        logger.info("reload config");
        lastModifed=time;
        configuration=loadConfiguration();
    }

    private String checkTS(String ts) {
        logger.info("checkTS : ",ts);
        if (ts.contains("+"))
            ts = ts.substring(0, ts.indexOf('+'));
        if (ts.contains("Z"))
            ts = ts.substring(0, ts.indexOf('Z'));
        if (StringUtils.countMatches(ts, ":") < 2)
            ts += ":00";
        return ts;
    }

    private String getKey(Partition partition) {
        return partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
    }

    private boolean partitionIsDeleted(Partition partition) {
        return partition.partitionLevelDeletion().markedForDeleteAt() > Long.MIN_VALUE;
    }

    private boolean rowIsDeleted(Row row) {
        return row.deletion().time().markedForDeleteAt() > Long.MIN_VALUE;
    }

    private static Pair<String, Boolean> byteBufferToString(AbstractType<?> abstractType, ByteBuffer value)
            throws IOException {

        if (value == null) {
            return Pair.create(null, Boolean.FALSE);

        } else if (abstractType instanceof UTF8Type) {

            UTF8Type type = (UTF8Type) abstractType;
            return Pair.create(type.getString(value), Boolean.FALSE);

        }
        throw new IOException("Unsupported type:" + abstractType);
    }

    private static Map<String, Object> loadConfiguration() {
        InputStream stream = null;
        try {
            stream = new FileInputStream(new File(FILE_PATH));
            Yaml yaml = new Yaml();
            return (Map<String, Object>) yaml.load(stream);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            FileUtils.closeQuietly(stream);
        }
    }

    private Object getProperty(String key, Map<String, Object> configuration,Object def) {
        if (!configuration.containsKey(key)) {
            logger.error("Property: " + key + " not found in configuration.");
            return def;
        }
        return configuration.get(key);
    }

}
