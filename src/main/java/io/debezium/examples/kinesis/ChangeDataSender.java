package io.debezium.examples.kinesis;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordRequest;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.MemoryDatabaseHistory;
import io.debezium.util.Clock;

/**
 * Demo for using the Debezium Embedded API to send change events to Amazon Kinesis.
 */
public class ChangeDataSender implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeDataSender.class);

    private static final String APP_NAME = "kinesis";
    private static final String KINESIS_REGION_CONF_NAME = "kinesis.region";

    private final Configuration config;
    private final JsonConverter valueConverter;
    private final AmazonKinesis kinesisClient;

    public ChangeDataSender() {

         config = Configuration.create()
                .with(EmbeddedEngine.CONNECTOR_CLASS, "io.debezium.connector.mysql.MySqlConnector")
                .with(EmbeddedEngine.ENGINE_NAME, "kinesis")
                .with(MySqlConnectorConfig.SERVER_NAME, "kinesis")
                .with(MySqlConnectorConfig.SERVER_ID, 8192)
                .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                .with(MySqlConnectorConfig.PORT, 3306)
                .with(MySqlConnectorConfig.USER, "debezium")
                .with(MySqlConnectorConfig.PASSWORD, "dbz")
                .with(MySqlConnectorConfig.DATABASE_WHITELIST, "inventory")
                .with(MySqlConnectorConfig.TABLE_WHITELIST, "inventory.customers")
                .with(EmbeddedEngine.OFFSET_STORAGE,
                        "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
                .with(MySqlConnectorConfig.DATABASE_HISTORY,
                        MemoryDatabaseHistory.class.getName())
                .with("schemas.enable", false)
                .build();


        valueConverter = new JsonConverter();
        valueConverter.configure(config.asMap(), false);

        final String regionName = "us-east-1";

        final AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider("default");

        kinesisClient = AmazonKinesisClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(regionName)
                .build();
    }

    @Override
    public void run() {
        final EmbeddedEngine engine = EmbeddedEngine.create()
                .using(config)
                .using(this.getClass().getClassLoader())
                .using(Clock.SYSTEM)
                .notifying(this::sendRecord)
                .build();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Requesting embedded engine to shut down");
            engine.stop();
        }));

        awaitTermination(executor);
    }

    private void awaitTermination(ExecutorService executor) {
        try {
            while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.info("Waiting another 10 seconds for the embedded engine to shut down");
            }
        }
        catch (InterruptedException e) {
            Thread.interrupted();
        }
    }

    private void sendRecord(SourceRecord record) {
        // We are interested only in data events not schema change events
        if (record.topic().equals(APP_NAME)) {
            return;
        }

        Schema schema = SchemaBuilder.struct()
            .field("key", record.keySchema())
            .field("value", record.valueSchema())
            .build();

        Struct message = new Struct(schema);
        message.put("key", record.key());
        message.put("value", record.value());

        // I think the partitionKey should be use other method than using hashCode method

        String partitionKey = String.valueOf(record.key() != null ? record.key().hashCode() : -1);
        final byte[] payload = valueConverter.fromConnectData("dummy", schema, message);

        PutRecordRequest putRecord = new PutRecordRequest();

        putRecord.setStreamName(streamNameMapper(record.topic()));
        putRecord.setPartitionKey(partitionKey);
        putRecord.setData(ByteBuffer.wrap(payload));

        kinesisClient.putRecord(putRecord);
    }

    private String streamNameMapper(String topic) {
        return topic;
    }

    public static void main(String[] args) {
        new ChangeDataSender().run();
    }
}
