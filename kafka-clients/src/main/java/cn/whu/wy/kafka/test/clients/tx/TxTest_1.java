package cn.whu.wy.kafka.test.clients.tx;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 测试事务型的消费者，是否能消费未启用事务的生产者发送的消息
 * <p>
 * https://www.baeldung.com/kafka-exactly-once
 *
 * @author WangYong
 * date 2022/05/29
 * Time 23:10
 */
@Slf4j
public class TxTest_1 {
    static final String BOOTSTRAP_SERVERS = "192.168.191.128:9092";
    static final String TEST_TOPIC = "test-topic";

    public static void main(String[] args) throws IOException, InterruptedException {

        TxTest_1 txTest_1 = new TxTest_1();
        log.info("create topic:{}", txTest_1.createTopic(TEST_TOPIC));

        KafkaProducer<String, String> producer = txTest_1.genProducer();
        KafkaConsumer<String, String> consumer = txTest_1.genConsumer();

        Executors.newSingleThreadExecutor().execute(() -> {
            log.info("consume...");
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    log.info("record={}", record.toString());
                }
            }
        });

        log.info("send...");
//        producer.initTransactions();
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>(TEST_TOPIC, "hello-" + i));
            TimeUnit.SECONDS.sleep(1);
        }

//        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
//        for (TopicPartition partition : records.partitions()) {
//            List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
//            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
//            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
//        }
//        producer.sendOffsetsToTransaction(offsetsToCommit, "my-group-id");

//        producer.commitTransaction();

    }

    public AdminClient getAdminClient() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        return AdminClient.create(props);
    }

    public boolean createTopic(String topic) {
        AdminClient client = getAdminClient();
        NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
        return client.createTopics(Collections.singleton(newTopic)).all().isDone();
    }

    public KafkaProducer<String, String> genProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

//        props.put("enable.idempotence", "true");
//        props.put("transactional.id", "prod-1");

        return new KafkaProducer<>(props);
    }

    public KafkaConsumer<String, String> genConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", "tx_test");
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "tx_test_1");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("isolation.level", "read_committed");

        return new KafkaConsumer<>(props);
    }
}
