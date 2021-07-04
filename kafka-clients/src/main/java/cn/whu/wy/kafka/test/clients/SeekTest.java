package cn.whu.wy.kafka.test.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * @Author WangYong
 * @Date 2021/07/03
 * @Time 21:31
 */
public class SeekTest {
    private static final String TOPIC = "seek-test-topic";
    static KafkaHelper<String, String> kafkaHelper = new KafkaHelper<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        if (kafkaHelper.listTopics().contains(TOPIC)) {
//            System.out.println("delete topic:" + TOPIC);
//            kafkaHelper.getAdminClient().deleteTopics(Collections.singletonList(TOPIC));
//        }
//
//        KafkaProducer<String, String> producer = kafkaHelper.genProducer();
//        for (int i = 0; i < 10; i++) {
//            producer.send(new ProducerRecord<>(TOPIC, "message" + i)).get();
//        }
//        System.out.println("send 10 msg");
//        producer.close();

        KafkaConsumer<String, String> consumer = kafkaHelper.genConsumer("g_2", "c_1");
        consumer.subscribe(Collections.singletonList(TOPIC));
        consumer.poll(Duration.ofMillis(1000));
        consumer.seekToBeginning(kafkaHelper.topicPartitions(TOPIC));
        int cnt = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            System.out.println("records.count()=" + records.count());
            if (records.isEmpty())
                consumer.seekToBeginning(kafkaHelper.topicPartitions(TOPIC)); //  这个逻辑为什么必须要。否则poll不到消息
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
                cnt++;
            }
            if (cnt == 10) {
                break;
            }
        }

        System.out.println("consume 10 msg");

        System.out.println(kafkaHelper.describe(TOPIC));
        System.out.println(kafkaHelper.listTopics());
        System.out.println(kafkaHelper.partitions(TOPIC));
        System.out.println(kafkaHelper.topicPartitions(TOPIC));

        consumer.seek(kafkaHelper.topicPartitions(TOPIC).get(0), 5);
        cnt = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
                cnt++;
            }
            if (cnt == 4) {
                break;
            }
        }
        System.out.println("done");
        consumer.close();

    }
}
