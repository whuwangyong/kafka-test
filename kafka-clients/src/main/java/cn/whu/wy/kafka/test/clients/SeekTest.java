package cn.whu.wy.kafka.test.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

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
//            kafkaHelper.getKafkaAdmin().deleteTopics(Collections.singletonList(TOPIC));
//        }
//
        KafkaProducer<String, String> producer = kafkaHelper.genProducer();
//        for (int i = 0; i < 10; i++) {
//            producer.send(new ProducerRecord<>(TOPIC, "message" + i)).get();
//        }
//        System.out.println("send 10 msg");

        KafkaConsumer<String, String> consumer = kafkaHelper.genConsumer("g_3", "c_1");
        consumer.subscribe(Collections.singletonList(TOPIC));
        int cnt = 0;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
                cnt++;
            }
            if (cnt == 10) {
                break;
            }
        }

        System.out.println("consume 10 msg");

        producer.close();
        consumer.close();

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

    }
}
