package cn.whu.wy.kafka.test.clients;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

/**
 * @author WangYong
 * Date 2021/07/03
 * Time 21:31
 */
public class SeekTest {
    private static final String TOPIC = "seek-test-topic";
    static KafkaHelper<String, String> kafkaHelper = new KafkaHelper<>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // if has no data, send some
        if (!kafkaHelper.listTopics().contains(TOPIC)) {
            KafkaProducer<String, String> producer = kafkaHelper.genProducer();
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "message" + i)).get();
            }
            System.out.println("send 10 msg");
            producer.close();
        }

        KafkaConsumer<String, String> consumer = kafkaHelper.genConsumer("seek-test_g_1", "c_1");
        consumer.assign(kafkaHelper.topicPartitions(TOPIC));
        System.out.println("assignment:" + consumer.assignment());


        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            System.out.println("records.count()=" + records.count());

            //  这个逻辑必须要，否则poll不到消息。因为当前的消费进度可能已是最新的
            if (records.isEmpty()) {
                consumer.seekToBeginning(kafkaHelper.topicPartitions(TOPIC));
            } else {
                break;
            }
        }

        System.out.println("records.count()=" + consumer.poll(Duration.ofMillis(100)).count());
        consumer.seek(kafkaHelper.topicPartitions(TOPIC).get(0),5);
        System.out.println("records.count()=" + consumer.poll(Duration.ofMillis(100)).count());

        System.out.println(kafkaHelper.describe(TOPIC));
        System.out.println(kafkaHelper.listTopics());
        System.out.println(kafkaHelper.partitions(TOPIC));
        System.out.println(kafkaHelper.topicPartitions(TOPIC));
        System.out.println("done");
        consumer.close();

    }
}
