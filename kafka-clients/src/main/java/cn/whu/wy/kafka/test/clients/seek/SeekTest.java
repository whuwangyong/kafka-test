package cn.whu.wy.kafka.test.clients.seek;

import cn.whu.wy.kafka.test.clients.KafkaHelper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 发现不管是新版还是旧版的poll方法，timeout都必须足够大。
 * 目前测试，在500ms以内，seek之后立即poll，大概率拉不到消息
 *
 * @author WangYong
 * Date 2021/07/03
 * Time 21:31
 */
public class SeekTest {
    private static final String TOPIC_1 = "seek-test-topic-1";
    private static final String TOPIC_2 = "seek-test-topic-2";

    //    private static final long POLL_TIMEOUT = 800;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);
    static KafkaHelper<String, String> kafkaHelper = new KafkaHelper<>("192.168.191.128:9092");

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (!kafkaHelper.listTopics().contains(TOPIC_1)) {
            kafkaHelper.createTopic(TOPIC_1, 2);
        }
        if (!kafkaHelper.listTopics().contains(TOPIC_2)) {
            kafkaHelper.createTopic(TOPIC_2, 2);
        }

        // send some data
//        send(TOPIC_1);
//        send(TOPIC_2);

        KafkaConsumer<String, String> consumer = kafkaHelper.genConsumer("seek-test_g_2", "c_1");


        fastSeekAndPoll(consumer, TOPIC_1, 0);
        fastSeekAndPoll(consumer, TOPIC_1, 1);
        fastSeekAndPoll(consumer, TOPIC_2, 0);
        fastSeekAndPoll(consumer, TOPIC_2, 1);

        seekAndPollInLoop(consumer, TOPIC_1, 0);
        seekAndPollInLoop(consumer, TOPIC_1, 1);
        seekAndPollInLoop(consumer, TOPIC_2, 0);
        seekAndPollInLoop(consumer, TOPIC_2, 1);

//        CheckAssign checkAssign = new CheckAssign();
//        seekWithRebalanceListener(consumer, checkAssign, TOPIC_1, 0);
//        seekWithRebalanceListener(consumer, checkAssign, TOPIC_1, 1);
//        seekWithRebalanceListener(consumer, checkAssign, TOPIC_2, 0);
//        seekWithRebalanceListener(consumer, checkAssign, TOPIC_2, 1);


        System.out.println("done");
        consumer.close();
    }

    private static void send(String topic) throws InterruptedException, ExecutionException {
        KafkaProducer<String, String> producer = kafkaHelper.genProducer();
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topic, "message" + i)).get();
        }
        System.out.println("send 10 msg");
        producer.close();
    }

    private static void fastSeekAndPoll(KafkaConsumer<String, String> consumer, String topic, int partition) {
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singleton(tp));
        System.out.println("assignment:" + consumer.assignment());
        consumer.seek(tp, 2);
        System.out.println("count=" + consumer.poll(POLL_TIMEOUT).count());
    }

    private static void seekAndPollInLoop(KafkaConsumer<String, String> consumer, String topic, int partition) throws InterruptedException {
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singleton(tp));
        System.out.println("assignment:" + consumer.assignment());
        consumer.seek(tp, 3);

        int loop = 1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
            if (records.isEmpty()) {
                loop++;
                TimeUnit.MILLISECONDS.sleep(10);
            } else {
                System.out.println("get message after loop=" + loop);
                break;
            }

            if (loop == 20) {
                System.out.println("can not get message after loop=" + loop);
                break;
            }
        }
    }

    private static void seekWithRebalanceListener(KafkaConsumer<String, String> consumer, CheckAssign checkAssign, String topic, int partition) throws InterruptedException {
        if (!consumer.assignment().isEmpty()) {
            consumer.unsubscribe();
        }
        while (checkAssign.isAssigned()) {
            System.out.println("wait partitions revoked...");
            TimeUnit.MILLISECONDS.sleep(100);
        }


        consumer.subscribe(Collections.singleton(topic), checkAssign);
        System.out.println("assignment-1:" + consumer.assignment());
        while (!checkAssign.isAssigned()) {
            System.out.println("wait partitions assigned...");
            consumer.poll(Duration.ofMillis(100));
            TimeUnit.MILLISECONDS.sleep(100);
        }
        System.out.println("assignment-2:" + consumer.assignment());
        consumer.seek(new TopicPartition(topic, partition), 4);
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
        System.out.println("count=" + records.count());
    }

}
