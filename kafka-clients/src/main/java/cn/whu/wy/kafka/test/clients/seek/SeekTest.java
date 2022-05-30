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
 * @author WangYong
 * Date 2021/07/03
 * Time 21:31
 */
public class SeekTest {
    private static final String TOPIC_1 = "seek-test-topic-1";
    private static final String TOPIC_2 = "seek-test-topic-2";
    static KafkaHelper<String, String> kafkaHelper = new KafkaHelper<>();

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


//        fastSeekAndPoll(consumer, TOPIC_1, 0);
//        fastSeekAndPoll(consumer, TOPIC_1, 1);
//        fastSeekAndPoll(consumer, TOPIC_2, 0);
//        fastSeekAndPoll(consumer, TOPIC_2, 1);
//
//        seekAndPollInLoop(consumer, TOPIC_1, 0);
//        seekAndPollInLoop(consumer, TOPIC_1, 1);
//        seekAndPollInLoop(consumer, TOPIC_2, 0);
//        seekAndPollInLoop(consumer, TOPIC_2, 1);

        CheckAssign checkAssign = new CheckAssign();
        seekWithRebalanceListener(consumer, checkAssign, TOPIC_1,0);
        seekWithRebalanceListener(consumer, checkAssign, TOPIC_1,1);
        seekWithRebalanceListener(consumer, checkAssign, TOPIC_2,0);
        seekWithRebalanceListener(consumer, checkAssign, TOPIC_2,1);


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
        System.out.println("count=" + consumer.poll(Duration.ofMillis(100)).count());
    }

    private static void seekAndPollInLoop(KafkaConsumer<String, String> consumer, String topic, int partition) throws InterruptedException {
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singleton(tp));
        System.out.println("assignment:" + consumer.assignment());
        consumer.seek(tp, 3);

        int loop = 1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                loop++;
                TimeUnit.MILLISECONDS.sleep(10);
            } else {
                System.out.println("get message after loop=" + loop);
                break;
            }

            if (loop == 50) {
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
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        System.out.println("count=" + records.count());
    }

}



/*
the result of use poll(Duration) or use poll(long) is consistent:

[main] INFO org.apache.kafka.common.utils.AppInfoParser - Kafka startTimeMs: 1653893944366
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Subscribed to partition(s): seek-test-topic-1-0
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Seeking to offset 2 for partition seek-test-topic-1-0
[main] INFO org.apache.kafka.clients.Metadata - [Consumer clientId=c_1, groupId=seek-test_g_1] Cluster ID: exmI-HpwReGMg5anZlbjvw
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Subscribed to partition(s): seek-test-topic-1-1
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Seeking to offset 2 for partition seek-test-topic-1-1
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Subscribed to partition(s): seek-test-topic-2-0
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Seeking to offset 2 for partition seek-test-topic-2-0
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Subscribed to partition(s): seek-test-topic-2-1
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Seeking to offset 2 for partition seek-test-topic-2-1
assignment:[seek-test-topic-1-0]
count=3
assignment:[seek-test-topic-1-1]
count=0
assignment:[seek-test-topic-2-0]
count=0
assignment:[seek-test-topic-2-1]
count=0


assignment:[seek-test-topic-1-0]
get message after loop=2
assignment:[seek-test-topic-1-1]
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Subscribed to partition(s): seek-test-topic-1-0
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Seeking to offset 3 for partition seek-test-topic-1-0
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Subscribed to partition(s): seek-test-topic-1-1
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Seeking to offset 3 for partition seek-test-topic-1-1
[main] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - [Consumer clientId=c_1, groupId=seek-test_g_1] Discovered group coordinator ThinkCentre:9092 (id: 2147483647 rack: null)
get message after loop=5
assignment:[seek-test-topic-2-0]
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Subscribed to partition(s): seek-test-topic-2-0
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Seeking to offset 3 for partition seek-test-topic-2-0
get message after loop=5
assignment:[seek-test-topic-2-1]
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Subscribed to partition(s): seek-test-topic-2-1
[main] INFO org.apache.kafka.clients.consumer.KafkaConsumer - [Consumer clientId=c_1, groupId=seek-test_g_1] Seeking to offset 3 for partition seek-test-topic-2-1
get message after loop=5
done

 */