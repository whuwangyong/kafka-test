package cn.whu.wy.kafka.test.clients.seek;

import cn.whu.wy.kafka.test.clients.KafkaHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
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

    // private static final long POLL_TIMEOUT = 1000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);
    static KafkaHelper<String, String> kafkaHelper = new KafkaHelper<>("127.0.0.1:9092");

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (!kafkaHelper.listTopics().contains(TOPIC_1)) {
            kafkaHelper.createTopic(TOPIC_1, 2);
        }
        if (!kafkaHelper.listTopics().contains(TOPIC_2)) {
            kafkaHelper.createTopic(TOPIC_2, 2);
        }

        KafkaConsumer<String, String> consumer = kafkaHelper.genConsumer("c_1", false);


//        seekTest(consumer);
        fastSeekAndPollBenchmark(consumer);


        System.out.println("done");
        consumer.close();
    }

    static void seekTest(KafkaConsumer<String, String> consumer) throws ExecutionException, InterruptedException {
        //send some data
//        send(TOPIC_1, 10);
//        send(TOPIC_2, 10);

        fastSeekAndPoll(consumer, TOPIC_1, 0, 0);
        fastSeekAndPoll(consumer, TOPIC_1, 1, 1);
        fastSeekAndPoll(consumer, TOPIC_2, 0, 3);
        fastSeekAndPoll(consumer, TOPIC_2, 1, 5);

        seekAndPollInLoop(consumer, TOPIC_1, 0);
        seekAndPollInLoop(consumer, TOPIC_1, 1);
        seekAndPollInLoop(consumer, TOPIC_2, 0);
        seekAndPollInLoop(consumer, TOPIC_2, 1);

//        CheckAssign checkAssign = new CheckAssign();
//        seekWithRebalanceListener(consumer, checkAssign, TOPIC_1, 0);
//        seekWithRebalanceListener(consumer, checkAssign, TOPIC_1, 1);
//        seekWithRebalanceListener(consumer, checkAssign, TOPIC_2, 0);
//        seekWithRebalanceListener(consumer, checkAssign, TOPIC_2, 1);
    }

    private static void send(String topic, int num) throws InterruptedException, ExecutionException {
        KafkaProducer<String, String> producer = kafkaHelper.genProducer();
        for (int i = 0; i < num; i++) {
            producer.send(new ProducerRecord<>(topic, "message" + i)).get();
        }
        System.out.printf("sent %d msg%n", num);
        producer.close();
    }

    /**
     * @param offset 从该offset开始消费
     */
    private static void fastSeekAndPoll(KafkaConsumer<String, String> consumer, String topic, int partition, int offset) {
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singleton(tp));
        System.out.println("assignment:" + consumer.assignment());
//        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
//        map.put(tp, new OffsetAndMetadata(2));
//        consumer.commitSync(map, Duration.ofSeconds(10));
//        consumer.enforceRebalance();

        // endOffset: the offset of the last successfully replicated message plus one
        // if there has 5 messages, valid offsets are [0,1,2,3,4], endOffset is 4+1=5
        Long endOffset = consumer.endOffsets(Collections.singleton(tp)).get(tp);
        if (offset < 0 || offset >= endOffset) {
            System.out.println("offset is illegal");
        } else {
            consumer.seek(tp, offset);
            System.out.println("count=" + consumer.poll(POLL_TIMEOUT).count());
        }
    }

    private static void seekAndPollInLoop(KafkaConsumer<String, String> consumer, String topic, int partition) throws InterruptedException {
        TopicPartition tp = new TopicPartition(topic, partition);
        consumer.assign(Collections.singleton(tp));
        System.out.println("assignment:" + consumer.assignment());
        consumer.seek(tp, 3);

        // this line is important
        Long endOffset = consumer.endOffsets(Collections.singleton(tp)).get(tp);

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

    /*
    POLL_TIMEOUT=2000时：
    invalid:103
    polled:897
    notPolled:0
    total=1000
    证明有效
     */
    private static void fastSeekAndPollBenchmark(KafkaConsumer<String, String> consumer) throws ExecutionException, InterruptedException {
        int num = 1000;
//        send(TOPIC_1, num);
//        send(TOPIC_2, num);

        ArrayList<Integer> offsets1 = new ArrayList<>();
        ArrayList<Long> offsets2 = new ArrayList<>();

        TopicPartition[] tps = {
                new TopicPartition(TOPIC_1, 0),
                new TopicPartition(TOPIC_1, 1),
                new TopicPartition(TOPIC_2, 0),
                new TopicPartition(TOPIC_2, 1)
        };

        // 循环测试1000次
        Random random = new Random();
        int invalid = 0;
        int polled = 0;
        int notPolled = 0;
        for (int i = 0; i < 100; i++) {
            TopicPartition tp = tps[random.nextInt(4)];
            // 每个分区有10000个消息，这里故意多给出1000的范围，测试超过endOffset的情况
            int offset = random.nextInt((int) (num / 2 * 1.1));
            consumer.assign(Collections.singleton(tp));
            Long endOffset = consumer.endOffsets(Collections.singleton(tp)).get(tp); // 这一行妙不可言啊，能起到更新metadata的作用
            if (offset >= endOffset) {
                invalid++;
            } else {
                offsets1.add(offset);
                consumer.seek(tp, offset);
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    notPolled++;
                } else {
                    ConsumerRecord<String, String> record = records.iterator().next();
                    offsets2.add(record.offset());
                    if (record.offset() != offset) {
                        notPolled++;
                    } else {
                        polled++;
                    }
                }
            }
        }

        System.out.println("invalid:" + invalid);
        System.out.println("polled:" + polled);
        System.out.println("notPolled:" + notPolled);
        System.out.printf("total=%d%n", invalid + polled + notPolled);

        System.out.println(offsets1);
        System.out.println(offsets2);
    }

}
