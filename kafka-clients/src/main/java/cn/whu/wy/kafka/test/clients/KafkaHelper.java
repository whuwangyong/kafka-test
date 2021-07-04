package cn.whu.wy.kafka.test.clients;

import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author WangYong
 * @Date 2021/07/03
 * @Time 20:54
 */
public class KafkaHelper<K, V> {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private final KafkaAdminClient adminClient;

    public KafkaAdminClient getAdminClient() {
        return adminClient;
    }

    public KafkaHelper() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        adminClient = (KafkaAdminClient) AdminClient.create(props);
    }

    public KafkaProducer<K, V> genProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public KafkaConsumer<K, V> genConsumer(String groupId, String clientId) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", groupId);
        props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    @SneakyThrows
    public Set<String> listTopics() {
        return adminClient.listTopics().names().get(2000, TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    public TopicDescription describe(String topic) {
        TopicDescription description = adminClient.describeTopics(Collections.singletonList(topic)).all()
                .get(2000, TimeUnit.MILLISECONDS).get(topic);
        return description;
    }


    public List<Integer> partitions(String topic) {
        List<TopicPartitionInfo> partitionInfos = describe(topic).partitions();
        List<Integer> result = new ArrayList<>();
        for (TopicPartitionInfo partitionInfo : partitionInfos) {
            result.add(partitionInfo.partition());
        }
        return result;
    }

    public List<TopicPartition> topicPartitions(String topic) {
        List<TopicPartitionInfo> partitionInfos = describe(topic).partitions();
        List<TopicPartition> result = new ArrayList<>();
        for (TopicPartitionInfo partitionInfo : partitionInfos) {
            result.add(new TopicPartition(topic, partitionInfo.partition()));
        }
        return result;
    }


}
