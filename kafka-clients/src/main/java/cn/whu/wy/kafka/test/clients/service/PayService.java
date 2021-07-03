package cn.whu.wy.kafka.test.clients.service;

import cn.whu.wy.kafka.test.clients.bean.PayRequest;
import cn.whu.wy.kafka.test.clients.bean.PayResponse;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author WangYong
 * @Date 2020/08/27
 * @Time 11:03
 */
@Service
@Slf4j
public class PayService {
    @Autowired
    private Properties producerProperties;
    @Autowired
    private Gson gson;

    private static final String PAY_REQ_TOPIC = "pay-request";
    private static final String PAY_RESP_TOPIC = "pay-response";

    /**
     * 处理支付请求
     */
    public void processPayReq() {
        log.info("等待支付请求...");

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "pay");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        consumer.subscribe(Arrays.asList(PAY_REQ_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                PayRequest request = gson.fromJson(record.value(), PayRequest.class);
                PayResponse response = PayResponse.builder()
                        .orderId(request.getOrderId())
                        .dateTime(LocalDateTime.now())
                        .result("wang8".equals(request.getUserId()) ? "failed" : "success")
                        .build();
                producer.send(new ProducerRecord<>(PAY_RESP_TOPIC, gson.toJson(response)));
            }
        }
    }
}
