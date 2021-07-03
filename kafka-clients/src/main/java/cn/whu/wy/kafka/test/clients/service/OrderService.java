package cn.whu.wy.kafka.test.clients.service;

import cn.whu.wy.kafka.test.clients.bean.PayRequest;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
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
public class OrderService {
    @Autowired
    private Properties producerProperties;
    @Autowired
    private Gson gson;

    private static final String PAY_REQ_TOPIC = "pay-request";
    private static final String PAY_RESP_TOPIC = "pay-response";

    public void sendReqToPay() {
        log.info("发送支付请求...");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        for (int i = 0; i < 10; i++) {
            PayRequest request = PayRequest.builder()
                    .orderId(System.currentTimeMillis() + "")
                    .userId("wang" + i)
                    .amount(BigDecimal.valueOf(4999.00))
                    .dateTime(LocalDateTime.now())
                    .build();
            producer.send(new ProducerRecord<>(PAY_REQ_TOPIC, gson.toJson(request)));
        }
        log.info("send done.");
        producer.close();
    }

    public void receive(){
        log.info("等待支付结果...");

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "order");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(PAY_RESP_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }

}
