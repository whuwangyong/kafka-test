package cn.whu.wy.kafka.test.clients.service;

import cn.whu.wy.kafka.test.clients.KafkaHelper;
import cn.whu.wy.kafka.test.clients.Util;
import cn.whu.wy.kafka.test.clients.bean.PayRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;

/**
 * @author WangYong
 * Date 2020/08/27
 * Time 11:03
 */

@Slf4j
public class OrderService {

    KafkaHelper<String, String> kafkaHelper = new KafkaHelper<>("127.0.0.1:9092");

    public void sendReqToPay() {
        log.info("发送支付请求...");
        KafkaProducer<String, String> producer = kafkaHelper.genProducer();
        for (int i = 0; i < 3; i++) {
            PayRequest request = PayRequest.builder()
                    .orderId(System.currentTimeMillis() + "")
                    .userId("wang" + i)
                    .amount(BigDecimal.valueOf(4999.00))
                    .dateTime(LocalDateTime.now())
                    .build();
            producer.send(new ProducerRecord<>(Util.PAY_REQ_TOPIC, Util.gson().toJson(request)));
        }
        log.info("send done.");
        producer.close();
    }

    public void receive() {
        log.info("等待支付结果...");

        KafkaConsumer<String, String> consumer = kafkaHelper.genConsumer("g_order","c_1", true);
        consumer.subscribe(Collections.singletonList(Util.PAY_RESP_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }

}
