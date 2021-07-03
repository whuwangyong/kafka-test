package cn.whu.wy.kafka.test.clients.service;

import cn.whu.wy.kafka.test.clients.KafkaHelper;
import cn.whu.wy.kafka.test.clients.Util;
import cn.whu.wy.kafka.test.clients.bean.PayRequest;
import cn.whu.wy.kafka.test.clients.bean.PayResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;

/**
 * @Author WangYong
 * @Date 2020/08/27
 * @Time 11:03
 */

@Slf4j
public class PayService {




    KafkaHelper<String, String> kafkaHelper = new KafkaHelper<>();

    /**
     * 处理支付请求
     */
    public void processPayReq() {
        log.info("等待支付请求...");

        KafkaConsumer<String, String> consumer = kafkaHelper.genConsumer("g_pay","c_1");

        KafkaProducer<String, String> producer = kafkaHelper.genProducer();
        consumer.subscribe(Arrays.asList(Util.PAY_REQ_TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                PayRequest request = Util.gson().fromJson(record.value(), PayRequest.class);
                PayResponse response = PayResponse.builder()
                        .orderId(request.getOrderId())
                        .dateTime(LocalDateTime.now())
                        .result("wang2".equals(request.getUserId()) ? "failed" : "success")
                        .build();
                producer.send(new ProducerRecord<>(Util.PAY_RESP_TOPIC, Util.gson().toJson(response)));
            }
        }
    }
}
