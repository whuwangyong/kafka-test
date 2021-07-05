## 1. 本文目的

演示kafka的使用，消息的发送与消费。

## 2. 环境准备

从Apache Kafka官网[下载](http://kafka.apache.org/downloads)软件包，按照官方文档[Quick Start]()启动zookeeper（已含在软件包内）和kafka server。

本文使用的kafka版本为`kafka_2.12-2.5.0`，其中2.12代表Scala的版本，2.5.0是kafka的版本。

## 3. 业务场景

演示的场景为，订单服务发送支付请求到支付服务，支付服务处理后返回消息到订单服务。

PayService 订阅 payRequest 主题，OrderService 订阅 payResponse 主题。两个服务都既是producer，又是consumer。

## 4. kafka编程

在java中使用kafka的两种方式：

### 4.1 使用apache的kafka-clients
```groovy
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
compile group: 'org.apache.kafka', name: 'kafka-clients', version: '2.5.0'
```
[官方示例 - 生产者](http://kafka.apache.org/25/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html)

[官方示例 - 消费者](http://kafka.apache.org/25/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html)



#### consumer在poll后，不能立即获取到分配给它的分区

> seek（） 方法中的参数 partit 工 on 表示分区，而 offset 参数用来指定从分区的哪个位置
> 开始消费。 seek（）方法只能重置消费者分配到的分区的消费位置，而分区的分配是在 po ll （） 方
> 法的调用过程中实现的 。 也就是说，在执行 seek （）方法之前需要先执行一次 p oll （） 方法 ， 等到
> 分配到分区之后才可以重置消费位置 。
>
> —— 《深入理解Kafka：核心技术与实践原理》 朱忠华

解决办法，就是在一个循环中多次poll，直到分配到分区。

见`SeekTest.java`:

```java
KafkaConsumer<String, String> consumer = kafkaHelper.genConsumer("seek-test_g_1", "c_1");
consumer.subscribe(Collections.singletonList(TOPIC));
System.out.println("topicPartitions" + kafkaHelper.topicPartitions(TOPIC));
System.out.println("assignment:" + consumer.assignment());

int i = 1;
while (consumer.assignment().isEmpty()) {
    System.out.println("poll " + i + " times, count=" + consumer.poll(Duration.ofMillis(100)).count());
    System.out.println("assignment:" + consumer.assignment());
    i++;
}
```

这段代码的日志：

```
send 10 msg
topicPartitions[seek-test-topic-0]
assignment:[]
poll 1 times, count=0
assignment:[]
poll 2 times, count=0
assignment:[]
# 省略
assignment:[]
poll 11 times, count=0
assignment:[]
poll 12 times, count=0
assignment:[seek-test-topic-0]

```

poll了12次之后才拿到分区。

`org.apache.kafka.clients.consumer.KafkaConsumer#assignment`上面的注释也说了：

> Get the set of partitions currently assigned to this consumer. If subscription happened by directly assigning partitions using assign(Collection) then this will simply return the same partitions that were assigned. If topic subscription was used, then this will give the set of topic partitions currently assigned to the consumer (which may be none if the assignment hasn't happened yet, or the partitions are in the process of getting reassigned).

因此，为了立即获得分区，应使用`assign`方法。示例代码：

```java
// consumer.subscribe(Collections.singletonList(TOPIC));
consumer.assign(kafkaHelper.topicPartitions(TOPIC));
System.out.println("assignment:" + consumer.assignment());
consumer.seekToBeginning(kafkaHelper.topicPartitions(TOPIC));
```



注意，`assign`方法与 `subscribe`方法是互斥的。一个consumer如果使用了`subscribe`，表示使用动态分区算法，此时不能再使用`assign`方法了。对于已经使用了`subscribe`方法的consumer，可以先调用`unsubscribe`，然后再使用`assign`。









### 4.2 使用spring封装的spring-kafka

```groovy
// https://mvnrepository.com/artifact/org.springframework.kafka/spring-kafka
compile group: 'org.springframework.kafka', name: 'spring-kafka', version: '2.5.3.RELEASE'
```

spring-kafka 与 springboot之间的版本兼容关系：

Spring for Apache Kafka is based on the pure java `kafka-clients` jar. The following is the compatibility matrix:

| Spring for Apache Kafka Version | Spring Integration for Apache Kafka Version | `kafka-clients`     | Spring Boot                  |
| ------------------------------- | ------------------------------------------- | ------------------- | ---------------------------- |
| 2.6.0-SNAPSHOT (pre-release)    | 5.4.0-SNAPSHOT (pre-release)                | 2.6.0               | 2.4.0-SNAPSHOT (pre-release) |
| **2.5.x**                       | 3.3.x                                       | **2.5.0**           | **2.3.x**                    |
| 2.4.x                           | 3.2.x                                       | 2.4.1               | 2.2.x                        |
| 2.3.x                           | 3.2.x                                       | 2.3.1               | 2.2.x                        |
| 2.2.x                           | 3.1.x                                       | 2.0.1, 2.1.x, 2.2.x | 2.1.x                        |
| ~~2.1.x~~                       | ~~3.0.x~~                                   | ~~1.0.2~~           | ~~2.0.x (End of Life)~~      |
| 1.3.x                           | 2.3.x                                       | 0.11.0.x, 1.0.x     | ~~1.5.x (End of Life)~~      |

更多信息查看官方文档：[Spring for Apache Kafka](https://spring.io/projects/spring-kafka)

本文使用的kafka是2.5.0版本，kafka-clients也是2.5.0版本，因此，对应的springboot使用2.3.x版本，spring-kafka使用2.5.x版本。上表中的第二列可以不管。

