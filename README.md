### 1. 本文目的

演示kafka的使用，消息的发送与消费。

### 2. 环境准备

从Apache Kafka官网[下载](http://kafka.apache.org/downloads)软件包，按照官方文档[Quick Start]()启动zookeeper（已含在软件包内）和kafka server。

本文使用的kafka版本为`kafka_2.12-2.5.0`，其中2.12代表Scala的版本，2.5.0是kafka的版本。

### 3. 业务场景

演示的场景为，订单服务发送支付请求到支付服务，支付服务处理后返回消息到订单服务。

PayService 订阅 payRequest 主题，OrderService 订阅 payResponse 主题。两个服务都既是producer，又是consumer。

### 4. kafka编程

在java中使用kafka的两种方式：

#### 4.1 使用apache的kafka-clients
```groovy
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
compile group: 'org.apache.kafka', name: 'kafka-clients', version: '2.5.0'
```
[官方示例 - 生产者](http://kafka.apache.org/25/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html)

[官方示例 - 消费者](http://kafka.apache.org/25/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html)

#### 4.2 使用spring封装的spring-kafka

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

