package org.doogle.embeddedkafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092",
    "port=9092"})
class EmbeddedKafkaApplicationTests {

  @Autowired
  private KafkaConsumer consumer;

  @Autowired
  private KafkaProducer producer;

  @Value("${test.topic}")
  private String topic;

  @Test
  void contextLoads() {
  }

  @Test
  public void givenEmbeddedKafkaBroker_whenSendingWithSimpleProducer_thenMessageReceived()
      throws Exception {
    String data = "Sending with our own simple KafkaProducer";

    producer.send(topic, data);

    boolean messageConsumed = consumer.getLatch().await(10, TimeUnit.SECONDS);
    assertTrue(messageConsumed);
    assertThat(consumer.getPayload(), containsString(data));
  }

}
