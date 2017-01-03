/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.rabbitmq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test of {@link RabbitMqIO}. */
@RunWith(JUnit4.class)
public class RabbitMqIOTest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(RabbitMqIOTest.class);

  private static int port;
  @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule public transient TestPipeline p = TestPipeline.create();

  private static transient Broker broker;

  @BeforeClass
  public static void startBroker() throws Exception {
    try (ServerSocket serverSocket = new ServerSocket(0)) {
      port = serverSocket.getLocalPort();
    }

    System.setProperty("derby.stream.error.field", "MyApp.DEV_NULL");
    broker = new Broker();
    BrokerOptions options = new BrokerOptions();
    options.setConfigProperty(BrokerOptions.QPID_AMQP_PORT, String.valueOf(port));
    options.setConfigProperty(BrokerOptions.QPID_WORK_DIR, temporaryFolder.newFolder().toString());
    options.setConfigProperty(BrokerOptions.QPID_HOME_DIR, "src/test/qpid");
    broker.startup(options);
  }

  @AfterClass
  public static void stopBroker() {
    broker.shutdown();
  }

  @Test
  public void testReadQueue() throws Exception {
    final int maxNumRecords = 10;
    PCollection<RabbitMqMessage> raw =
        p.apply(
            RabbitMqIO.read()
                .withUri("amqp://guest:guest@localhost:" + port)
                .withQueue("READ")
                .withMaxNumRecords(maxNumRecords));
    PCollection<byte[]> output = raw.apply(ParDo.of(new ConverterFn()));

    List<byte[]> records = generateRecords(maxNumRecords);
    PAssert.that(output).containsInAnyOrder(records);

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare("READ", false, false, false, null);
    for (byte[] record : records) {
      channel.basicPublish("", "READ", null, record);
    }

    p.run();

    channel.close();
    connection.close();
  }

  @Test(timeout = 60 * 1000)
  public void testReadExchange() throws Exception {
    final int maxNumRecords = 10;
    PCollection<RabbitMqMessage> raw =
        p.apply(
            RabbitMqIO.read()
                .withUri("amqp://guest:guest@localhost:" + port)
                .withExchange("READEXCHANGE", "fanout", "test")
                .withMaxNumRecords(maxNumRecords));
    PCollection<byte[]> output = raw.apply(ParDo.of(new ConverterFn()));

    List<byte[]> records = generateRecords(maxNumRecords);
    PAssert.that(output).containsInAnyOrder(records);

    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = connectionFactory.newConnection();
    final Channel channel = connection.createChannel();
    channel.exchangeDeclare("READEXCHANGE", "fanout");
    Thread publisher =
        new Thread(
            () -> {
              try {
                Thread.sleep(5000);
              } catch (Exception e) {
                LOG.error(e.getMessage(), e);
              }
              for (int i = 0; i < 10; i++) {
                try {
                  channel.basicPublish(
                      "READEXCHANGE", "test", null, ("Test " + i).getBytes(StandardCharsets.UTF_8));
                } catch (Exception e) {
                  LOG.error(e.getMessage(), e);
                }
              }
            });
    publisher.start();
    p.run();
    publisher.join();

    channel.close();
    connection.close();
  }

  @Test
  public void testWriteQueue() throws Exception {
    final int maxNumRecords = 1000;
    List<RabbitMqMessage> data =
        IntStream.range(0, maxNumRecords)
            .mapToObj(i -> new RabbitMqMessage(("Test " + i).getBytes(StandardCharsets.UTF_8)))
            .collect(Collectors.toList());
    p.apply(Create.of(data))
        .apply(
            RabbitMqIO.write().withUri("amqp://guest:guest@localhost:" + port).withQueue("TEST"));

    final List<String> received = new ArrayList<>();
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    channel.queueDeclare("TEST", true, false, false, null);
    Consumer consumer =
        new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(
              String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
              throws IOException {
            String message = new String(body, "UTF-8");
            received.add(message);
          }
        };
    channel.basicConsume("TEST", true, consumer);

    p.run();

    while (received.size() < maxNumRecords) {
      Thread.sleep(500);
    }

    assertEquals(maxNumRecords, received.size());
    for (int i = 0; i < maxNumRecords; i++) {
      assertTrue(received.contains("Test " + i));
    }

    channel.close();
    connection.close();
  }

  @Test
  public void testWriteExchange() throws Exception {
    final int maxNumRecords = 1000;
    List<RabbitMqMessage> data =
        IntStream.range(0, maxNumRecords)
            .mapToObj(i -> new RabbitMqMessage(("Test " + i).getBytes(StandardCharsets.UTF_8)))
            .collect(Collectors.toList());
    p.apply(Create.of(data))
        .apply(
            RabbitMqIO.write()
                .withUri("amqp://guest:guest@localhost:" + port)
                .withExchange("WRITE", "fanout"));

    final List<String> received = new ArrayList<>();
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setUri("amqp://guest:guest@localhost:" + port);
    Connection connection = connectionFactory.newConnection();
    Channel channel = connection.createChannel();
    channel.exchangeDeclare("WRITE", "fanout");
    String queueName = channel.queueDeclare().getQueue();
    channel.queueBind(queueName, "WRITE", "");
    Consumer consumer =
        new DefaultConsumer(channel) {
          @Override
          public void handleDelivery(
              String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
              throws IOException {
            String message = new String(body, "UTF-8");
            received.add(message);
          }
        };
    channel.basicConsume(queueName, true, consumer);

    p.run();

    while (received.size() < maxNumRecords) {
      Thread.sleep(500);
    }

    assertEquals(maxNumRecords, received.size());
    for (int i = 0; i < maxNumRecords; i++) {
      assertTrue(received.contains("Test " + i));
    }

    channel.close();
    connection.close();
  }

  private static class ConverterFn extends DoFn<RabbitMqMessage, byte[]> {
    ConverterFn() {}

    @ProcessElement
    public void processElement(ProcessContext c) {
      RabbitMqMessage message = c.element();
      c.output(message.getBody());
    }
  }

  private static List<byte[]> generateRecords(int maxNumRecords) {
    return IntStream.range(0, maxNumRecords)
        .mapToObj(i -> ("Test " + i).getBytes(StandardCharsets.UTF_8))
        .collect(Collectors.toList());
  }
}
