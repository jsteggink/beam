package org.apache.beam.sdk.io.elasticsearch;

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchHlrcIOTestCommon.ES_INDEX;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.Network;

/** Tests for {@link ElasticsearchHlrcIO}. */
@RunWith(JUnit4.class)
public class ElasticsearchHlrcIOTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private Network network;

  private ElasticsearchHlrcIOTestCommon elasticsearchHLRCIOTestCommon;
  private ElasticsearchHlrcIO.ConnectionConfiguration connectionConfiguration;

  private HttpHost[] esHosts;

  @Before
  public void setup() throws Exception {
    try {
      if(network == null) {
        network = Network.builder().build();
      }
    } catch (Exception e) {
    }

    ElasticsearchContainer esMaster =
        new ElasticsearchContainer()
            .withNetwork(network)
            .withNetworkAliases("esmaster");
    esMaster.start();

    ElasticsearchContainer esData =
        new ElasticsearchContainer()
            .withNetwork(network)
            .withEnv("discovery.zen.ping.unicast.hosts", "esmaster")
            .withEnv("node.master","false");
    esData.start();

    esHosts = new HttpHost[]{
        new HttpHost(esMaster.getContainerIpAddress(),
            esMaster.getMappedPort(ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PORT)),
        new HttpHost(esData.getContainerIpAddress(),
            esData.getMappedPort(ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PORT))
    };
    connectionConfiguration = ElasticsearchHlrcIO.ConnectionConfiguration
        .create(ES_INDEX, esHosts)
        .withMaxRetryTimeoutMillis(180000);

    elasticsearchHLRCIOTestCommon =
        new ElasticsearchHlrcIOTestCommon(
            connectionConfiguration,
            new RestHighLevelClient(
                RestClient.builder(
                    esHosts).setMaxRetryTimeoutMillis(180000)),
            false);
    elasticsearchHLRCIOTestCommon.createIndex();
  }

  @Test
  public void testRead() throws Exception {
    elasticsearchHLRCIOTestCommon.setPipeline(pipeline);
    elasticsearchHLRCIOTestCommon.testRead();
  }

  @Test
  public void testReadWithQuery() throws Exception {
    elasticsearchHLRCIOTestCommon.setPipeline(pipeline);
    elasticsearchHLRCIOTestCommon.testReadWithQuery();
  }

  @Test
  public void testWrite() throws Exception {
    elasticsearchHLRCIOTestCommon.setPipeline(pipeline);
    elasticsearchHLRCIOTestCommon.testWrite();
  }

  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Test
  public void testWriteWithErrors() throws Exception {
    expectedException.expect(IllegalArgumentException.class);
    elasticsearchHLRCIOTestCommon.testWriteWithErrors();
  }

  @After
  public void clean() {
    try {
      network.close();
    } catch (Exception e) {
    }
    finally {
      network = null;
    }

  }
}
