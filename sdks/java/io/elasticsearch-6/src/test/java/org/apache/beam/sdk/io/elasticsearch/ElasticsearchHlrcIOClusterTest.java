package org.apache.beam.sdk.io.elasticsearch;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchHlrcIOTestCommon.ES_INDEX;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchHlrcIOTestCommon.ES_TYPE;

import java.io.File;
import java.time.Duration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

/** ES cluster tests for {@link ElasticsearchHlrcIO}. */
@RunWith(JUnit4.class)
public class ElasticsearchHlrcIOClusterTest {

  private static final int ELASTICSEARCH_PORT = 9200;

  @Rule
  public DockerComposeContainer environment =
      new DockerComposeContainer(new File("src/test/resources/docker-compose.yml"))
          .withExposedService("beam-elasticsearch1", ELASTICSEARCH_PORT,
              new HttpWaitStrategy()
                  .forPort(ELASTICSEARCH_PORT)
                  .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                  .withStartupTimeout(Duration.ofMinutes(2)))
          .withExposedService("beam-elasticsearch2", ELASTICSEARCH_PORT,
              new HttpWaitStrategy()
                  .forPort(ELASTICSEARCH_PORT)
                  .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                  .withStartupTimeout(Duration.ofMinutes(2))
          )
          .withTailChildContainers(true);

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private ElasticsearchHlrcIOTestCommon elasticsearchHLRCIOTestCommon;
  private ElasticsearchHlrcIO.ConnectionConfiguration connectionConfiguration;

  HttpHost[] esHosts;

  @Before
  public void setup() throws Exception {
    esHosts = new HttpHost[]{
        new HttpHost(
            environment.getServiceHost("beam-elasticsearch1", ELASTICSEARCH_PORT),
            environment.getServicePort("beam-elasticsearch1", ELASTICSEARCH_PORT)),
        new HttpHost(
            environment.getServiceHost("beam-elasticsearch2", ELASTICSEARCH_PORT),
            environment.getServicePort("beam-elasticsearch2", ELASTICSEARCH_PORT))
    };
    connectionConfiguration = ElasticsearchHlrcIO.ConnectionConfiguration
        .create(ES_INDEX, esHosts);
    elasticsearchHLRCIOTestCommon =
        new ElasticsearchHlrcIOTestCommon(
            connectionConfiguration,
            new RestHighLevelClient(RestClient.builder(esHosts)),
            false);
    CreateIndexResponse response = elasticsearchHLRCIOTestCommon.createIndex();
    if(response == null || !response.isAcknowledged()) {
      throw new Exception("Couldn't create index.");
    }
    elasticsearchHLRCIOTestCommon.setPipeline(pipeline);
  }

  @Test
  public void testRead() throws Exception {
    elasticsearchHLRCIOTestCommon.testRead();
  }

  @Test
  public void testReadWithQuery() throws Exception {
    elasticsearchHLRCIOTestCommon.testReadWithQuery();
  }

  @Test
  public void testWrite() throws Exception {
    elasticsearchHLRCIOTestCommon.testWrite();
  }

  @Rule public ExpectedException expectedException = ExpectedException.none();
  @Test
  public void testWriteWithErrors() throws Exception {
    elasticsearchHLRCIOTestCommon.setExpectedException(expectedException);
    elasticsearchHLRCIOTestCommon.testWriteWithErrors();
  }

}
