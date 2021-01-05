package org.apache.beam.sdk.io.elasticsearch;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static junit.framework.TestCase.assertEquals;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchHlrcIOTestCommon.ES_INDEX;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;

import java.io.File;
import java.time.Duration;
import java.util.List;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

/** ES cluster tests for {@link ElasticsearchHlrcIO}. */
@RunWith(JUnit4.class)
public class ElasticsearchHlrcIOIT {

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

  private ElasticsearchHlrcIOTestCommon elasticsearchHlrcIOTestCommon;

  @Before
  public void setup() throws Exception {
    HttpHost[] esHosts = new HttpHost[] {
        new HttpHost(environment.getServiceHost("beam-elasticsearch1", ELASTICSEARCH_PORT),
            environment.getServicePort("beam-elasticsearch1", ELASTICSEARCH_PORT)),
        new HttpHost(environment.getServiceHost("beam-elasticsearch2", ELASTICSEARCH_PORT),
            environment.getServicePort("beam-elasticsearch2", ELASTICSEARCH_PORT)) };
    ElasticsearchHlrcIO.ConnectionConfiguration connectionConfiguration =
        ElasticsearchHlrcIO.ConnectionConfiguration
        .create(ES_INDEX, esHosts)
        .withMaxRetryTimeoutMillis(180000);
    elasticsearchHlrcIOTestCommon =
        new ElasticsearchHlrcIOTestCommon(connectionConfiguration,
            new RestHighLevelClient(
                RestClient.builder(esHosts).setMaxRetryTimeoutMillis(180000)
            ),
            false);

    CreateIndexResponse response = elasticsearchHlrcIOTestCommon.createIndex();
    if(response == null || !response.isAcknowledged()) {
      throw new Exception("Couldn't create index.");
    }
    elasticsearchHlrcIOTestCommon.setPipeline(pipeline);
  }

  @Test
  public void testRead() throws Exception {
    elasticsearchHlrcIOTestCommon.testRead();
  }

  @Test
  public void testReadWithQuery() throws Exception {
    elasticsearchHlrcIOTestCommon.testReadWithQuery();
  }

  @Test
  public void testWrite() throws Exception {
    elasticsearchHlrcIOTestCommon.testWrite();
  }

  @Test
  public void testDelete() throws Exception {
    elasticsearchHlrcIOTestCommon.testDelete();
  }

  @Test
  public void testUpdate() throws Exception {
    elasticsearchHlrcIOTestCommon.testUpdate();
  }

  @Test
  public void testSplitsVolume() throws Exception {
    elasticsearchHlrcIOTestCommon.testSplitsVolume();
  }

}
