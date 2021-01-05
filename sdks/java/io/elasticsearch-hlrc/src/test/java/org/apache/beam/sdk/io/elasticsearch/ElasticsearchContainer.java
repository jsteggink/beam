package org.apache.beam.sdk.io.elasticsearch;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import com.github.dockerjava.api.model.Ulimit;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

/**
 * Elasticsearch container for cluster.
 */
public class ElasticsearchContainer extends
    GenericContainer<ElasticsearchContainer> {

  /** Elasticsearch Default HTTP port. */
  public static final int ELASTICSEARCH_DEFAULT_PORT = 9200;

  /** Elasticsearch Default Transport port. */
  public static final int ELASTICSEARCH_DEFAULT_TCP_PORT = 9300;

  /** Elasticsearch Docker base URL. */
  private static final String ELASTICSEARCH_DEFAULT_IMAGE = "docker.elastic"
      + ".co/elasticsearch/elasticsearch-oss";

  /** Elasticsearch Default version. */
  protected static final String ELASTICSEARCH_DEFAULT_VERSION = "6.5.2";

  public ElasticsearchContainer() {
    this(ELASTICSEARCH_DEFAULT_IMAGE + ":" + ELASTICSEARCH_DEFAULT_VERSION);
  }

  /**
   * Create an Elasticsearch Container by passing the full docker image name.
   * @param dockerImageName Full docker image name, like: docker.elastic
   *                        .co/elasticsearch/elasticsearch-oss:6.5.1
   */
  public ElasticsearchContainer(String dockerImageName) {
    super(dockerImageName);
    logger().info("Starting an elasticsearch container using [{}]", dockerImageName);
    withCreateContainerCmdModifier(it -> it.withUlimits(new Ulimit("memlock", -1, -1)));
    withEnv("cluster.name", "docker-cluster");
    withEnv("bootstrap.memory_lock", "true");
    withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
    addExposedPorts(ELASTICSEARCH_DEFAULT_PORT, ELASTICSEARCH_DEFAULT_TCP_PORT);
    setWaitStrategy(new HttpWaitStrategy()
        .forPort(ELASTICSEARCH_DEFAULT_PORT)
        .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
        .withStartupTimeout(Duration.ofMinutes(2)));
  }
}
