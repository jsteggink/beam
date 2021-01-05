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
package org.apache.beam.sdk.io.elasticsearch;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transforms for reading and writing data from/to Elasticsearch.
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>{@link ElasticsearchHlrcIO#read ElasticsearchHlrcIO.read()} returns a bounded {@link PCollection
 * PCollection&lt;String&gt;} representing JSON documents.
 *
 * <p>To configure the {@link ElasticsearchHlrcIO#read}, you have to provide a connection configuration
 * containing the HTTP address of the instances, an index name and a type. The following example
 * illustrates options for configuring the source:
 *
 * <pre>{@code
 * pipeline.apply(ElasticsearchHlrcIO.read().withConnectionConfiguration(
 *    ElasticsearchHlrcIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
 * )
 *
 * }</pre>
 *
 * <p>The connection configuration also accepts optional configuration: {@code withUsername()} and
 * {@code withPassword()}.
 *
 * <p>You can also specify a query on the {@code read()} using {@code withQuery()}.
 *
 * <h3>Writing to Elasticsearch</h3>
 *
 * <p>To write documents to Elasticsearch, use {@link ElasticsearchHlrcIO#write
 * ElasticsearchHlrcIO.write()}, which writes JSON documents from a {@link PCollection
 * PCollection&lt;String&gt;} (which can be bounded or unbounded).
 *
 * <p>To configure {@link ElasticsearchHlrcIO#write ElasticsearchHlrcIO.write()}, similar to the read, you
 * have to provide a connection configuration. For instance:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(ElasticsearchHlrcIO.write().withConnectionConfiguration(
 *      ElasticsearchHlrcIO.ConnectionConfiguration.create("my-index", "my-type")
 *   )
 *
 * }</pre>
 *
 * <p>Optionally, you can provide {@code withBatchSize()} and {@code withBatchSizeBytes()} to
 * specify the size of the write batch in number of documents or in bytes.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class ElasticsearchHlrcIO {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchHlrcIO.class);

  /**
   * Create a read object.
   * @param connectionConfiguration connection configuration
   * @param query query
   * @return Read object
   */
  public static Read read(ConnectionConfiguration connectionConfiguration, String query) {
    // default scrollKeepalive = 5m as a majorant for un-predictable time between 2 start/read calls
    // default batchSize to 100 as recommended by ES dev team as a safe value when dealing
    // with big documents and still a good compromise for performances
    return new AutoValue_ElasticsearchHlrcIO_Read.Builder()
        .setConnectionConfiguration(connectionConfiguration)
        .setQuery(query)
        .setScrollKeepalive("5m")
        .setBatchSize(100)
        .build();
  }

  /**
   *
   * @param connectionConfiguration connection configuration
   * @return Write
   */
  public static Write write(ConnectionConfiguration connectionConfiguration) {
    return new AutoValue_ElasticsearchHlrcIO_Write.Builder()
        .setConnectionConfiguration(connectionConfiguration)
        // advised default starting batch size in ES docs
        .setMaxBatchSize(1000)
        // advised default starting batch size bytes in ES docs
        .setMaxBatchSizeBytes(5L * 1024L * 1024L)
        .build();
  }

  private ElasticsearchHlrcIO() {}

  private static final ObjectMapper mapper = new ObjectMapper();

  @VisibleForTesting private static JsonNode parseResponse(Response response) throws IOException {
    return mapper.readValue(response.getEntity().getContent(), JsonNode.class);
  }

  /** A POJO describing a connection configuration to Elasticsearch. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    public abstract List<HttpHost> getAddresses();

    @Nullable
    public abstract String getUsername();

    @Nullable
    public abstract String getPassword();

    @Nullable
    public abstract String getKeystorePath();

    @Nullable
    public abstract String getKeystorePassword();

    public abstract String getIndex();

    @Nullable
    public abstract String getType();

    @Nullable
    public abstract List<Header> getDefaultHeaders();

    @Nullable
    public abstract Integer getMaxRetryTimeoutMillis();

    @Nullable
    public abstract NodeSelector getNodeSelector();

    @Nullable
    public abstract String getPathPrefix();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<HttpHost> addresses);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setKeystorePath(String keystorePath);

      abstract Builder setKeystorePassword(String password);

      abstract Builder setIndex(String index);

      abstract Builder setType(String type);

      abstract Builder setDefaultHeaders(List<Header> defaultHeaders);

      abstract Builder setMaxRetryTimeoutMillis(Integer maxRetryTimeoutMillis);

      abstract Builder setNodeSelector(NodeSelector nodeSelector);

      abstract Builder setPathPrefix(String pathPrefix);

      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Elasticsearch connection configuration.
     *
     * @param index the index toward which the requests will be issued
     * @param addresses list of addresses of Elasticsearch nodes
     * @return the connection configuration object
     */
    public static ConnectionConfiguration create(String index, HttpHost... addresses) {
      checkArgument(addresses != null, "addresses can not be null");
      checkArgument(addresses.length > 0, "addresses can not be empty");
      checkArgument(index != null, "index can not be null");

      return new AutoValue_ElasticsearchHlrcIO_ConnectionConfiguration.Builder()
          .setAddresses(Arrays.asList(addresses))
          .setIndex(index)
          .build();
    }

    /**
     * List of addresses of Elasticsearch nodes.
     *
     * @param addresses list of addresses of Elasticsearch nodes
     * @return the connection configuration object
     */
    public ConnectionConfiguration withAddresses(HttpHost[] addresses) {
      checkArgument(addresses != null, "addresses can not be null");
      checkArgument(addresses.length > 0, "addresses can not be empty");
      return builder().setAddresses(Arrays.asList(addresses)).build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide the username.
     *
     * @param username the username used to authenticate to Elasticsearch
     * @return the connection configuration object
     */
    public ConnectionConfiguration withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      checkArgument(!username.isEmpty(), "username can not be empty");
      return builder().setUsername(username).build();
    }

    /**
     * If Elasticsearch authentication is enabled, provide the password.
     *
     * @param password the password used to authenticate to Elasticsearch
     * @return the connection configuration object
     */
    public ConnectionConfiguration withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      checkArgument(!password.isEmpty(), "password can not be empty");
      return builder().setPassword(password).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS with mutual authentication (via shield), provide the keystore
     * containing the restHighLevelClient key.
     *
     * @param keystorePath the location of the keystore containing the restHighLevelClient key.
     * @return the connection configuration object
     */
    public ConnectionConfiguration withKeystorePath(String keystorePath) {
      checkArgument(keystorePath != null, "keystorePath can not be null");
      checkArgument(!keystorePath.isEmpty(), "keystorePath can not be empty");
      return builder().setKeystorePath(keystorePath).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS with mutual authentication (via shield), provide the password
     * to open the restHighLevelClient keystore.
     *
     * @param keystorePassword the password of the restHighLevelClient keystore.
     * @return the connection configuration object
     */
    public ConnectionConfiguration withKeystorePassword(String keystorePassword) {
      checkArgument(keystorePassword != null, "keystorePassword can not be null");
      return builder().setKeystorePassword(keystorePassword).build();
    }

    public ConnectionConfiguration withDefaultHeaders(List<Header> defaultHeaders) {
      checkArgument(defaultHeaders != null, "defaultHeaders can not be null");
      return builder().setDefaultHeaders(defaultHeaders).build();
    }

    public ConnectionConfiguration withType(String type) {
      checkArgument(type != null, "type can not be null");
      return builder().setType(type).build();
    }

    public ConnectionConfiguration withMaxRetryTimeoutMillis(int maxRetryTimeoutMillis) {
      checkArgument(maxRetryTimeoutMillis >= 0, "maxRetryTimeoutMillis can not be smaller than 0");
      return builder().setMaxRetryTimeoutMillis(maxRetryTimeoutMillis).build();
    }

    public ConnectionConfiguration withNodeSelector(NodeSelector nodeSelector) {
      checkArgument(nodeSelector != null, "nodeSelector can not be null");
      return builder().setNodeSelector(nodeSelector).build();
    }

    public ConnectionConfiguration withPathPrefix(String pathPrefix) {
      checkArgument(pathPrefix != null, "pathPrefix can not be null");
      return builder().setPathPrefix(pathPrefix).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("address", getAddresses().toString()));
      builder.add(DisplayData.item("index", getIndex()));
      builder.addIfNotNull(DisplayData.item("type", getType()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
      builder.addIfNotNull(DisplayData.item("keystore.path", getKeystorePath()));
    }

    @VisibleForTesting
    RestHighLevelClient createClient() throws IOException {
      RestClientBuilder restClientBuilder =
          RestClient.builder(getAddresses()
              .toArray(new HttpHost[getAddresses().size()]));

      if (getUsername() != null) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY, new UsernamePasswordCredentials(getUsername(), getPassword()));
        restClientBuilder.setHttpClientConfigCallback(
            httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
      }

      if(getDefaultHeaders() != null) {
        restClientBuilder.setDefaultHeaders(getDefaultHeaders()
            .toArray(new Header[getDefaultHeaders().size()]));
      }

      if(getPathPrefix() != null) {
        restClientBuilder.setPathPrefix(getPathPrefix());
      }

      if(getMaxRetryTimeoutMillis() != null) {
        restClientBuilder.setMaxRetryTimeoutMillis(getMaxRetryTimeoutMillis());
      }

      if(getNodeSelector() != null) {
        restClientBuilder.setNodeSelector(getNodeSelector());
      }

      if (getKeystorePath() != null && !getKeystorePath().isEmpty()) {
        try {
          KeyStore keyStore = KeyStore.getInstance("jks");
          try (InputStream is = new FileInputStream(new File(getKeystorePath()))) {
            String keystorePassword = getKeystorePassword();
            keyStore.load(is, (keystorePassword == null) ? null : keystorePassword.toCharArray());
          }
          final SSLContext sslContext =
              SSLContexts.custom()
                  .loadTrustMaterial(keyStore, new TrustSelfSignedStrategy())
                  .build();
          final SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext);
          restClientBuilder.setHttpClientConfigCallback(
              httpClientBuilder ->
                  httpClientBuilder.setSSLContext(sslContext).setSSLStrategy(sessionStrategy));
        } catch (Exception e) {
          throw new IOException(
              "Can't load the restHighLevelClient certificate from the keystore", e);
        }
      }

      return new RestHighLevelClient(restClientBuilder);
    }
  }

  /** A {@link PTransform} reading data from Elasticsearch. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    private static final int MAX_BATCH_SIZE = 1000000;

    abstract ConnectionConfiguration getConnectionConfiguration();

    abstract int getBatchSize();

    abstract String getQuery();

    @Nullable
    abstract String getScrollKeepalive();

    @Nullable
    abstract String getSliceField();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setBatchSize(int batchSize);

      abstract Builder setQuery(String query);

      abstract Builder setScrollKeepalive(String scrollKeepalive);

      abstract Builder setSliceField(String sliceField);

      abstract Read build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration instance of ConnectionConfiguration
     * @return Read object
     */
    public Read withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a query used while reading from Elasticsearch.
     *
     * @param query Query in json format
     * @return Read object
     */
    public Read withQuery(String query) {
      checkArgument(query != null, "query can not be null");
      checkArgument(!query.trim().isEmpty(), "query can not be empty");
      return builder().setQuery(query).build();
    }

    /**
     * Provide a scroll keepalive. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">scroll
     * API</a> Default is "5m". Change this only if you get "No search context found" errors.
     *
     * @param scrollKeepalive time to keep the scroll alive
     * @return Read object
     */
    public Read withScrollKeepalive(String scrollKeepalive) {
      checkArgument(scrollKeepalive != null, "scrollKeepalive can not be null");
      checkArgument(!"0m".equals(scrollKeepalive), "scrollKeepalive can not be 0m");
      return builder().setScrollKeepalive(scrollKeepalive).build();
    }

    /**
     * Provide a size for the scroll read. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">
     * scroll API</a> Default is 100. Maximum is 10,000. If documents are small, increasing batch
     * size might improve read performance. If documents are big, you might need to decrease
     * batchSize.
     * @param batchSize number of documents read in each scroll read
     * @return Read object
     */
    public Read withBatchSize(int batchSize) {
      checkArgument(
          batchSize > 0 && batchSize <= MAX_BATCH_SIZE,
          "batchSize must be > 0 and <= %s, but was: %s",
          MAX_BATCH_SIZE,
          batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    /**
     * Set the slice field to slice the query on. When not specified, the _uid field will be used.
     * Make sure the field has the following properties:
     * - The field is numeric.
     * - doc_values are enabled on that field
     * - Every document should contain a single value. If a document has multiple values for the
     * specified field, the first value is used.
     * - The value for each document should be set once when the document is created and never
     * updated. This ensures that each slice gets deterministic results.
     * - The cardinality of the field should be high. This ensures that each slice gets
     * approximately the same amount of documents.
     * @param sliceField name of the field to slice on
     * @return Read object
     */
    public Read withSliceField(String sliceField) {
      checkArgument(sliceField != null, "sliceField cannot be null");
      return builder().setSliceField(sliceField).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");
      return input.apply(
          org.apache.beam.sdk.io.Read.from(new BoundedElasticsearchSource(this, null, null)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("query", getQuery()));
      builder.add(DisplayData.item("batchSize", getBatchSize()));
      builder.addIfNotNull(DisplayData.item("scrollKeepalive", getScrollKeepalive()));
      builder.addIfNotNull(DisplayData.item("sliceField", getSliceField()));
      Objects.requireNonNull(getConnectionConfiguration()).populateDisplayData(builder);
    }
  }

  /** A {@link BoundedSource} reading from Elasticsearch. */
  @VisibleForTesting
  public static class BoundedElasticsearchSource extends BoundedSource<String> {
    private static final String AGGREGATION_NAME = "avg_size";
    private static final String SIZE_FIELD = "_size";
    private final Read spec;
    private Long estimatedSize = -1L;

    @Nullable private final Integer numSlices;
    @Nullable private final Integer sliceId;

    @VisibleForTesting
    BoundedElasticsearchSource(
        Read spec,
        @Nullable Integer numSlices,
        @Nullable Integer sliceId) {
      this.spec = spec;
      this.numSlices = numSlices;
      this.sliceId = sliceId;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws IOException {
      long indexSize =  estimateIndexSize(spec.getQuery());
      float nbBundlesFloat = (float) indexSize / desiredBundleSizeBytes;
      int nbBundles = (int) Math.ceil(nbBundlesFloat);
      //ES slice api imposes that the number of slices is <= 1024 even if it can be overloaded
      if (nbBundles > 1024) {
        nbBundles = 1024;
      }
      List<BoundedElasticsearchSource> sources = new ArrayList<>();
      for (int i = 0; i < nbBundles; i++) {
        sources.add(new BoundedElasticsearchSource(spec, nbBundles, i));
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      if(estimatedSize != null) {
        return estimatedSize;
      } else {
        estimatedSize = estimateIndexSize(spec.getQuery());
        return estimatedSize;
      }
    }

    @VisibleForTesting
    public long estimateIndexSize(String query) throws IOException {
      if(this.estimatedSize > -1) {
        return this.estimatedSize;
      }
      ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
      RestHighLevelClient restHighLevelClient = connectionConfiguration.createClient();
      GetFieldMappingsRequest request = new GetFieldMappingsRequest();
      request.indices(connectionConfiguration.getIndex());
      if (connectionConfiguration.getType() != null) {
        request.types(connectionConfiguration.getType());
      } else {
        request.types("_doc");
      }
      request.fields(SIZE_FIELD);

      try {
        GetFieldMappingsResponse response = restHighLevelClient.indices()
            .getFieldMapping(request, RequestOptions.DEFAULT);
        final Map<String, Map<String, Map<String, GetFieldMappingsResponse.FieldMappingMetaData>>> mappings = response
            .mappings();
        final Map<String, GetFieldMappingsResponse.FieldMappingMetaData> typeMappings = mappings
            .get(connectionConfiguration.getIndex()).get(SIZE_FIELD);

        SearchRequest searchRequest = new SearchRequest(connectionConfiguration.getIndex());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new WrapperQueryBuilder(query));
        searchSourceBuilder.size(0);
        AvgAggregationBuilder aggregation = AggregationBuilders.avg(AGGREGATION_NAME)
            .field(SIZE_FIELD);
        searchSourceBuilder.aggregation(aggregation);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.
            search(searchRequest, RequestOptions.DEFAULT);

        Avg avg = searchResponse.getAggregations().get(AGGREGATION_NAME);
        return (long) Math.ceil(avg.getValue());
      } catch (Exception e) {
        LOG.info(String.format("The %s field doesn't exist so we can't estimate the document "
                + "size very well. Consider adding the %s field. "
                + "See: https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-size-usage.html",
            SIZE_FIELD, SIZE_FIELD));
      }

      JsonNode statsJson = getStats(connectionConfiguration);
      JsonNode indexStats = statsJson.path("indices").path(connectionConfiguration.getIndex())
          .path("primaries");
      JsonNode store = indexStats.path("store");
      return store.path("size_in_bytes").asLong();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("numSlices", numSlices));
      builder.addIfNotNull(DisplayData.item("sliceId", sliceId));
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
      return new BoundedElasticsearchReader(this);
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    private static JsonNode getStats(
        ConnectionConfiguration connectionConfiguration) throws IOException {
      String endpoint = String.format("/%s/_stats", connectionConfiguration.getIndex());
      try (RestClient restClient = connectionConfiguration.createClient().getLowLevelClient()) {
        Request request = new Request("GET", endpoint);
        return parseResponse(restClient.performRequest(request));
      }
    }
  }

  private static class BoundedElasticsearchReader extends BoundedSource.BoundedReader<String> {

    private final BoundedElasticsearchSource source;

    private RestHighLevelClient restHighLevelClient;
    private String current;
    private String scrollId;
    private ListIterator<String> batchIterator;

    private BoundedElasticsearchReader(BoundedElasticsearchSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      String index = Objects.requireNonNull(source.spec.getConnectionConfiguration()).getIndex();
      restHighLevelClient = source.spec.getConnectionConfiguration().createClient();

      String scrollKeepalive = source.spec.getScrollKeepalive();
      final Scroll scroll =
          new Scroll(
              TimeValue.parseTimeValue(
                  scrollKeepalive, TimeValue.timeValueMinutes(5L), "scrollKeepalive"));
      SearchRequest searchRequest = new SearchRequest(index);
      searchRequest.scroll(scroll);

      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(Integer.MAX_VALUE);

      // if there is more than one slice
      if (source.sliceId != null && source.numSlices != null && source.numSlices > 1) {
        SliceBuilder sliceBuilder;
        if(source.spec.getSliceField() != null) {
          sliceBuilder = new SliceBuilder(source.spec.getSliceField(), source.sliceId,
              source.numSlices);
        }
        else {
          sliceBuilder = new SliceBuilder(source.sliceId, source.numSlices);
        }
        searchSourceBuilder.slice(sliceBuilder);
      }
      WrapperQueryBuilder queryBuilder = new WrapperQueryBuilder(source.spec.getQuery());
      searchSourceBuilder.query(queryBuilder);
      searchSourceBuilder.size(source.spec.getBatchSize());
      searchRequest.source(searchSourceBuilder);

      SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
      scrollId = searchResponse.getScrollId();
      SearchHits searchHits = searchResponse.getHits();

      return readNextBatchAndReturnFirstDocument(searchHits);
    }

    @Override
    public boolean advance() throws IOException {
      if (batchIterator.hasNext()) {
        current = batchIterator.next();
        return true;
      } else {
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        String scrollKeepalive = source.spec.getScrollKeepalive();
        scrollRequest.scroll(
            TimeValue.parseTimeValue(
                scrollKeepalive, TimeValue.timeValueMinutes(5L), "scrollKeepalive"));
        SearchResponse searchScrollResponse = restHighLevelClient.scroll(scrollRequest,
            RequestOptions.DEFAULT);
        scrollId = searchScrollResponse.getScrollId();
        SearchHits searchHits = searchScrollResponse.getHits();

        return readNextBatchAndReturnFirstDocument(searchHits);
      }
    }

    private boolean readNextBatchAndReturnFirstDocument(SearchHits searchHits) throws IOException {
      //stop if no more data
      if (searchHits.totalHits == 0) {
        current = null;
        batchIterator = null;
        clearScrollRequest();
        return false;
      }
      // list behind iterator is empty
      List<String> batch = new ArrayList<>();
      for (SearchHit searchHit : searchHits) {
        String document = searchHit.getSourceAsString();
        batch.add(document);
      }
      batchIterator = batch.listIterator();
      if (batchIterator.hasNext()) {
        current = batchIterator.next();
      } else {
        return false;
      }
      return true;
    }

    private boolean clearScrollRequest() {
      if(scrollId != null) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        try {
          ClearScrollResponse clearScrollResponse = restHighLevelClient
              .clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
          return clearScrollResponse.isSucceeded();
        } catch (Exception e) {
          LOG.info("Scroll ID couldn't be found, probably gone already.");
        }
      }
      return true;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override
    public void close() throws IOException {
      // remove the scroll
      try {
        clearScrollRequest();
      } finally {
        if (restHighLevelClient != null) {
          restHighLevelClient.close();
        }
      }
    }

    @Override
    public BoundedSource<String> getCurrentSource() {
      return source;
    }
  }

  /** A {@link PTransform} writing data to Elasticsearch. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<DocWriteRequest>, PDone> {

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    abstract Long getFlushInterval();

    @Nullable
    abstract Integer getMaxBatchSize();

    @Nullable
    abstract Long getMaxBatchSizeBytes();

    @Nullable
    abstract Integer getTimeout();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      /**
       * {@link BulkProcessor.Builder#setFlushInterval(org.elasticsearch.common.unit.TimeValue)}.
       *
       * @param flushIntervalSeconds Sets a flush interval flushing *any* bulk actions pending if
       *                             the interval passes
       * @return Builder object
       */
      abstract Builder setFlushInterval(Long flushIntervalSeconds);

      /**
       * {@link BulkProcessor.Builder#setBulkActions(int)}.
       *
       * @param maxBatchSize Sets when to flush a new bulk request based on the number of actions
       *                     currently added
       * @return Builder object
       */
      abstract Builder setMaxBatchSize(Integer maxBatchSize);

      /**
       * {@link BulkProcessor.Builder#setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}.
       *
       * @param maxBatchSizeBytes Sets when to flush a new bulk request based on the size of
       *                          actions currently added
       * @return Builder object
       */
      abstract Builder setMaxBatchSizeBytes(Long maxBatchSizeBytes);

      /**
       * Sets the timeout for the bulk request.
       * @param timeout in seconds
       * @return Builder object
       */
      abstract Builder setTimeout(Integer timeout);

      abstract Write build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link Write} with connection configuration set
     */
    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a flush interval flushing *any* bulk actions pending if the interval passes. Defaults
     * to not set.
     *
     * @param flushInterval flush interval in seconds for the BulkProcessor.
     * @return the {@link Write} with flush interval set.
     */
    public Write withFlushInterval(Long flushInterval) {
      return builder().setFlushInterval(flushInterval).build();
    }

    /**
     * Provide a maximum size in number of documents for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html). Default is
     * 1000 docs (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSize maximum batch size in number of documents.
     * @return the {@link Write} with connection batch size set.
     */
    public Write withMaxBatchSize(int batchSize) {
      checkArgument(batchSize > 0, "batchSize must be > 0, but was %s", batchSize);
      return builder().setMaxBatchSize(batchSize).build();
    }

    /**
     * Provide a maximum size in bytes for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html). Default is
     * 5MB (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSizeBytes maximum batch size in bytes
     * @return the {@link Write} with connection batch size in bytes set
     */
    public Write withMaxBatchSizeBytes(long batchSizeBytes) {
      checkArgument(batchSizeBytes > 0, "batchSizeBytes must be > 0, but was %s", batchSizeBytes);
      return builder().setMaxBatchSizeBytes(batchSizeBytes).build();
    }

    /**
     * Sets the timeout for the bulk request.
     * @param timeout timeout in seconds
     * @return Write object
     */
    public Write withTimeout(int timeout) {
      checkArgument(timeout >= 0, "timeout has to be at least 0");
      return builder().setTimeout(timeout).build();
    }

    @Override
    public PDone expand(PCollection<DocWriteRequest> input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null, "withConnectionConfiguration() is required");
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    /**
     * See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-bulk-processor.html">Bulk
     * Processor</a> {@link DoFn} to for the {@link Write} transform.
     */
    @VisibleForTesting
    static class WriteFn extends DoFn<DocWriteRequest, Void> {

      private final Write spec;
      private transient RestHighLevelClient restHighLevelClient;
      //private BulkProcessor bulkProcessor;
      private transient BulkRequest bulkRequest;
      private transient Map<String, ValueInSingleWindow<DocWriteRequest>> docWriteRequests;
      private static final String FAILED_BULK_TAG_ID = "failedDocWriteRequest";
      private TupleTag<DocWriteRequest> tag = new TupleTag<>(FAILED_BULK_TAG_ID);

      @VisibleForTesting
      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws IOException {
        ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
        try {
          assert connectionConfiguration != null;
          restHighLevelClient = connectionConfiguration.createClient();
        } catch (IOException e) {
          throw new IOException("Couldn't connect to Elasticsearch.", e);
        }

        bulkRequest = new BulkRequest();
        if(spec.getTimeout() != null) {
          bulkRequest.timeout(TimeValue.timeValueSeconds(spec.getTimeout()));
        }
      }

      @StartBundle
      public void startBundle(StartBundleContext context) {
        docWriteRequests = new HashMap<>();
      }

      @ProcessElement
      public void processElement(ProcessContext context, BoundedWindow window) {
        docWriteRequests.put(context.element().id(), (ValueInSingleWindow.of(context.element(),
            context.timestamp(),
            window, context.pane())));
        //bulkProcessor.add(context.element());
        bulkRequest.add(context.element());
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context) throws Exception {
        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
          for(BulkItemResponse itemResponse : bulkResponse)
          {
            ValueInSingleWindow<DocWriteRequest> request = docWriteRequests.
                get(itemResponse.getId());
            context.output(tag, request.getValue(), request.getTimestamp(), request.getWindow());
          }
        }
      }

      @Teardown
      public void closeClient() throws Exception {
        if (restHighLevelClient != null) {
          restHighLevelClient.close();
        }
      }
    }
  }
}
