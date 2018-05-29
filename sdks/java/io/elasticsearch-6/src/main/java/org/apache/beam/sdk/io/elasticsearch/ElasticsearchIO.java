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
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
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
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;

/**
 * Transforms for reading and writing data from/to Elasticsearch.
 *
 * <h3>Reading from Elasticsearch</h3>
 *
 * <p>{@link ElasticsearchIO#read ElasticsearchIO.read()} returns a bounded {@link PCollection
 * PCollection&lt;String&gt;} representing JSON documents.
 *
 * <p>To configure the {@link ElasticsearchIO#read}, you have to provide a connection configuration
 * containing the HTTP address of the instances, an index name and a type. The following example
 * illustrates options for configuring the source:
 *
 * <pre>{@code
 * pipeline.apply(ElasticsearchIO.read().withConnectionConfiguration(
 *    ElasticsearchIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
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
 * <p>To write documents to Elasticsearch, use {@link ElasticsearchIO#write
 * ElasticsearchIO.write()}, which writes JSON documents from a {@link PCollection
 * PCollection&lt;String&gt;} (which can be bounded or unbounded).
 *
 * <p>To configure {@link ElasticsearchIO#write ElasticsearchIO.write()}, similar to the read, you
 * have to provide a connection configuration. For instance:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(ElasticsearchIO.write().withConnectionConfiguration(
 *      ElasticsearchIO.ConnectionConfiguration.create("http://host:9200", "my-index", "my-type")
 *   )
 *
 * }</pre>
 *
 * <p>Optionally, you can provide {@code withBatchSize()} and {@code withBatchSizeBytes()}
 * to specify the size of the write batch in number of documents or in bytes.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class ElasticsearchIO {

  private static final int VALID_ES_VERSION = 6;

  public static Read read() {
    // TODO Why are we setting defaults instead of using ES defaults.
    // default scrollKeepalive = 5m as a majorant for un-predictable time between 2 start/read calls
    // default batchSize to 100 as recommended by ES dev team as a safe value when dealing
    // with big documents and still a good compromise for performances
    return new AutoValue_ElasticsearchIO_Read.Builder()
        .setScrollKeepalive("5m")
        .setBatchSize(100)
        .build();
  }

  public static Write write() {
    // TODO Why are we setting defaults instead of using ES defaults.
    return new AutoValue_ElasticsearchIO_Write.Builder()
        // default ES value
        //.setBackOffPolicy(BackoffPolicy.exponentialBackoff())
        // advised default starting batch size in ES docs
        .setMaxBatchSize(1000)
        // advised default starting batch size bytes in ES docs
        .setMaxBatchSizeBytes(5L * 1024L * 1024L)
        // default ES value
        .setConcurrentRequests(1)
        .build();
  }

  private ElasticsearchIO() {}

  private static final ObjectMapper mapper = new ObjectMapper();

  @VisibleForTesting
  static JsonNode parseResponse(Response response) throws IOException {
    return mapper.readValue(response.getEntity().getContent(), JsonNode.class);
  }

  static void checkForErrors(Response response) throws IOException {
    JsonNode searchResult = parseResponse(response);
    boolean errors = searchResult.path("errors").asBoolean();
    if (errors) {
      StringBuilder errorMessages =
          new StringBuilder(
              "Error writing to Elasticsearch, some elements could not be inserted:");
      JsonNode items = searchResult.path("items");
      //some items present in bulk might have errors, concatenate error messages
      for (JsonNode item : items) {
        JsonNode errorRoot = item.path("index");
        JsonNode error = errorRoot.get("error");
        if (error != null) {
          String type = error.path("type").asText();
          String reason = error.path("reason").asText();
          String docId = errorRoot.path("_id").asText();
          errorMessages.append(String.format("%nDocument id %s: %s (%s)", docId, reason, type));
          JsonNode causedBy = error.get("caused_by");
          if (causedBy != null) {
            String cbReason = causedBy.path("reason").asText();
            String cbType = causedBy.path("type").asText();
            errorMessages.append(String.format("%nCaused by: %s (%s)", cbReason, cbType));
          }
        }
      }
      throw new IOException(errorMessages.toString());
    }
  }

  /** A POJO describing a connection configuration to Elasticsearch. */
  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {

    public abstract List<String> getAddresses();

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

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setAddresses(List<String> addresses);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setKeystorePath(String keystorePath);

      abstract Builder setKeystorePassword(String password);

      abstract Builder setIndex(String index);

      abstract Builder setType(String type);

      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Elasticsearch connection configuration.
     *
     * @param addresses list of addresses of Elasticsearch nodes
     * @param index the index toward which the requests will be issued
     * @param type the document type toward which the requests will be issued, best to set to
     *             "_doc" or null since types are deprecated and will be removed in ES 7.0.0
     * @return the connection configuration object
     */
    public static ConnectionConfiguration create(String[] addresses, String index, String type) {
      checkArgument(addresses != null, "addresses can not be null");
      checkArgument(addresses.length > 0, "addresses can not be empty");
      checkArgument(index != null, "index can not be null");
      // The new default of type is _doc since types are deprecated and will be removed in ES 7.
      // https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html
      if (type == null) {
        type = "_doc";
      }
      ConnectionConfiguration connectionConfiguration =
          new AutoValue_ElasticsearchIO_ConnectionConfiguration.Builder()
              .setAddresses(Arrays.asList(addresses))
              .setIndex(index)
              .setType(type)
              .build();
      return connectionConfiguration;
    }

    /**
     * If Elasticsearch authentication is enabled, provide the username.
     *
     * @param username the username used to authenticate to Elasticsearch
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
     */
    public ConnectionConfiguration withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      checkArgument(!password.isEmpty(), "password can not be empty");
      return builder().setPassword(password).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS with mutual authentication (via shield),
     * provide the keystore containing the restHighLevelClient key.
     *
     * @param keystorePath the location of the keystore containing the restHighLevelClient key.
     */
    public ConnectionConfiguration withKeystorePath(String keystorePath) {
      checkArgument(keystorePath != null, "keystorePath can not be null");
      checkArgument(!keystorePath.isEmpty(), "keystorePath can not be empty");
      return builder().setKeystorePath(keystorePath).build();
    }

    /**
     * If Elasticsearch uses SSL/TLS with mutual authentication (via shield),
     * provide the password to open the restHighLevelClient keystore.
     *
     * @param keystorePassword the password of the restHighLevelClient keystore.
     */
    public ConnectionConfiguration withKeystorePassword(String keystorePassword) {
        checkArgument(keystorePassword != null, "keystorePassword can not be null");
        return builder().setKeystorePassword(keystorePassword).build();
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("address", getAddresses().toString()));
      builder.add(DisplayData.item("index", getIndex()));
      builder.add(DisplayData.item("type", getType()));
      builder.addIfNotNull(DisplayData.item("username", getUsername()));
      builder.addIfNotNull(DisplayData.item("keystore.path", getKeystorePath()));
    }

    @VisibleForTesting RestHighLevelClient createClient() throws IOException {
      HttpHost[] hosts = new HttpHost[getAddresses().size()];
      int i = 0;
      for (String address : getAddresses()) {
        URL url = new URL(address);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      }

      RestClientBuilder restClientBuilder = RestClient.builder(hosts);
      if (getUsername() != null) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY, new UsernamePasswordCredentials(getUsername(), getPassword()));
        restClientBuilder.setHttpClientConfigCallback(
            httpAsyncClientBuilder -> httpAsyncClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider));
      }

      if (getKeystorePath() != null && !getKeystorePath().isEmpty()) {
        try {
          KeyStore keyStore = KeyStore.getInstance("jks");
          try (InputStream is = new FileInputStream(new File(getKeystorePath()))) {
            String keystorePassword = getKeystorePassword();
            keyStore.load(is, (keystorePassword == null) ? null : keystorePassword.toCharArray());
          }
          final SSLContext sslContext = SSLContexts.custom()
              .loadTrustMaterial(keyStore, new TrustSelfSignedStrategy()).build();
          final SSLIOSessionStrategy sessionStrategy = new SSLIOSessionStrategy(sslContext);
          restClientBuilder.setHttpClientConfigCallback(
              httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext)
                  .setSSLStrategy(sessionStrategy));
        } catch (Exception e) {
          throw new IOException("Can't load the restHighLevelClient certificate from the keystore",
              e);
        }
      }

      RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);

      return client;
    }
  }

  /** A {@link PTransform} reading data from Elasticsearch. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    private static final int MAX_BATCH_SIZE = 10000;

    @Nullable
    abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable
    abstract QueryBuilder getQueryBuilder();

    abstract String getScrollKeepalive();

    abstract int getBatchSize();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setQueryBuilder(QueryBuilder queryBuilder);

      abstract Builder setScrollKeepalive(String scrollKeepalive);

      abstract Builder setBatchSize(int batchSize);

      abstract Read build();
    }

    /** Provide the Elasticsearch connection configuration object. */
    public Read withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a query used while reading from Elasticsearch.
     *
     * @param queryBuilder instance of QueryBuilder. See <a
     *     href="https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest
     *                     -high-query-builders.html">Building Queries</a>
     */
    public Read withQueryBuilder(QueryBuilder queryBuilder) {
      checkArgument(queryBuilder != null, "queryBuilder can not be null");
      return builder().setQueryBuilder(queryBuilder).build();
    }

    /**
     * Provide a scroll keepalive. See <a
     * href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">scroll
     * API</a> Default is "5m". Change this only if you get "No search context found" errors.
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
     * batchSize
     *
     * @param batchSize number of documents read in each scroll read
     */
    public Read withBatchSize(int batchSize) {
      checkArgument(
          batchSize > 0 && batchSize <= MAX_BATCH_SIZE,
          "batchSize must be > 0 and <= %d, but was: %d",
          MAX_BATCH_SIZE,
          batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(
          connectionConfiguration != null,
          "withConnectionConfiguration() is required");
      return input.apply(org.apache.beam.sdk.io.Read
          .from(new BoundedElasticsearchSource(this, null, null, null)));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (null != getQueryBuilder()) {
        builder.addIfNotNull(DisplayData.item("query", getQueryBuilder().toString()));
      }
      builder.addIfNotNull(DisplayData.item("batchSize", getBatchSize()));
      builder.addIfNotNull(DisplayData.item("scrollKeepalive", getScrollKeepalive()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /** A {@link BoundedSource} reading from Elasticsearch. */
  @VisibleForTesting
  public static class BoundedElasticsearchSource extends BoundedSource<String> {

    private int backendVersion;

    private final Read spec;
    // shardPreference is the shard id where the source will read the documents
    @Nullable
    private final String shardPreference;
    @Nullable
    private final Integer numSlices;
    @Nullable
    private final Integer sliceId;

    //constructor used in split() when we know the backend version
    private BoundedElasticsearchSource(Read spec, @Nullable String shardPreference,
        @Nullable Integer numSlices, @Nullable Integer sliceId, int backendVersion) {
      this.backendVersion = backendVersion;
      this.spec = spec;
      this.shardPreference = shardPreference;
      this.numSlices = numSlices;
      this.sliceId = sliceId;
    }

    @VisibleForTesting
    BoundedElasticsearchSource(Read spec, @Nullable String shardPreference,
        @Nullable Integer numSlices, @Nullable Integer sliceId) {
      this.spec = spec;
      this.shardPreference = shardPreference;
      this.numSlices = numSlices;
      this.sliceId = sliceId;
    }

    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
      this.backendVersion = getBackendVersion(connectionConfiguration);
      List<BoundedElasticsearchSource> sources = new ArrayList<>();
      long indexSize = BoundedElasticsearchSource.estimateIndexSize(connectionConfiguration);
      float nbBundlesFloat = (float) indexSize / desiredBundleSizeBytes;
      int nbBundles = (int) Math.ceil(nbBundlesFloat);
      //ES slice api imposes that the number of slices is <= 1024 even if it can be overloaded
      if (nbBundles > 1024) {
        nbBundles = 1024;
      }
      // split the index into nbBundles chunks of desiredBundleSizeBytes by creating
      // nbBundles sources each reading a slice of the index
      // (see https://goo.gl/MhtSWz)
      // the slice API allows to split the ES shards
      // to have bundles closer to desiredBundleSizeBytes
      for (int i = 0; i < nbBundles; i++) {
        sources.add(new BoundedElasticsearchSource(spec, null, nbBundles, i, backendVersion));
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      return estimateIndexSize(spec.getConnectionConfiguration());
    }

    @VisibleForTesting
    static long estimateIndexSize(ConnectionConfiguration connectionConfiguration)
        throws IOException {
      // we use indices stats API to estimate size and list the shards
      // (https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-stats.html)
      // as Elasticsearch 2.x doesn't not support any way to do parallel read inside a shard
      // the estimated size bytes is not really used in the split into bundles.
      // However, we implement this method anyway as the runners can use it.
      // NB: Elasticsearch 5.x now provides the slice API.
      // (https://www.elastic.co/guide/en/elasticsearch/reference/5.0/search-request-scroll.html
      // #sliced-scroll)
      JsonNode statsJson = getStats(connectionConfiguration, false);
      JsonNode indexStats =
          statsJson
              .path("indices")
              .path(connectionConfiguration.getIndex())
              .path("primaries");
      JsonNode store = indexStats.path("store");
      return store.path("size_in_bytes").asLong();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("shard", shardPreference));
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

    // TODO See if we can convert this to the RestHighLevelClient.
    private static JsonNode getStats(ConnectionConfiguration connectionConfiguration,
        boolean shardLevel) throws IOException {
      HashMap<String, String> params = new HashMap<>();
      if (shardLevel) {
        params.put("level", "shards");
      }
      RestHighLevelClient client = connectionConfiguration.createClient();
      String endpoint = String.format("/%s/_stats", connectionConfiguration.getIndex());
      try (RestClient restClient = connectionConfiguration.createClient().getLowLevelClient()) {

        return parseResponse(
            restClient.performRequest("GET", endpoint, params));
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
      String index = source.spec.getConnectionConfiguration().getIndex();
      restHighLevelClient = source.spec.getConnectionConfiguration().createClient();
      String scrollKeepalive = source.spec.getScrollKeepalive();
      final Scroll scroll =
          new Scroll(TimeValue.parseTimeValue(scrollKeepalive, TimeValue.timeValueMinutes(5L),
              "scrollKeepalive"));
      SearchRequest searchRequest = new SearchRequest(index);
      searchRequest.scroll(scroll);

      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(Integer.MAX_VALUE);

      // if there is more than one slice
      if (source.numSlices != null && source.numSlices > 1) {
        SliceBuilder sliceBuilder = new SliceBuilder(source.sliceId, source.numSlices);
        searchSourceBuilder.slice(sliceBuilder);
      }
      QueryBuilder queryBuilder = source.spec.getQueryBuilder();
      if (queryBuilder == null) {
        searchSourceBuilder.query(matchAllQuery());
      } else {
        searchSourceBuilder.query(queryBuilder);
      }

      searchSourceBuilder.size(source.spec.getBatchSize());
      searchRequest.source(searchSourceBuilder);

      SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
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
        scrollRequest.scroll(TimeValue.parseTimeValue(scrollKeepalive,
            TimeValue.timeValueMinutes(5L), "scrollKeepalive"));
        SearchResponse searchScrollResponse = restHighLevelClient.searchScroll(scrollRequest);
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
      for (SearchHit searchHit: searchHits) {
        String document = searchHit.getSourceAsString();
        batch.add(document);
      }
      batchIterator = batch.listIterator();
      current = batchIterator.next();
      return true;
    }

    private boolean clearScrollRequest() throws IOException {
      ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
      clearScrollRequest.addScrollId(scrollId);
      ClearScrollResponse clearScrollResponse = restHighLevelClient.clearScroll(clearScrollRequest);
      return clearScrollResponse.isSucceeded();
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

    //@Nullable
    //abstract BackoffPolicy getBackOffPolicy();

    @Nullable
    abstract Integer getConcurrentRequests();

    @Nullable
    abstract Long getFlushInterval();

    @Nullable
    abstract Integer getMaxBatchSize();

    @Nullable
    abstract Long getMaxBatchSizeBytes();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      /**
       *{@link BulkProcessor.Builder#setBackoffPolicy(org.elasticsearch.action.bulk.BackoffPolicy)}.
       * @param backOffPolicy
       * @return
       */
      // TODO The BackOffPolicy can't be serialized, so we have to make something custom.
      //abstract Builder setBackOffPolicy(BackoffPolicy backOffPolicy);

      /**
       * {@link BulkProcessor.Builder#setConcurrentRequests(int)}.
       * @param concurrentRequests
       * @return
       */
      abstract Builder setConcurrentRequests(Integer concurrentRequests);

      /**
       * {@link BulkProcessor.Builder#setFlushInterval(org.elasticsearch.common.unit.TimeValue)}.
       * @param flushIntervalSeconds
       * @return
       */
      abstract Builder setFlushInterval(Long flushIntervalSeconds);

      /**
       * {@link BulkProcessor.Builder#setBulkActions(int)}.
       * @param maxBatchSize
       * @return
       */
      abstract Builder setMaxBatchSize(Integer maxBatchSize);

      /**
       * {@link BulkProcessor.Builder#setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}.
       * @param maxBatchSizeBytes
       * @return
       */
      abstract Builder setMaxBatchSizeBytes(Long maxBatchSizeBytes);

      abstract Write build();
    }

    /**
     * Provide the Elasticsearch connection configuration object.
     *
     * @param connectionConfiguration the Elasticsearch {@link ConnectionConfiguration} object
     * @return the {@link Write} with connection configuration set
     */
    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null,
          "connectionConfiguration can not be null");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a maximum size in number of documents for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html). Default is
     * 1000
     * docs (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSize maximum batch size in number of documents
     * @return the {@link Write} with connection batch size set
     */
    public Write withMaxBatchSize(int batchSize) {
      checkArgument(batchSize > 0,
          "batchSize must be > 0, but was %d", batchSize);
      return builder().setMaxBatchSize(batchSize).build();
    }

    /**
     * Provide a maximum size in bytes for the batch see bulk API
     * (https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html).
     * Default is 5MB (like Elasticsearch bulk size advice). See
     * https://www.elastic.co/guide/en/elasticsearch/guide/current/bulk.html Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller ElasticSearch bulks.
     *
     * @param batchSizeBytes maximum batch size in bytes
     * @return the {@link Write} with connection batch size in bytes set
     */
    public Write withMaxBatchSizeBytes(long batchSizeBytes) {
      checkArgument(batchSizeBytes > 0,
          "batchSizeBytes must be > 0, but was %d", batchSizeBytes);
      return builder().setMaxBatchSizeBytes(batchSizeBytes).build();
    }

    // TODO Add other options.

    @Override
    public PDone expand(PCollection<DocWriteRequest> input) {
      ConnectionConfiguration connectionConfiguration = getConnectionConfiguration();
      checkState(connectionConfiguration != null,
          "withConnectionConfiguration() is required");
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    /**
     * See <a href="https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-bulk-processor.html">Bulk Processor</a>
     * {@link DoFn} to for the {@link Write} transform.
     * */
    @VisibleForTesting
    static class WriteFn extends DoFn<DocWriteRequest, Void> {

      private int backendVersion;
      private final Write spec;
      private transient RestHighLevelClient restHighLevelClient;
      private ArrayList<String> batch;
      private long currentBatchSizeBytes;
      private BulkProcessor bulkProcessor;

      @VisibleForTesting
      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup
      public void setup() throws Exception {
        ConnectionConfiguration connectionConfiguration = spec.getConnectionConfiguration();
        backendVersion = getBackendVersion(connectionConfiguration);
        restHighLevelClient = connectionConfiguration.createClient();

        // TODO Have a configurable BulkProcessor

        BulkProcessor.Listener bulkProcessorListener = new BulkProcessor.Listener() {
          @Override
          public void beforeBulk(long executionId, BulkRequest request) {

          }

          @Override
          public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {

          }

          @Override
          public void afterBulk(long executionId, BulkRequest request, Throwable failure) {

          }
        };

        BulkProcessor.Builder bulkProcessorBuilder = BulkProcessor.builder(
              restHighLevelClient::bulkAsync, bulkProcessorListener
          );

        bulkProcessorBuilder.setBulkActions(spec.getMaxBatchSize())
            .setBulkSize(new ByteSizeValue(spec.getMaxBatchSizeBytes()))
            .setConcurrentRequests(spec.getConcurrentRequests());

        if (null != spec.getFlushInterval()) {
          bulkProcessorBuilder.
              setFlushInterval(TimeValue.timeValueSeconds(spec.getFlushInterval()));
        }

        // TODO The BackOffPolicy can't be serialized, so we have to make something custom.
        /*if (null != spec.getBackOffPolicy()) {
          bulkProcessorBuilder.setBackoffPolicy(spec.getBackOffPolicy());
        }*/

        bulkProcessor = bulkProcessorBuilder.build();
      }

      @StartBundle
      public void startBundle(StartBundleContext context) throws Exception {
        batch = new ArrayList<>();
        currentBatchSizeBytes = 0;
      }

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        bulkProcessor.add(context.element());
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context) throws Exception {
        bulkProcessor.flush();
      }

      @Teardown
      public void closeClient() throws Exception {
        if (restHighLevelClient != null) {
          restHighLevelClient.close();
        }
      }
    }
  }

  static int getBackendVersion(ConnectionConfiguration connectionConfiguration) {
    try (RestHighLevelClient restClient = connectionConfiguration.createClient()) {
      MainResponse response = restClient.info();
      int backendVersion = response.getVersion().major;
      checkArgument((backendVersion == VALID_ES_VERSION),
          "The Elasticsearch version to connect to is %s.x. "
          + "This version of the ElasticsearchIO is only compatible with Elasticsearch %s.x",
          backendVersion, VALID_ES_VERSION);
      return backendVersion;

    } catch (IOException e){
      throw (new IllegalArgumentException("Cannot get Elasticsearch version"));
    }
  }
}