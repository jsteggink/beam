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

import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchHlrcIOTestUtils.NUM_SCIENTISTS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchHlrcIO.ConnectionConfiguration;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import junit.framework.TestCase;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common test class for {@link ElasticsearchHlrcIO}. */
class ElasticsearchHlrcIOTestCommon implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchHlrcIOTestCommon.class);

  static final String ES_INDEX = "beam";
  static final String ES_TYPE = "_doc";
  static final long NUM_DOCS_UTESTS = 400L;
  static final long NUM_DOCS_ITESTS = 50000L;

  private static final int BATCH_SIZE = 200;

  private final long numDocs;
  private final ConnectionConfiguration connectionConfiguration;

  private final RestHighLevelClient restHighLevelClient;
  private final boolean useAsITests;

  private TestPipeline pipeline;
  private ExpectedException expectedException;

  ElasticsearchHlrcIOTestCommon(
      ConnectionConfiguration connectionConfiguration,
      RestHighLevelClient restHighLevelClient,
      boolean useAsITests) {
    this.connectionConfiguration = connectionConfiguration;
    this.restHighLevelClient = restHighLevelClient;
    this.numDocs = useAsITests ? NUM_DOCS_ITESTS : NUM_DOCS_UTESTS;
    this.useAsITests = useAsITests;

  }

  CreateIndexResponse createIndex() throws IOException {
    Settings settings =
        Settings.builder()
            .put("index.number_of_shards", 2)
            .put("index.number_of_replicas", 1)
            .build();
    return createIndex(settings);
  }

  CreateIndexResponse createIndex(Settings settings) throws IOException {
    return ElasticSearchHlrcIOTestUtils.createIndex(
        connectionConfiguration, restHighLevelClient, settings);
  }

  // lazy init of the test rules (cannot be static)
  void setPipeline(TestPipeline pipeline) {
    this.pipeline = pipeline;
  }

  void setExpectedException(ExpectedException expectedException) {
    this.expectedException = expectedException;
  }

  private void insertTestDocuments() throws IOException {
    ElasticSearchHlrcIOTestUtils.insertTestDocuments(
        connectionConfiguration, numDocs, restHighLevelClient);
  }

  void testRead() throws Exception {
    insertTestDocuments();

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchHlrcIO.read(
                connectionConfiguration, QueryBuilders.matchAllQuery().toString()));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(numDocs);
    pipeline.run();
  }

  void testReadWithQuery() throws Exception {
    insertTestDocuments();

    QueryBuilder query = QueryBuilders.matchQuery("scientist", "Einstein");

    PCollection<String> output =
        pipeline.apply(ElasticsearchHlrcIO.read(connectionConfiguration, query.toString()));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally()))
        .isEqualTo(numDocs / NUM_SCIENTISTS);
    pipeline.run();
  }

  void testWrite() throws Exception {
    List<DocWriteRequest> data =
        ElasticSearchHlrcIOTestUtils.createIndexRequests(
            numDocs, connectionConfiguration.getIndex());

    pipeline
        .apply(Create.of(data).withCoder(ESDocWriteRequestCoder.of()))
        .apply(ElasticsearchHlrcIO.write(connectionConfiguration));
    pipeline.run();

    long currentNumDocs =
        ElasticSearchHlrcIOTestUtils.refreshIndexAndGetCurrentNumDocs(
            connectionConfiguration, restHighLevelClient);
    assertEquals(numDocs, currentNumDocs);
  }

  void testDelete() throws Exception {
    insertTestDocuments();

    List<DocWriteRequest> data =
        ElasticSearchHlrcIOTestUtils.createDeleteRequests(
            numDocs, connectionConfiguration.getIndex());

    pipeline
        .apply(Create.of(data).withCoder(ESDocWriteRequestCoder.of()))
        .apply(ElasticsearchHlrcIO.write(connectionConfiguration));
    pipeline.run();

    long currentNumDocs =
        ElasticSearchHlrcIOTestUtils.refreshIndexAndGetCurrentNumDocs(
            connectionConfiguration, restHighLevelClient);

    assertEquals(0, currentNumDocs);
  }

  void testUpdate() throws Exception {
    insertTestDocuments();

    List<DocWriteRequest> data =
        ElasticSearchHlrcIOTestUtils.createUpdateRequests(
            numDocs, connectionConfiguration.getIndex());

    pipeline
        .apply(Create.of(data).withCoder(ESDocWriteRequestCoder.of()))
        .apply(ElasticsearchHlrcIO.write(connectionConfiguration));
    pipeline.run();

    long currentNumDocs =
        ElasticSearchHlrcIOTestUtils.refreshIndexAndGetCurrentNumDocs(
            connectionConfiguration, restHighLevelClient);
    assertEquals(numDocs, currentNumDocs);

    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery("scientist",
        new StringBuilder("Einstein").reverse().toString()));
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse =
        restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

    assertEquals(numDocs / NUM_SCIENTISTS, searchResponse.getHits().getTotalHits());
  }

  void testSplitsVolume() throws Exception {
    String query = QueryBuilders.matchAllQuery().toString();
    ElasticsearchHlrcIO.Read read =
        ElasticsearchHlrcIO.read(connectionConfiguration, query);
    ElasticsearchHlrcIO.BoundedElasticsearchSource initialSource =
        new ElasticsearchHlrcIO.BoundedElasticsearchSource(read, null, null);
    int desiredBundleSizeBytes = 10000;
    // Empty options, we don't need it.
    PipelineOptions options = PipelineOptionsFactory.create();
    List<? extends BoundedSource<String>> splits =
        initialSource.split(desiredBundleSizeBytes, options);
    SourceTestUtils.assertSourcesEqualReferenceSource(initialSource, splits, options);
    long indexSize = initialSource.estimateIndexSize(query);
    float expectedNumSourcesFloat = (float) indexSize / desiredBundleSizeBytes;
    int expectedNumSources = (int) Math.ceil(expectedNumSourcesFloat);
    TestCase.assertEquals(expectedNumSources, splits.size());
    int nonEmptySplits = 0;
    for (BoundedSource<String> subSource : splits) {
      if (readFromSource(subSource, options).size() > 0) {
        nonEmptySplits += 1;
      }
    }
    TestCase.assertEquals("Wrong number of empty splits", expectedNumSources, nonEmptySplits);
  }
}