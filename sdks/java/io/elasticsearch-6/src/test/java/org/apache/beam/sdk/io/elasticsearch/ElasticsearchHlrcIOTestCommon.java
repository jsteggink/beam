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
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchHlrcIOTestUtils.countByMatch;
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchHlrcIOTestUtils.countByScientistName;
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchHlrcIOTestUtils.refreshIndexAndGetCurrentNumDocs;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchHlrcIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchHlrcIO.Write;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.hamcrest.CustomMatcher;
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
  static final float ACCEPTABLE_EMPTY_SPLITS_PERCENTAGE = 0.5f;
  private static final long AVERAGE_DOC_SIZE = 25L;

  private static final int BATCH_SIZE = 200;
  private static final long BATCH_SIZE_BYTES = 2048L;

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
    Settings settings = Settings.builder()
        .put("index.number_of_shards", 2)
        .put("index.number_of_replicas", 1)
        .build();
    return createIndex(settings);
  }

  CreateIndexResponse createIndex(Settings settings) throws IOException {
    return ElasticSearchHlrcIOTestUtils.createIndex(connectionConfiguration, restHighLevelClient,
        settings);
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
    if (!useAsITests) {
      insertTestDocuments();
    }

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchHlrcIO.read(connectionConfiguration,
                QueryBuilders.matchAllQuery().toString()));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(numDocs);
    pipeline.run();
  }

  void testReadWithQuery() throws Exception {
    if (!useAsITests) {
      insertTestDocuments();
    }

    QueryBuilder query = QueryBuilders.matchQuery("scientist", "Einstein");

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchHlrcIO.read(connectionConfiguration, query.toString()));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally()))
        .isEqualTo(numDocs / NUM_SCIENTISTS);
    pipeline.run();
  }

  void testWrite() throws Exception {
    List<DocWriteRequest> data =
        ElasticSearchHlrcIOTestUtils.createDocuments(
            numDocs,
            ElasticSearchHlrcIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS,
            connectionConfiguration.getIndex());

    pipeline
        .apply(Create.of(data).withCoder(ESDocWriteRequestCoder.of()))
        .apply(
            ElasticsearchHlrcIO.write(connectionConfiguration)
                .withBackOffPolicyConfiguration(
                    ElasticsearchHlrcIO.BackOffPolicyConfiguration.create(
                        ElasticsearchHlrcIO.BackOffPolicyConfiguration.BackOffPolicyType.EXPONENTIAL,
                        null,
                        null))
                );
    pipeline.run();

    long currentNumDocs =
        ElasticSearchHlrcIOTestUtils.refreshIndexAndGetCurrentNumDocs(
            connectionConfiguration, restHighLevelClient);
    assertEquals(numDocs, currentNumDocs);

    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery("scientist", "Einstein"));
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

    assertEquals(numDocs / NUM_SCIENTISTS, searchResponse.getHits().getTotalHits());
  }

  void testWriteWithErrors() throws Exception {
    Write write =
        ElasticsearchHlrcIO.write(connectionConfiguration)
            .withBackOffPolicyConfiguration(
                ElasticsearchHlrcIO.BackOffPolicyConfiguration.create(
                    ElasticsearchHlrcIO.BackOffPolicyConfiguration.BackOffPolicyType.EXPONENTIAL,
                    null,
                    null))
            .withMaxBatchSize(BATCH_SIZE);
    List<DocWriteRequest> input =
        ElasticSearchHlrcIOTestUtils.createDocuments(
            numDocs,
            ElasticSearchHlrcIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS,
            connectionConfiguration.getIndex());
    expectedException.expect(isA(IOException.class));
    expectedException.expectMessage(
        new CustomMatcher<String>("RegExp matcher") {
          @Override
          public boolean matches(Object o) {
            String message = (String) o;
            // This regexp tests that 2 malformed documents are actually in error
            // and that the message contains their IDs.
            // It also ensures that root reason, root error type,
            // caused by reason and caused by error type are present in message.
            // To avoid flakiness of the test in case of Elasticsearch error message change,
            // only "failed to parse" root reason is matched,
            // the other messages are matched using .+
            return message.matches(
                "(?is).*Error writing to Elasticsearch, some elements could not be inserted"
                    + ".*Document id .+: failed to parse \\(.+\\).*Caused by: .+ \\(.+\\).*"
                    + "Document id .+: failed to parse \\(.+\\).*Caused by: .+ \\(.+\\).*");
          }
        });
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<DocWriteRequest, Void> fnTester = DoFnTester.of(new Write.WriteFn(write))) {
      // inserts into Elasticsearch
      fnTester.processBundle(input);
    }

  }

  /**
   * Tests partial updates by adding a group field to each document in the standard test set. The
   * group field is populated as the modulo 2 of the document id allowing for a test to ensure the
   * documents are split into 2 groups.
   */
  void testWritePartialUpdate() throws Exception {
    if (!useAsITests) {
      ElasticSearchHlrcIOTestUtils.insertTestDocuments(
          connectionConfiguration, numDocs, restHighLevelClient);
    }

    // defensive coding to ensure our initial state is as expected

    long currentNumDocs =
        refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restHighLevelClient);
    assertEquals(numDocs, currentNumDocs);

    // partial documents containing the ID and group only
    List<DocWriteRequest> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      IndexRequest doc =
          new IndexRequest(connectionConfiguration.getIndex(), "_doc", Integer.toString(i));
      doc.source(String.format("{\"group\" : %s}", i % 2), XContentType.JSON);
      data.add(doc);
    }

    pipeline
        .apply(Create.of(data).withCoder(ESDocWriteRequestCoder.of()))
        .apply(
            ElasticsearchHlrcIO.write(connectionConfiguration)
                .withBackOffPolicyConfiguration(
                    ElasticsearchHlrcIO.BackOffPolicyConfiguration.create(
                        ElasticsearchHlrcIO.BackOffPolicyConfiguration.BackOffPolicyType.EXPONENTIAL,
                        null,
                        null))
                );
    pipeline.run();

    currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restHighLevelClient);

    // check we have not unwittingly modified existing behaviour
    assertEquals(numDocs, currentNumDocs);
    assertEquals(
        numDocs / NUM_SCIENTISTS,
        countByScientistName(connectionConfiguration, restHighLevelClient, "Einstein"));

    // Partial update assertions
    assertEquals(
        numDocs / 2, countByMatch(connectionConfiguration, restHighLevelClient, "group", "0"));
    assertEquals(
        numDocs / 2, countByMatch(connectionConfiguration, restHighLevelClient, "group", "1"));
  }
}
