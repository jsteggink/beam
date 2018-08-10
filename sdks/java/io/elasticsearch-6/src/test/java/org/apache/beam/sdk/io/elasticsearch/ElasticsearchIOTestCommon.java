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

import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchIOTestUtils.NUM_SCIENTISTS;
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchIOTestUtils.countByMatch;
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchIOTestUtils.countByScientistName;
import static org.apache.beam.sdk.io.elasticsearch.ElasticSearchIOTestUtils.refreshIndexAndGetCurrentNumDocs;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.BoundedElasticsearchSource;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Read;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.parseResponse;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.hamcrest.CustomMatcher;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common test class for {@link ElasticsearchIO}. */
class ElasticsearchIOTestCommon implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchIOTestCommon.class);

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

  ElasticsearchIOTestCommon(ConnectionConfiguration connectionConfiguration,
      RestHighLevelClient restHighLevelClient,
      boolean useAsITests) {
    this.connectionConfiguration = connectionConfiguration;
    this.restHighLevelClient = restHighLevelClient;
    this.numDocs = useAsITests ? NUM_DOCS_ITESTS : NUM_DOCS_UTESTS;
    this.useAsITests = useAsITests;
  }

  // lazy init of the test rules (cannot be static)
  void setPipeline(TestPipeline pipeline) {
    this.pipeline = pipeline;
  }

  void setExpectedException(ExpectedException expectedException) {
    this.expectedException = expectedException;
  }

  void insertTestDocuments() throws IOException {
    ElasticSearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs,
          restHighLevelClient);
  }

  void testSizes() throws IOException {
    if (!useAsITests) {
      insertTestDocuments();
    }
    PipelineOptions options = PipelineOptionsFactory.create();
    Read read =
        ElasticsearchIO.read().withConnectionConfiguration(connectionConfiguration);
    BoundedElasticsearchSource initialSource = new BoundedElasticsearchSource(read,
        null, null, null);
    // can't use equal assert as Elasticsearch indexes never have same size
    // (due to internal Elasticsearch implementation)
    long estimatedSize = initialSource.getEstimatedSizeBytes(options);
    LOG.info("Estimated size: {}", estimatedSize);
    assertThat("Wrong estimated size", estimatedSize, greaterThan(AVERAGE_DOC_SIZE
        * numDocs));
  }


  void testRead() throws Exception {
    if (!useAsITests) {
      insertTestDocuments();
    }

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                //set to default value, useful just to test parameter passing.
                .withScrollKeepalive("5m")
                //set to default value, useful just to test parameter passing.
                .withBatchSize(100));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally())).isEqualTo(numDocs);
    pipeline.run();
  }

  void testReadWithQuery() throws Exception {
    if (!useAsITests){
      insertTestDocuments();
    }

    QueryBuilder queryBuilder = QueryBuilders.matchQuery("Einstein", "scientist");

    PCollection<String> output =
        pipeline.apply(
            ElasticsearchIO.read()
                .withConnectionConfiguration(connectionConfiguration)
                .withQueryBuilder(queryBuilder));
    PAssert.thatSingleton(output.apply("Count", Count.<String>globally()))
        .isEqualTo(numDocs / NUM_SCIENTISTS);
    pipeline.run();
  }

  void testWrite() throws Exception {
    List<DocWriteRequest> data =
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS,
            connectionConfiguration.getIndex());

    pipeline
        .apply(Create.of(data).withCoder(ESDocWriteRequestCoder.of()))
        .apply(ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration)
            .withBackOffPolicyConfiguration(ElasticsearchIO.BackOffPolicyConfiguration.create(
                ElasticsearchIO.BackOffPolicyConfiguration.BackOffPolicyType.EXPONENTIAL, null,
                null))
            .withConcurrentRequests(0)); // ES advices to set concurrent requests to 0 for testing.
    /* https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-bulk-processor.html#java-docs-bulk-processor-tests */
    pipeline.run();

    long currentNumDocs = ElasticSearchIOTestUtils
        .refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restHighLevelClient);
    assertEquals(numDocs, currentNumDocs);

    String requestBody =
        "{\n"
        + "  \"query\" : {\"match\": {\n"
        + "    \"scientist\": \"Einstein\"\n"
        + "  }}\n"
        + "}\n";
    String endPoint = String.format("/%s/%s/_search", connectionConfiguration.getIndex(),
        connectionConfiguration.getType());
    HttpEntity httpEntity = new NStringEntity(requestBody, ContentType.APPLICATION_JSON);
    Response response =
        restHighLevelClient.getLowLevelClient().performRequest(
            "GET",
            endPoint,
            Collections.<String, String>emptyMap(),
            httpEntity);
    JsonNode searchResult = parseResponse(response);
    int count = searchResult.path("hits").path("total").asInt();
    assertEquals(numDocs / NUM_SCIENTISTS, count);
  }

  void testWriteWithErrors() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withConcurrentRequests(0) // ES advices to set concurrent requests to 0 for testing.
            .withBackOffPolicyConfiguration(ElasticsearchIO.BackOffPolicyConfiguration.create(
                ElasticsearchIO.BackOffPolicyConfiguration.BackOffPolicyType.EXPONENTIAL, null,
                null))
            .withMaxBatchSize(BATCH_SIZE);
    List<DocWriteRequest> input =
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, ElasticSearchIOTestUtils.InjectionMode.INJECT_SOME_INVALID_DOCS,
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

  void testWriteWithMaxBatchSize() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withConcurrentRequests(0) // ES advices to set concurrent requests to 0 for testing.
            .withMaxBatchSize(BATCH_SIZE);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<DocWriteRequest, Void> fnTester = DoFnTester.of(new Write.WriteFn(write))) {
      List<DocWriteRequest> input =
          ElasticSearchIOTestUtils.createDocuments(
              numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS,
              connectionConfiguration.getIndex());
      long numDocsProcessed = 0;
      long numDocsInserted = 0;
      for (DocWriteRequest document : input) {
        fnTester.processElement(document);
        numDocsProcessed++;
        // test every 100 docs to avoid overloading ES
        if ((numDocsProcessed % 100) == 0) {
          // force the index to upgrade after inserting for the inserted docs
          // to be searchable immediately
          long currentNumDocs = ElasticSearchIOTestUtils
              .refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restHighLevelClient);
          if ((numDocsProcessed % BATCH_SIZE) == 0) {
          /* bundle end */
            assertEquals(
                "we are at the end of a bundle, we should have inserted all processed"
                    + " documents",
                numDocsProcessed,
                currentNumDocs);
            numDocsInserted = currentNumDocs;
          } else {
          /* not bundle end */
            assertEquals(
                "we are not at the end of a bundle, we should have inserted no more"
                    + " documents",
                numDocsInserted,
                currentNumDocs);
          }
        }
      }
    }
  }

  void testWriteWithMaxBatchSizeBytes() throws Exception {
    Write write =
        ElasticsearchIO.write()
            .withConnectionConfiguration(connectionConfiguration)
            .withConcurrentRequests(0) // ES advices to set concurrent requests to 0 for testing.
            .withMaxBatchSizeBytes(BATCH_SIZE_BYTES);
    // write bundles size is the runner decision, we cannot force a bundle size,
    // so we test the Writer as a DoFn outside of a runner.
    try (DoFnTester<DocWriteRequest, Void> fnTester = DoFnTester.of(new Write.WriteFn(write))) {
      List<DocWriteRequest> input =
          ElasticSearchIOTestUtils.createDocuments(
              numDocs, ElasticSearchIOTestUtils.InjectionMode.DO_NOT_INJECT_INVALID_DOCS,
              connectionConfiguration.getIndex());
      long numDocsProcessed = 0;
      long sizeProcessed = 0;
      long numDocsInserted = 0;
      long batchInserted = 0;
      for (DocWriteRequest document : input) {
        fnTester.processElement(document);
        numDocsProcessed++;
        // TODO Do we need this?
        //sizeProcessed += document.getBytes().length;
        // test every 40 docs to avoid overloading ES
        if ((numDocsProcessed % 40) == 0) {
          // force the index to upgrade after inserting for the inserted docs
          // to be searchable immediately
          long currentNumDocs = ElasticSearchIOTestUtils
              .refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restHighLevelClient);
          if (sizeProcessed / BATCH_SIZE_BYTES > batchInserted) {
          /* bundle end */
            assertThat(
                "we have passed a bundle size, we should have inserted some documents",
                currentNumDocs,
                greaterThan(numDocsInserted));
            numDocsInserted = currentNumDocs;
            batchInserted = (sizeProcessed / BATCH_SIZE_BYTES);
          } else {
          /* not bundle end */
            assertEquals(
                "we are not at the end of a bundle, we should have inserted no more"
                    + " documents",
                numDocsInserted,
                currentNumDocs);
          }
        }
      }
    }
  }

  /**
   * Tests partial updates by adding a group field to each document in the standard test set. The
   * group field is populated as the modulo 2 of the document id allowing for a test to ensure the
   * documents are split into 2 groups.
   */
  void testWritePartialUpdate() throws Exception {
    if (!useAsITests) {
      ElasticSearchIOTestUtils.insertTestDocuments(connectionConfiguration, numDocs, restHighLevelClient);
    }

    // defensive coding to ensure our initial state is as expected

    long currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restHighLevelClient);
    assertEquals(numDocs, currentNumDocs);

    // partial documents containing the ID and group only
    List<DocWriteRequest> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      IndexRequest doc = new IndexRequest(connectionConfiguration.getIndex(), "_doc", Integer.toString(i));
      doc.source(String.format("{\"group\" : %s}", i % 2), XContentType.JSON);
      data.add(doc);
    }

    pipeline
        .apply(Create.of(data).withCoder(ESDocWriteRequestCoder.of()))
        .apply(ElasticsearchIO.write().withConnectionConfiguration(connectionConfiguration)
            .withBackOffPolicyConfiguration(ElasticsearchIO.BackOffPolicyConfiguration.create(
                ElasticsearchIO.BackOffPolicyConfiguration.BackOffPolicyType.EXPONENTIAL, null,
                null))
            .withConcurrentRequests(0));
    pipeline.run();

    currentNumDocs = refreshIndexAndGetCurrentNumDocs(connectionConfiguration, restHighLevelClient);

    // check we have not unwittingly modified existing behaviour
    assertEquals(numDocs, currentNumDocs);
    assertEquals(
        numDocs / NUM_SCIENTISTS,
        countByScientistName(connectionConfiguration, restHighLevelClient, "Einstein"));

    // Partial update assertions
    assertEquals(numDocs / 2, countByMatch(connectionConfiguration, restHighLevelClient, "group", "0"));
    assertEquals(numDocs / 2, countByMatch(connectionConfiguration, restHighLevelClient, "group", "1"));
  }
}
