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

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.ConnectionConfiguration;
import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.parseResponse;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test utilities to use with {@link ElasticsearchIO}. */
class ElasticSearchIOTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchIOTestUtils.class);

  static final String[] FAMOUS_SCIENTISTS = {
      "Einstein",
      "Darwin",
      "Copernicus",
      "Pasteur",
      "Curie",
      "Faraday",
      "Newton",
      "Bohr",
      "Galilei",
      "Maxwell"
  };
  static final int NUM_SCIENTISTS = FAMOUS_SCIENTISTS.length;

  /** Enumeration that specifies whether to insert malformed documents. */
  public enum InjectionMode {
    INJECT_SOME_INVALID_DOCS,
    DO_NOT_INJECT_INVALID_DOCS
  }

  /** Create the given index synchronously. */
  static void createIndex(ConnectionConfiguration connectionConfiguration,
      RestHighLevelClient restClient) throws IOException {
    try {
    CreateIndexResponse response = restClient.indices().create(new CreateIndexRequest
        (connectionConfiguration.getIndex()));
    } catch (ElasticsearchException e) {
      LOG.warn(e.getMessage());
    }
  }

  /** Deletes the given index synchronously. */
  static void deleteIndex(RestHighLevelClient restHighLevelClient, String index) throws IOException {
    try {
      DeleteIndexResponse response = restHighLevelClient.indices().delete(new DeleteIndexRequest
        (index));
    } catch (ElasticsearchException e) {
      LOG.warn(e.getMessage());
    }
  }

  /**
   * Synchronously deletes the target if it exists and then (re)creates it as a copy of source
   * synchronously.
   */
  static void copyIndex(RestHighLevelClient restClient, String source, String target) throws IOException {
    deleteIndex(restClient, target);
    HttpEntity entity =
        new NStringEntity(
            String.format(
                "{\"source\" : { \"index\" : \"%s\" }, \"dest\" : { \"index\" : \"%s\" } }",
                source, target),
            ContentType.APPLICATION_JSON);
    restClient.getLowLevelClient().performRequest("POST", "/_reindex", Collections.EMPTY_MAP, entity);
  }

  /** Inserts the given number of test documents into Elasticsearch. */
  static void insertTestDocuments(ConnectionConfiguration connectionConfiguration,
      long numDocs, RestHighLevelClient restHighLevelClient) throws IOException {
    RestClient restClient = restHighLevelClient.getLowLevelClient();
    List<DocWriteRequest> data =
        ElasticSearchIOTestUtils.createDocuments(
            numDocs, InjectionMode.DO_NOT_INJECT_INVALID_DOCS, connectionConfiguration.getIndex());
    StringBuilder bulkRequest = new StringBuilder();
    int i = 0;
    for (DocWriteRequest document : data) {
      bulkRequest.append(String.format(
          "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\", \"_id\" : \"%s\" } }%n%s%n",
          connectionConfiguration.getIndex(), connectionConfiguration.getType(), i++, document));
    }
    String endPoint = String.format("/%s/%s/_bulk", connectionConfiguration.getIndex(),
        connectionConfiguration.getType());
    HttpEntity requestBody =
        new NStringEntity(bulkRequest.toString(), ContentType.APPLICATION_JSON);
    Response response = restClient.performRequest("POST", endPoint,
        Collections.singletonMap("refresh", "true"), requestBody);
    ElasticsearchIO.checkForErrors(response);
  }

  /**
   * Forces a refresh of the given index to make recently inserted documents available for search.
   *
   * @return The number of docs in the index
   */
  static long refreshIndexAndGetCurrentNumDocs(
      ConnectionConfiguration connectionConfiguration,
      RestHighLevelClient restHighLevelClient) throws IOException {
    RestClient restClient = restHighLevelClient.getLowLevelClient();
    long result = 0;
    try {
      String endPoint = String.format("/%s/_refresh", connectionConfiguration.getIndex());
      restClient.performRequest("POST", endPoint);

      endPoint = String.format("/%s/%s/_search", connectionConfiguration.getIndex(),
          connectionConfiguration.getType());
      Response response = restClient.performRequest("GET", endPoint);
      JsonNode searchResult = ElasticsearchIO.parseResponse(response);
      result = searchResult.path("hits").path("total").asLong();
    } catch (IOException e) {
      // it is fine to ignore bellow exceptions because in testWriteWithBatchSize* sometimes,
      // we call upgrade before any doc have been written
      // (when there are fewer docs processed than batchSize).
      // In that cases index/type has not been created (created upon first doc insertion)
      if (!e.getMessage().contains("index_not_found_exception")) {
        throw e;
      }
    }
    return result;
  }

  /**
   * Generates a list of test documents for insertion.
   *
   * @param numDocs Number of docs to generate
   * @param injectionMode {@link InjectionMode} that specifies whether to insert malformed documents
   * @return the list of DocWriteRequests representing the documents
   */
  static List<DocWriteRequest> createDocuments(long numDocs, InjectionMode injectionMode,
      String indexName) {

    ArrayList<DocWriteRequest> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      int index = i % FAMOUS_SCIENTISTS.length;
      // insert 2 malformed documents
      if (InjectionMode.INJECT_SOME_INVALID_DOCS.equals(injectionMode) && (i == 6 || i == 7)) {
        IndexRequest doc = new IndexRequest(indexName, "_doc", Integer.toString(i));
        doc.source(String.format("{\"scientist\";\"%s\", \"id\":%s}",
            FAMOUS_SCIENTISTS[index], i), XContentType.JSON);
        data.add(doc);

      } else {
        IndexRequest doc = new IndexRequest(indexName, "_doc", Integer.toString(i));
        doc.source(String.format("{\"scientist\":\"%s\", \"id\":%s}",
            FAMOUS_SCIENTISTS[index], i), XContentType.JSON);
        data.add(doc);
      }
    }
    return data;
  }

  /**
   * Executes a query for the named scientist and returns the count from the result.
   *
   * @param connectionConfiguration Specifies the index and type
   * @param restClient To use to execute the call
   * @param scientistName The scientist to query for
   * @return The cound of documents found
   * @throws IOException On error talking to Elasticsearch
   */
  static long countByScientistName(
      ConnectionConfiguration connectionConfiguration, RestHighLevelClient restClient, String
      scientistName)
      throws IOException {
    return countByMatch(connectionConfiguration, restClient, "scientist", scientistName);
  }

  /**
   * Executes a match query for given field/value and returns the count of results.
   *
   * @param connectionConfiguration Specifies the index and type
   * @param restClient To use to execute the call
   * @param field The field to query
   * @param value The value to match
   * @return The count of documents in the search result
   * @throws IOException On error communicating with Elasticsearch
   */
  static long countByMatch(
      ConnectionConfiguration connectionConfiguration,
      RestHighLevelClient restClient,
      String field,
      String value)
      throws IOException {

    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery(field, value));
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = restClient.search(searchRequest);
    return searchResponse.getHits().totalHits;
  }
}
