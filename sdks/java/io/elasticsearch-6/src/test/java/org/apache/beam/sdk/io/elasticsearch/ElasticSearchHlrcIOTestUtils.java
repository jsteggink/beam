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

import static org.apache.beam.sdk.io.elasticsearch.ElasticsearchHlrcIO.ConnectionConfiguration;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
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
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test utilities to use with {@link ElasticsearchHlrcIO}. */
class ElasticSearchHlrcIOTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchHlrcIOTestUtils.class);

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
  static CreateIndexResponse createIndex(
      ConnectionConfiguration connectionConfiguration, RestHighLevelClient restHighLevelClient)
      throws IOException {
    return createIndex(connectionConfiguration, restHighLevelClient, Settings.EMPTY);
  }

  /** Create the given index synchronously with settings. */
  static CreateIndexResponse createIndex(
      ConnectionConfiguration connectionConfiguration, RestHighLevelClient restHighLevelClient,
      Settings settings)
      throws IOException {
    try {
      return
          restHighLevelClient.indices().create(new CreateIndexRequest(connectionConfiguration.getIndex(),
                  settings),
              RequestOptions.DEFAULT);
    } catch (ElasticsearchException e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }

  /** Deletes the given index synchronously. */
  static void deleteIndex(RestHighLevelClient restHighLevelClient, String index)
      throws IOException {
    try {
      DeleteIndexResponse response =
          restHighLevelClient.indices().delete(new DeleteIndexRequest(index),
              RequestOptions.DEFAULT);
    } catch (ElasticsearchException e) {
      LOG.warn(e.getMessage());
    }
  }

  /**
   * Synchronously deletes the target if it exists and then (re)creates it as a copy of source
   * synchronously.
   */
  static void copyIndex(RestHighLevelClient restHighLevelClient, String source, String target)
      throws IOException {
    deleteIndex(restHighLevelClient, target);
    HttpEntity entity =
        new NStringEntity(
            String.format(
                "{\"source\" : { \"index\" : \"%s\" }, \"dest\" : { \"index\" : \"%s\" } }",
                source, target),
            ContentType.APPLICATION_JSON);
    Request request = new Request("POST", "/_reindex");
    request.setEntity(entity);
    restHighLevelClient
        .getLowLevelClient()
        .performRequest(request);
  }

  /** Inserts the given number of test documents into Elasticsearch. */
  static void insertTestDocuments(
      ConnectionConfiguration connectionConfiguration,
      long numDocs,
      RestHighLevelClient restHighLevelClient)
      throws IOException {

    List<DocWriteRequest> data =
        ElasticSearchHlrcIOTestUtils.createDocuments(
            numDocs, InjectionMode.DO_NOT_INJECT_INVALID_DOCS, connectionConfiguration.getIndex());

    int i = 0;
    BulkRequest request = new BulkRequest();
    request.timeout("2m");
    for (DocWriteRequest docWriteRequest : data) {
      request.add(docWriteRequest);
    }

    BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);

    if(bulkResponse.hasFailures()) {
      LOG.error("Bulk had failures.");
    }
  }

  /**
   * Forces a refresh of the given index to make recently inserted documents available for search.
   *
   * @return The number of docs in the index
   */
  static long refreshIndexAndGetCurrentNumDocs(
      ConnectionConfiguration connectionConfiguration, RestHighLevelClient restHighLevelClient)
      throws IOException {

      RefreshRequest request = new RefreshRequest(connectionConfiguration.getIndex());

      restHighLevelClient.indices().refresh(request, RequestOptions.DEFAULT);

      SearchRequest searchRequest = new SearchRequest();
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.query(QueryBuilders.matchAllQuery());
      searchRequest.source(searchSourceBuilder);
      SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

      return searchResponse.getHits().getTotalHits();
  }

  /**
   * Generates a list of test documents for insertion.
   *
   * @param numDocs Number of docs to generate
   * @param injectionMode {@link InjectionMode} that specifies whether to insert malformed documents
   * @return the list of DocWriteRequests representing the documents
   */
  static List<DocWriteRequest> createDocuments(
      long numDocs, InjectionMode injectionMode, String indexName) throws IOException {

    ArrayList<DocWriteRequest> data = new ArrayList<>();
    for (int i = 0; i < numDocs; i++) {
      int index = i % FAMOUS_SCIENTISTS.length;
        IndexRequest doc = new IndexRequest(indexName, "_doc", Integer.toString(i));
        XContentBuilder builder = XContentFactory.jsonBuilder();
       builder.startObject();
       {
          builder.field("scientist", FAMOUS_SCIENTISTS[index]);
       }
       builder.endObject();
       doc.source(builder);
       data.add(doc);

       if (injectionMode == InjectionMode.INJECT_SOME_INVALID_DOCS) {
         doc = new IndexRequest(indexName, "_doc", Integer.toString(i));
         doc.source(String.format("{non working doc %s]", i));
         data.add(doc);
       }
      }

    return data;
  }

  /**
   * Executes a query for the named scientist and returns the count from the result.
   *
   * @param connectionConfiguration Specifies the index and type
   * @param restHighLevelClient To use to execute the call
   * @param scientistName The scientist to query for
   * @return The cound of documents found
   * @throws IOException On error talking to Elasticsearch
   */
  static long countByScientistName(
      ConnectionConfiguration connectionConfiguration,
      RestHighLevelClient restHighLevelClient,
      String scientistName)
      throws IOException {
    return countByMatch(connectionConfiguration, restHighLevelClient, "scientist", scientistName);
  }

  /**
   * Executes a match query for given field/value and returns the count of results.
   *
   * @param connectionConfiguration Specifies the index and type
   * @param restHighLevelClient To use to execute the call
   * @param field The field to query
   * @param value The value to match
   * @return The count of documents in the search result
   * @throws IOException On error communicating with Elasticsearch
   */
  static long countByMatch(
      ConnectionConfiguration connectionConfiguration,
      RestHighLevelClient restHighLevelClient,
      String field,
      String value)
      throws IOException {

    SearchRequest searchRequest = new SearchRequest();
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchQuery(field, value));
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    return searchResponse.getHits().totalHits;
  }
}
