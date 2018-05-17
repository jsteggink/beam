package org.apache.beam.sdk.io.elasticsearch;

import java.io.IOException;
import java.util.List;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;

/**
 * This client is needed to reuse the RestClient generated in ESIntTestCase.
 */
public class RestHighLevelTestClient extends RestHighLevelClient{

  protected RestHighLevelTestClient(RestClient restClient,
      CheckedConsumer<RestClient, IOException> doClose,
      List<NamedXContentRegistry.Entry> namedXContentEntries) {
    super(restClient, doClose, namedXContentEntries);
  }
}
