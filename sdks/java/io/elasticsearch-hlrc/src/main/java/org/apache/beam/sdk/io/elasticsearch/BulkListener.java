package org.apache.beam.sdk.io.elasticsearch;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class BulkListener implements BulkProcessor.Listener {

  private static final Logger LOG = LoggerFactory.getLogger(BulkListener.class);

  @Override public void beforeBulk(long executionId, BulkRequest request) {
    int numberOfActions = request.numberOfActions();
    LOG.debug("Executing bulk [{}] with {} requests:"+executionId+","+numberOfActions);
  }

  @Override public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    if (response.hasFailures()) {
      LOG.warn("Bulk [{}] executed with failures:" + executionId);
    } else {
      LOG.debug("Bulk [{}] completed in {} milliseconds:" + executionId + "," + response.getTook().getMillis());
    }
  }

  @Override public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    LOG.error("Failed to execute bulk:", failure);
  }
}
