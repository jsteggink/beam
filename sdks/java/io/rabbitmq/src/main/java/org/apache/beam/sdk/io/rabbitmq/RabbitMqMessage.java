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
package org.apache.beam.sdk.io.rabbitmq;

import com.rabbitmq.client.AMQP;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * It contains the message payload, and additional metadata like routing key or attributes. The main
 * reason of this class is that AMQP.BasicProperties doesn't provide a serializable public API.
 */
public class RabbitMqMessage implements Serializable {

  @Nullable private final String routingKey;
  private final byte[] body;
  private final String contentType;
  private final String contentEncoding;
  private final Map<String, Object> headers;
  private final Integer deliveryMode;
  private final Integer priority;
  @Nullable private final String correlationId;
  @Nullable private final String replyTo;
  private final String expiration;
  private final String messageId;
  private final Date timestamp;
  @Nullable private final String type;
  @Nullable private final String userId;
  @Nullable private final String appId;
  @Nullable private final String clusterId;

  public RabbitMqMessage(byte[] body) {
    this.body = body;
    routingKey = "";
    contentType = null;
    contentEncoding = null;
    headers = new HashMap<>();
    deliveryMode = 1;
    priority = 1;
    correlationId = null;
    replyTo = null;
    expiration = null;
    messageId = null;
    timestamp = new Date();
    type = null;
    userId = null;
    appId = null;
    clusterId = null;
  }

  public RabbitMqMessage(
      String routingKey,
      byte[] body,
      String contentType,
      String contentEncoding,
      Map<String, Object> headers,
      Integer deliveryMode,
      Integer priority,
      String correlationId,
      String replyTo,
      String expiration,
      String messageId,
      Date timestamp,
      String type,
      String userId,
      String appId,
      String clusterId) {
    this.routingKey = routingKey;
    this.body = body;
    this.contentType = contentType;
    this.contentEncoding = contentEncoding;
    this.headers = headers;
    this.deliveryMode = deliveryMode;
    this.priority = priority;
    this.correlationId = correlationId;
    this.replyTo = replyTo;
    this.expiration = expiration;
    this.messageId = messageId;
    this.timestamp = timestamp;
    this.type = type;
    this.userId = userId;
    this.appId = appId;
    this.clusterId = clusterId;
  }

  public String getRoutingKey() {
    return routingKey;
  }

  public byte[] getBody() {
    return body;
  }

  public String getContentType() {
    return contentType;
  }

  public String getContentEncoding() {
    return contentEncoding;
  }

  public Map<String, Object> getHeaders() {
    return headers;
  }

  public Integer getDeliveryMode() {
    return deliveryMode;
  }

  public Integer getPriority() {
    return priority;
  }

  public String getCorrelationId() {
    return correlationId;
  }

  public String getReplyTo() {
    return replyTo;
  }

  public String getExpiration() {
    return expiration;
  }

  public String getMessageId() {
    return messageId;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public String getType() {
    return type;
  }

  public String getUserId() {
    return userId;
  }

  public String getAppId() {
    return appId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public AMQP.BasicProperties createProperties() {
    return new AMQP.BasicProperties()
        .builder()
        .contentType(contentType)
        .contentEncoding(contentEncoding)
        .headers(headers)
        .deliveryMode(deliveryMode)
        .priority(priority)
        .correlationId(correlationId)
        .replyTo(replyTo)
        .expiration(expiration)
        .messageId(messageId)
        .timestamp(timestamp)
        .type(type)
        .userId(userId)
        .appId(appId)
        .clusterId(clusterId)
        .build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        routingKey,
        Arrays.hashCode(body),
        contentType,
        contentEncoding,
        deliveryMode,
        priority,
        correlationId,
        replyTo,
        expiration,
        messageId,
        timestamp,
        type,
        userId,
        appId,
        clusterId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RabbitMqMessage) {
      RabbitMqMessage other = (RabbitMqMessage) obj;
      if (routingKey == null
          && other.routingKey == null
          && body == null
          && other.body == null
          && contentType == null
          && other.contentType == null
          && contentEncoding == null
          && other.contentEncoding == null
          && deliveryMode == null
          && other.deliveryMode == null
          && priority == null
          && other.priority == null
          && correlationId == null
          && other.correlationId == null
          && replyTo == null
          && other.replyTo == null
          && expiration == null
          && other.expiration == null
          && messageId == null
          && other.messageId == null
          && timestamp == null
          && other.timestamp == null
          && type == null
          && other.type == null
          && userId == null
          && other.userId == null
          && appId == null
          && other.appId == null
          && clusterId == null
          && other.clusterId == null) {
        return true;
      } else {
        boolean equals = false;
        if (routingKey != null) {
          equals = routingKey.equals(other.routingKey);
        }
        if (body != null) {
          equals = Arrays.equals(body, other.body);
        }
        if (contentType != null) {
          equals = contentType.equals(other.contentType);
        }
        if (contentEncoding != null) {
          equals = contentEncoding.equals(other.contentEncoding);
        }
        if (deliveryMode != null) {
          equals = (deliveryMode.equals(other.deliveryMode));
        }
        if (priority != null) {
          equals = (priority.equals(other.priority));
        }
        if (correlationId != null) {
          equals = correlationId.equals(other.correlationId);
        }
        if (replyTo != null) {
          equals = replyTo.equals(other.replyTo);
        }
        if (expiration != null) {
          equals = expiration.equals(other.expiration);
        }
        if (messageId != null) {
          equals = messageId.equals(other.messageId);
        }
        if (timestamp != null) {
          equals = timestamp.equals(other.timestamp);
        }
        if (type != null) {
          equals = type.equals(other.type);
        }
        if (userId != null) {
          equals = userId.equals(other.userId);
        }
        if (appId != null) {
          equals = appId.equals(other.appId);
        }
        if (clusterId != null) {
          equals = clusterId.equals(other.clusterId);
        }
        return equals;
      }
    }
    return false;
  }
}
