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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;

/**
 * A {@link Coder} that serializes and deserializes the {@link DocWriteRequest} objects. }.
 */
class ESDocWriteRequestCoder extends AtomicCoder<DocWriteRequest> implements Serializable {
  private static final ESDocWriteRequestCoder INSTANCE = new ESDocWriteRequestCoder();

  private ESDocWriteRequestCoder() {}

  public static ESDocWriteRequestCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(DocWriteRequest docWriteRequest, OutputStream outStream) throws IOException {
    BytesStreamOutput streamOutput = new BytesStreamOutput();
    DocWriteRequest.writeDocumentRequest(streamOutput, docWriteRequest);
    streamOutput.bytes().writeTo(outStream);
  }

  @Override
  public DocWriteRequest decode(InputStream inStream) throws IOException {
    StreamInput streamInput = new InputStreamStreamInput(inStream);
    return DocWriteRequest.readDocumentRequest(streamInput);
  }

  /**
   * Returns a {@link CoderProvider} which uses the {@link ESDocWriteRequestCoder}
   * for {@link DocWriteRequest docWriteRequests}.
   */
  static CoderProvider getCoderProvider() {
    return ES_DOCWRITEREQUEST_CODER_PROVIDER;
  }

  private static final CoderProvider ES_DOCWRITEREQUEST_CODER_PROVIDER =
      new ESDocWriteRequestCoderProvider();

  /** A {@link CoderProvider} for {@link DocWriteRequest docWriteRequests}. */
  private static class ESDocWriteRequestCoderProvider extends CoderProvider {
    @Override
    public <T> Coder<T> coderFor(
        TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!typeDescriptor.isSubtypeOf(ES_DOCWRITEREQUEST_TYPE_DESCRIPTOR)) {
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide %s because %s is not a subclass of %s",
                ESDocWriteRequestCoder.class.getSimpleName(),
                typeDescriptor,
                DocWriteRequest.class.getName()));
      }

      try {
        @SuppressWarnings("unchecked")
        Coder<T> coder = (Coder<T>) ESDocWriteRequestCoder.of();
        return coder;
      } catch (IllegalArgumentException e) {
        throw new CannotProvideCoderException(e);
      }
    }
  }

  private static final TypeDescriptor<DocWriteRequest> ES_DOCWRITEREQUEST_TYPE_DESCRIPTOR =
      new TypeDescriptor<DocWriteRequest>() {};
}
