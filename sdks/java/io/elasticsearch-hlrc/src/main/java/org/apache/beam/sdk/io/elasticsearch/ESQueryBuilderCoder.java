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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;

/** A {@link Coder} that serializes and deserializes the {@link QueryBuilder} objects. }. */
class ESQueryBuilderCoder extends AtomicCoder<QueryBuilder> implements Serializable {
  private static final ESQueryBuilderCoder INSTANCE = new ESQueryBuilderCoder();

  private ESQueryBuilderCoder() {}

  public static ESQueryBuilderCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(QueryBuilder queryBuilder, OutputStream outStream) throws IOException {
    BytesStreamOutput streamOutput = new BytesStreamOutput();
    queryBuilder.writeTo(streamOutput);
    streamOutput.bytes().writeTo(outStream);
  }

  @Override
  public QueryBuilder decode(InputStream inStream) throws IOException {
    StreamInput streamInput = new InputStreamStreamInput(inStream);
    return new WrapperQueryBuilder(streamInput);
  }

  /**
   * Returns a {@link CoderProvider} which uses the {@link ESQueryBuilderCoder} for {@link
   * QueryBuilder queryBuilder}.
   */
  static CoderProvider getCoderProvider() {
    return QUERYBUILDER_CODER_PROVIDER;
  }

  private static final CoderProvider QUERYBUILDER_CODER_PROVIDER =
      new QueryBuilderCoderProvider();

  /** A {@link CoderProvider} for {@link QueryBuilder queryBuilder}. */
  private static class QueryBuilderCoderProvider extends CoderProvider {
    @Override
    public <T> Coder<T> coderFor(
        TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!typeDescriptor.isSubtypeOf(QUERYBUILDER_TYPE_DESCRIPTOR)) {
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide %s because %s is not a subclass of %s",
                ESQueryBuilderCoder.class.getSimpleName(),
                typeDescriptor,
                QueryBuilder.class.getName()));
      }

      try {
        @SuppressWarnings("unchecked")
        Coder<T> coder = (Coder<T>) ESQueryBuilderCoder.of();
        return coder;
      } catch (IllegalArgumentException e) {
        throw new CannotProvideCoderException(e);
      }
    }
  }

  private static final TypeDescriptor<QueryBuilder> QUERYBUILDER_TYPE_DESCRIPTOR =
      new TypeDescriptor<QueryBuilder>() {};
}
