/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.firehose.zeromq;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A configuration object for a ZeroMQ connection.
 */
public class ZeroMQFirehoseConfig
{
  public static ZeroMQFirehoseConfig makeDefaultConfig()
  {
    return new ZeroMQFirehoseConfig("data", "tcp://localhost:5556");
  }

  private final String uri;
  private final String filter;

  @JsonCreator
  public ZeroMQFirehoseConfig(
      @JsonProperty("filter") String filter,
      @JsonProperty("uri") String uri
  )
  {
    this.filter = filter;
    this.uri = uri;
  }

  @JsonProperty
  public String getFilter()
  {
    return filter;
  }

  @JsonProperty
  public String getUri()
  {
    return uri;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ZeroMQFirehoseConfig that = (ZeroMQFirehoseConfig) o;

    if (!filter.equals(that.filter)) {
      return false;
    }
    if (!uri.equals(that.uri)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = uri != null ? uri.hashCode() : 0;
    result = 31 * result + (filter != null ? filter.hashCode() : 0);
    return result;
  }
}
