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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.common.StringUtils;

import org.zeromq.*;

import com.metamx.common.parsers.ParseException;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;
import jdk.nashorn.internal.parser.JSONParser;

import java.io.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;

/**
 * A FirehoseFactory for ZeroMQ.
 * <p/>
 * It will receive it's configuration through the realtime.spec file and expects to find a
 * consumerProps element in the firehose definition with values for a number of configuration options.
 * Below is a complete example for a ZeroMQ firehose configuration with some explanation. Options
 * that have defaults can be skipped but options with no defaults must be specified with the exception
 * of the URI property. If the URI property is set, it will override any other property that was also
 * set.
 * <p/>
 * File: <em>realtime.spec</em>
 * <pre>
 *   "firehose" : {
 *     "type" : "zeromq",
 *     "config" : {
 *       "filter": "data",                    # The filter to subscribe to
 *       "uri" : "tcp://localhost:5556",      # The queue uri to connect to
 *       "field_map" : "",                    # The path to a field map file see constants.json for an example
 *     },
 *     "parser" : {
 *       "timestampSpec" : { "column" : "utcdt", "format" : "iso" },
 *       "data" : { "format" : "json" },
 *       "dimensionExclusions" : ["wp"]
 *     }
 *   },
 * </pre>
 * <p/>
 * <b>Limitations:</b> This implementation will not attempt to reconnect to the MQ broker if the
 * connection to it is lost. Furthermore it does not support any automatic failover on high availability
 * ZeroMQ clusters. This is not supported by the underlying AMQP client library and while the behavior
 * could be "faked" to some extent we haven't implemented that yet. However, if a policy is defined in
 * the ZeroMQ cluster that sets the "ha-mode" and "ha-sync-mode" properly on the queue that this
 * Firehose connects to, messages should survive an MQ broker node failure and be delivered once a
 * connection to another node is set up.
 * <p/>
 * For more information on ZeroMQ high availability please see:
 * <a href="http://www.zeromq.com/ha.html">http://www.zeromq.com/ha.html</a>.
 */
public class ZeroMQFirehoseFactory implements FirehoseFactory<StringInputRowParser>
{
  private static final Logger log = new Logger(ZeroMQFirehoseFactory.class);

  private ZMQ.Context context;
  private ZMQ.Socket subscriber;
  private final ZeroMQFirehoseConfig config;

  @JsonCreator
  public ZeroMQFirehoseFactory(
      @JsonProperty("config") ZeroMQFirehoseConfig config
  ) throws Exception
  {
    this.config = config == null ? ZeroMQFirehoseConfig.makeDefaultConfig() : config;

  }

  @JsonProperty
  public ZeroMQFirehoseConfig getConfig()
  {
    return config;
  }

  private static final Function<JsonNode, Object> valueFunction = new Function<JsonNode, Object>()
  {
    @Override
    public Object apply(JsonNode node)
    {
      if (node == null || node.isMissingNode() || node.isNull()) {
        return null;
      }
      if (node.isIntegralNumber()) {
        if (node.canConvertToLong()) {
          return node.asLong();
        } else {
          return node.asDouble();
        }
      }
      if (node.isFloatingPointNumber()) {
        return node.asDouble();
      }

      if (node.isObject())
      {
        return jsonNodeToMap(node);
      }

      final String s = node.asText();
      final CharsetEncoder enc = Charsets.UTF_8.newEncoder();
      if (s != null && !enc.canEncode(s)) {
        // Some whacky characters are in this string (e.g. \uD900). These are problematic because they are decodeable
        // by new String(...) but will not encode into the same character. This dance here will replace these
        // characters with something more sane.
        return new String(s.getBytes(Charsets.UTF_8), Charsets.UTF_8);
      } else {
        return s;
      }
    }
  };

  private static Map<String, Object> jsonNodeToMap(JsonNode root)
  {
    Map<String, Object> map = new LinkedHashMap<String, Object>();
    Iterator<String> keysIter = root.fieldNames();

    while (keysIter.hasNext()) {
      String key = keysIter.next();

      JsonNode node = root.path(key);

      if (node.isArray()) {
        final List<Object> nodeValue = Lists.newArrayListWithExpectedSize(node.size());
        for (final JsonNode subnode : node) {
          final Object subnodeValue = valueFunction.apply(subnode);
          if (subnodeValue != null) {
            nodeValue.add(subnodeValue);
          }
        }
        map.put(key, nodeValue);
      } else {
        final Object nodeValue = valueFunction.apply(node);
        if (nodeValue != null) {
          map.put(key, nodeValue);
        }
      }
    }
    return map;
  }

  private Map<String, Object> parseFieldMap(String input)
  {
    ObjectMapper objectMapper = new ObjectMapper();

    try {
      JsonNode root = objectMapper.readTree(input);

      return jsonNodeToMap(root);
    }
    catch (Exception e) {
      throw new ParseException(e, "Unable to parse row [%s]", input);
    }
  }

  @Override
  public Firehose connect(StringInputRowParser firehoseParser) throws IOException
  {
    Map<String, String> rekeyMap = new HashMap<String, String>();

    if (!config.getFieldMap().isEmpty())
    {
      File fieldMapFile = new File(config.getFieldMap());

      byte[] encoded = Files.readAllBytes(Paths.get(config.getFieldMap()));
      String fieldMapString = new String(encoded, Charset.defaultCharset());

      Map<String, Object> fieldMap = parseFieldMap(fieldMapString);

      for (String key : fieldMap.keySet())
      {
        Object fieldObj = fieldMap.get(key);

        if (fieldObj instanceof Map)
        {
          try
          {
            Map<String, Object> field = (Map<String, Object>) fieldObj;
            rekeyMap.put(key, (String)field.get("name"));
          }
          catch(ClassCastException ignore)
          {
            // Bad format? ignore
          }
        }
      }
    }

    final RekeyStringInputRowParser rekeyStringParser = new RekeyStringInputRowParser(
       firehoseParser.getParseSpec(),
       rekeyMap
    );

    context = ZMQ.context(1);
    subscriber = context.socket(ZMQ.SUB);
    subscriber.connect(config.getUri());

    subscriber.subscribe(config.getFilter().getBytes());

    return new Firehose()
    {
      private String data;

      @Override
      public boolean hasMore()
      {
        try {
          // Gets the entire message, including all frames
          ZMsg msg = ZMsg.recvMsg(subscriber);
          ZFrame lastFrame = msg.getLast();

          data = lastFrame.toString();

          if (!data.equals("")) {
            return true;
          }
        }
        catch (RuntimeException e) {
          // A little unclear on how we should handle this.

          // At any rate, we're in an unknown state now so let's log something and return false.
          log.wtf(e, "Got interrupted while waiting for next data message. Doubt this should ever happen.");
        }

        // This means that data is null or we caught the exception above so we report that we have
        // nothing more to process.
        return false;
      }

      @Override
      public InputRow nextRow()
      {
        if (data == null) {
          //Just making sure.
          log.wtf("I have nothing in data. Method hasMore() should have returned false.");
          return null;
        }

        return rekeyStringParser.parse(StringUtils.fromUtf8(data.getBytes()));
      }

      @Override
      public Runnable commit()
      {
        // This method will be called from the same thread that calls the other methods of
        // this Firehose. However, the returned Runnable will be called by a different thread.
        return new Runnable()
        {
          @Override
          public void run()
          {
            // Non-op we have no need to acknowledge
          }
        };
      }

      @Override
      public void close() throws IOException
      {
        log.info("Closing connection to ZeroMQ");
        subscriber.close();
        context.term();
      }
    };
  }

}
