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
import com.metamx.common.logger.Logger;
import com.metamx.common.StringUtils;
import org.zeromq.ZMQ;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

  @Override
  public Firehose connect(StringInputRowParser firehoseParser) throws IOException
  {
    final StringInputRowParser stringParser = firehoseParser;

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
          // Wait for the next data. This will block until something is available.
          // Use trim to remove the trailing '0'
          data = subscriber.recvStr(0).trim()
          if (data != null) {
            return true;
          }
        }
        catch (InterruptedException e) {
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

        // TODO: we may need to adjust the format of the string to be something that can be used by the
        // StringInputRowParser this assumes the raw queue data is in the format that StringInputRowParser likes
        return stringParser.parse(StringUtils.fromUtf8(data));
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
