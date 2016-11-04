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
package org.apache.flume.source.http;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.JSONEvent;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Base class used to encapsulate configuration code shared between {@linkplain TestHTTPSource} and {@linkplain TestCustomHTTPSource}.
 */
public class AbstractTestHTTPSource {
    protected static HTTPSource source;
    protected static HTTPSource httpsSource;
    protected static Channel channel;
    protected static int selectedPort;
    protected static int sslPort;
    protected DefaultHttpClient httpClient;
    protected HttpPost postRequest;

    private static int findFreePort() throws IOException {
      ServerSocket socket = new ServerSocket(0);
      int port = socket.getLocalPort();
      socket.close();
      return port;
    }

    protected static void doSetup(HTTPSource externalHttpSource, HTTPSource externalHttpsSource) throws Exception{
      selectedPort = findFreePort();
  
      source = externalHttpSource;
      channel = new MemoryChannel();
  
      httpsSource = externalHttpsSource;
      httpsSource.setName("HTTPS Source");
  
      Context ctx = new Context();
      ctx.put("capacity", "100");
      Configurables.configure(channel, ctx);
  
      List<Channel> channels = new ArrayList<Channel>(1);
      channels.add(channel);
  
      ChannelSelector rcs = new ReplicatingChannelSelector();
      rcs.setChannels(channels);
  
      source.setChannelProcessor(new ChannelProcessor(rcs));
  
      channel.start();
  
      httpsSource.setChannelProcessor(new ChannelProcessor(rcs));
  
      // HTTP context
      Context context = new Context();
  
      context.put("port", String.valueOf(selectedPort));
      context.put("host", "0.0.0.0");
  
      // SSL context props
      Context sslContext = new Context();
      sslContext.put(HTTPSourceConfigurationConstants.SSL_ENABLED, "true");
      sslPort = findFreePort();
      sslContext.put(HTTPSourceConfigurationConstants.CONFIG_PORT,
              String.valueOf(sslPort));
      sslContext.put(HTTPSourceConfigurationConstants.SSL_KEYSTORE_PASSWORD, "password");
      sslContext.put(HTTPSourceConfigurationConstants.SSL_KEYSTORE,
              "src/test/resources/jettykeystore");
  
      Configurables.configure(source, context);
      Configurables.configure(httpsSource, sslContext);
      source.start();
      httpsSource.start();
    }

    protected ResultWrapper putWithEncoding(String encoding, int n) throws Exception {
      Type listType = new TypeToken<List<JSONEvent>>() {}.getType();
      List<JSONEvent> events = Lists.newArrayList();
      Random rand = new Random();
      for (int i = 0; i < n; i++) {
        Map<String, String> input = Maps.newHashMap();
        for (int j = 0; j < 10; j++) {
          input.put(String.valueOf(i) + String.valueOf(j), String.valueOf(i));
        }
        JSONEvent e = new JSONEvent();
        e.setHeaders(input);
        e.setBody(String.valueOf(rand.nextGaussian()).getBytes(encoding));
        events.add(e);
      }
      Gson gson = new Gson();
      String json = gson.toJson(events, listType);
      StringEntity input = new StringEntity(json);
      input.setContentType("application/json; charset=" + encoding);
      postRequest.setEntity(input);
      HttpResponse resp = httpClient.execute(postRequest);
      return new ResultWrapper(resp, events);
    }

    protected class ResultWrapper {
      public final HttpResponse response;
      public final List<JSONEvent> events;
  
      public ResultWrapper(HttpResponse resp, List<JSONEvent> events) {
        this.response = resp;
        this.events = events;
      }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        source.stop();
        channel.stop();
        httpsSource.stop();
    }

    @Before
    public void setUp() {
        httpClient = new DefaultHttpClient();
        postRequest = new HttpPost("http://0.0.0.0:" + selectedPort);
    }
}
