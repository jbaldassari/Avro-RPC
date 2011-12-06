/**
 * Copyright 2011 James Baldassari
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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jbaldassari.rtb;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.junit.After;
import org.junit.Assert;

/**
 * Abstract base class for Bidder tests.
 */
abstract public class AbstractBidderTest {
  protected static final CharSequence SNIPPET = new Utf8(
      "<a href=\"http://wikipedia.org\"><img src=\"" + 
      "http://upload.wikimedia.org/wikipedia/commons/b/b0/Qxz-ad39.png\"" +
      " /></a>");
  private static final List<Server> servers = new LinkedList<Server>();
  private static final List<Transceiver> transceivers = 
    new LinkedList<Transceiver>();
  protected static final BidRequest bidRequest;

  // Generate a bid request:
  static {
    bidRequest = BidRequest.newBuilder().
        setAuctionId(new Utf8("Auction 1")).
        setDimension(Dimension.newBuilder().setWidth(480).setHeight(60).build()).
        build();
  }

  @After
  public void tearDown() throws Exception {
    // Close all Transceivers:
    for (Transceiver transceiver : transceivers) {
      transceiver.close();
    }
    transceivers.clear();

    // Close all Servers:
    for (Server server : servers) {
      server.close();
    }
    servers.clear();
  }

  /**
   * Starts a server, and returns a client for executing RPCs on that server.
   * @param bidder the server-side Bidder implementation to use.
   * @return a Bidder.Callback client to use to communicate with the server.
   * @throws IOException if an error occurs initializing the client.
   */
  protected Bidder.Callback startServerAndGetClient(Bidder bidder) throws 
    IOException {
    // Initialize responder:
    Responder responder = new SpecificResponder(Bidder.class, bidder);

    // Initialize/start server:
    Server server = new NettyServer(responder, new InetSocketAddress(0));
    servers.add(server);
    server.start();

    // Create the client, and connect to the server:
    Transceiver transceiver = 
        new NettyTransceiver(new InetSocketAddress(server.getPort()));
    transceivers.add(transceiver);
    Bidder.Callback client = SpecificRequestor.getClient(
        Bidder.Callback.class, transceiver);

    // Ping the server, which performs the initial client-server handshake 
    // (the first RPC always performs this handshake synchronously) and 
    // verifies that communication is working properly:
    Assert.assertTrue(client.ping());
    return client;
  }
  
  /**
   * Validates a BidResponse.
   * @param bidRequest the BidRequest that initiated the response.
   * @param bidResponse the BidResponse to validate.
   */
  protected void validateBidResponse(BidRequest bidRequest, 
      BidResponse bidResponse) {
    Assert.assertNotNull(bidResponse);
    if (bidResponse.getMaxBidMicroCpm() > 0) {
      Assert.assertEquals(SNIPPET, bidResponse.getCreativeSnippet());
    }
    else {
      Assert.assertNull(bidResponse.getCreativeSnippet());
    }
  }
}
