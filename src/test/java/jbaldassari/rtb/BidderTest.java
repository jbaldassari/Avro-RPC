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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.avro.ipc.CallFuture;
import org.apache.avro.ipc.Callback;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the Bidder protocol.
 */
public class BidderTest {
  private static final int NUM_REQUESTS = 10;
  private static final CharSequence SNIPPET = new Utf8(
    "<a href=\"http://wikipedia.org\"><img src=\"" + 
    "http://upload.wikimedia.org/wikipedia/commons/b/b0/Qxz-ad39.png\" /></a>");
  private static final List<Server> servers = new LinkedList<Server>();
  private static final List<Transceiver> transceivers = 
    new LinkedList<Transceiver>();
  private static final BidRequest bidRequest;
  private static final Logger logger = Logger.getLogger(BidderTest.class);
  
  // Generate a bid request:
  static {
    bidRequest = new BidRequest();
    bidRequest.auctionId = new Utf8("Auction 1");
    Dimension dimension = new Dimension();
    dimension.width = 480;
    dimension.height = 60;
    bidRequest.dimension = dimension;
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
   * Starts a server.
   * @param bidder the server-side Bidder implementation to use.
   * @return a Bidder.Callback client to use to communicate with the server.
   * @throws IOException if an error occurs initializing the client.
   */
  private Bidder.Callback startServer(Bidder bidder) throws IOException {
    // Initialize responder:
    Responder responder = 
      new SpecificResponder(Bidder.class, bidder);
    
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
  
  @Test
  public void sendBidRequestWithCallback() throws Exception {    
    // Initialize a bidder proxy instance, which connects to the server:
    Bidder.Callback bidder = startServer(new RandomBidder(SNIPPET));
    
    // Send bid requests, inserting the results asynchronously into a queue:
    final Queue<BidResponse> responses = new LinkedBlockingQueue<BidResponse>();
    final CountDownLatch responsesLatch = new CountDownLatch(NUM_REQUESTS);
    for (int ii = 0; ii < NUM_REQUESTS; ii++) {
      bidder.bid(bidRequest, new Callback<BidResponse>() {
        @Override
        public void handleResult(BidResponse bidResponse) {
          responses.add(bidResponse);
          responsesLatch.countDown();
        }
        @Override
        public void handleError(Throwable t) {
          responsesLatch.countDown();
        }
      });
    }
    
    // Wait for all requests to complete:
    Assert.assertTrue(responsesLatch.await(10, TimeUnit.SECONDS));
    Assert.assertEquals(NUM_REQUESTS, responses.size());
    for (BidResponse bidResponse : responses) {
      validateBidResponse(bidRequest, bidResponse);
    }
  }
  
  @Test
  public void sendBidRequestWithFuture() throws Exception {
    // Initialize a bidder proxy instance, which connects to the server:
    Bidder.Callback bidder = startServer(new RandomBidder(SNIPPET));
    
    // Send bid requests, using a CallFuture to wait for each result:
    for (int ii = 0; ii < NUM_REQUESTS; ii++) {
      CallFuture<BidResponse> bidResponseFuture = new CallFuture<BidResponse>();
      bidder.bid(bidRequest, bidResponseFuture);
      BidResponse bidResponse = bidResponseFuture.get(10, TimeUnit.SECONDS);
      validateBidResponse(bidRequest, bidResponse);
    }
  }
  
  @Test
  public void runAuction() throws Exception {
    // Start servers for each participant in the auction
    // Each server will have a specific constant bid and delay associated
    // with it.
    Map<CharSequence, Bidder.Callback> bidders = 
      new LinkedHashMap<CharSequence, Bidder.Callback>();
    for (int ii = 0; ii < NUM_REQUESTS; ii++) {
      CharSequence bidderId = new Utf8("Bidder " + (ii + 1));
      long constantBid = 100000L * (ii + 1);
      long delayMillis = 100 * (ii + 1);
      logger.info("Creating bidder " + bidderId + " with constant bid " + 
          constantBid + " micro-dollars CPM and delay of " + 
          delayMillis + "ms");
      bidders.put(bidderId, startServer(new DelayInjectingBidder(
          new ConstantBidder(SNIPPET, constantBid), delayMillis)));
    }
    
    // Start an auction with a timeout smaller than the delay configured for 
    // some bidders.
    // With a delay of 580ms, only bidders 1-5 should respond in time.
    // Bidder 5 should always have the highest bid equal to $0.50 CPM
    Auction auction = new Auction(bidRequest, bidders, 
        580, TimeUnit.MILLISECONDS);
    AuctionResult result = auction.call();
    Assert.assertEquals(bidRequest.auctionId, result.auctionId);
    Assert.assertTrue(result.isWon);
    Assert.assertEquals(new Utf8("Bidder 5"), result.winningBidderId);
    validateBidResponse(bidRequest, result.winningBid);
    
    // Wait for the rest of the RPCs to complete so that the connections 
    // can be torn down gracefully:
    Thread.sleep(2000L);
  }
  
  /**
   * Validates a BidResponse.
   * @param bidRequest the BidRequest that initiated the response.
   * @param bidResponse the BidResponse to validate.
   */
  private void validateBidResponse(BidRequest bidRequest, 
      BidResponse bidResponse) {
    Assert.assertNotNull(bidResponse);
    if (bidResponse.maxBidMicroCpm > 0) {
      Assert.assertEquals(SNIPPET, bidResponse.creativeSnippet);
    }
    else {
      Assert.assertNull(bidResponse.creativeSnippet);
    }
  }
}
