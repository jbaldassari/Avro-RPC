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
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.avro.ipc.Callback;
import org.apache.log4j.Logger;

/**
 * Conducts an action for an ad slot.
 * Each Auction instance can only be used to conduct a single auction.
 */
public class Auction implements Callable<AuctionResult> {
  private static final Logger logger = Logger.getLogger(Auction.class);
  private static final Random random = new Random();
  private final BidRequest bidRequest;
  private final Map<CharSequence, Bidder.Callback> bidders;
  private final long timeout;
  private final TimeUnit timeoutUnit;
  private final CountDownLatch auctionLatch;

  /**
   * Creates an auction.
   * @param bidRequest the bid request to send to all bidders.
   * @param bidders the participants in the auction (map of bidder ID to 
   * Bidder instance).
   * @param timeout amount of time to wait for bids from all participants.
   * @param timeoutUnit unit of the timeout parameter.
   */
  public Auction(
      BidRequest bidRequest, 
      Map<CharSequence, Bidder.Callback> bidders, 
      long timeout, TimeUnit timeoutUnit) {
    
    if (bidRequest == null) {
      throw new NullPointerException("bidRequest is null");
    }
    if (bidders == null) {
      throw new NullPointerException("bidders is null");
    }
    if (bidders.size() == 0) {
      throw new IllegalArgumentException("Auction has zero bidders");
    }
    if (timeout <= 0) {
      throw new IllegalArgumentException("timeout must be > 0");
    }
    if (timeoutUnit == null) {
      throw new NullPointerException("timeoutUnit is null");
    }
    
    this.bidRequest = bidRequest;
    this.bidders = bidders;
    this.timeout = timeout;
    this.timeoutUnit = timeoutUnit;
    auctionLatch = new CountDownLatch(bidders.size());
  }

  /**
   * Conducts the auction, and returns the winner.
   * @return the result of the auction.
   * @throws IOException if an error occurs executing an RPC.
   * @throws InterruptedException if interrupted while waiting for bids.
   */
  public AuctionResult call() throws IOException, InterruptedException {
    AuctionResult.Builder result = 
        AuctionResult.newBuilder().setAuctionId(bidRequest.getAuctionId());

    // Create map of bid amount to map of bidder ID to BidResponse
    ConcurrentNavigableMap<Long, ConcurrentMap<CharSequence, BidResponse>> bids 
      = new ConcurrentSkipListMap<Long, ConcurrentMap<CharSequence, BidResponse>>();

    // Send bid requests to all participants:
    for (Map.Entry<CharSequence, Bidder.Callback> entry : bidders.entrySet()) {
      Bidder.Callback bidder = entry.getValue();
      Callback<BidResponse> callback = new AuctionCallback(entry.getKey(), bids);
      bidder.bid(bidRequest, callback);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("All bid requests for auction \"" + 
          bidRequest.getAuctionId() + "\" have been sent.");
    }

    // Wait for bidders to bid:
    auctionLatch.await(timeout, timeoutUnit);
    if (logger.isDebugEnabled()) {
      logger.debug("Auction \"" + bidRequest.getAuctionId() + "\" is over.");
    }

    // Pick the highest non-zero bid:
    if (!bids.isEmpty()) {
      Map.Entry<Long, ConcurrentMap<CharSequence, BidResponse>> highestBid = 
        bids.lastEntry();
      long maxBidMicroCpm = highestBid.getKey();
      if (maxBidMicroCpm > 0) {
        // At least one valid bid
        Map<CharSequence, BidResponse> winners = highestBid.getValue();
        CharSequence winnerId = null;
        BidResponse winningBid = null;

        // If we have a tie, choose randomly:
        if (winners.size() > 1) {
          winnerId = new ArrayList<CharSequence>(
              winners.keySet()).get(random.nextInt(winners.size()));
          winningBid = winners.get(winnerId);
        }
        else if (winners.size() == 1) {
          Map.Entry<CharSequence, BidResponse> winnerEntry = 
            winners.entrySet().iterator().next();
          winnerId = winnerEntry.getKey();
          winningBid = winnerEntry.getValue();
        }

        if (winnerId != null) {
          // Set the appropriate fields in the AuctionResult:
          result.setIsWon(true);
          result.setWinningBidderId(winnerId);
          result.setWinningBid(winningBid);

          // Send win/loss notifications:
          for (Map.Entry<CharSequence, Bidder.Callback> bidder : 
            bidders.entrySet()) {
            // Build the notification:
            Notification.Builder notification = Notification.newBuilder().
                setAuctionId(bidRequest.getAuctionId()).
                setNotificationType(NotificationType.LOSS);
            
            // Note: Notification has the following default values:
            // type = LOSS
            // winPriceMicroCpm = 0
            
            if (result.getIsWon()) {
              // There was a winner in this auction, so set the winning price:
              notification.setWinPriceMicroCpm(winningBid.getMaxBidMicroCpm());
              
              // Was the current bidder the winner?
              if (bidder.getKey().equals(winnerId)) {
                notification.setNotificationType(NotificationType.WIN);
              }
            }
            
            // Send the notification to the bidder:
            bidder.getValue().notify(notification.build());
          }
        }
      }
    }

    // Return the auction result:
    return result.build();
  }

  /**
   * Handles bid responses from an Auction.
   */
  private class AuctionCallback implements Callback<BidResponse> {
    private final long startTimeNanos = System.nanoTime();
    private final CharSequence bidderId;
    private final ConcurrentMap<Long, 
    ConcurrentMap<CharSequence, BidResponse>> bids;

    /**
     * Creates an AuctionCallback.
     * @param bidderId the ID of the bidder associated with this AuctionCallback.
     * @param bids map into which the bid response should be inserted.
     */
    public AuctionCallback(CharSequence bidderId, 
        ConcurrentMap<Long, ConcurrentMap<CharSequence, BidResponse>> bids) {
      if (bidderId == null) {
        throw new NullPointerException("bidderId is null");
      }
      if (bids == null) {
        throw new NullPointerException("bids is null");
      }
      this.bidderId = bidderId;
      this.bids = bids;
    }

    @Override
    public void handleError(Throwable t) {
      logger.error("Bidder " + bidderId + " threw error in auction " + 
          bidRequest.getAuctionId(), t);
      auctionLatch.countDown();	// Always count down the latch
    }

    @Override
    public void handleResult(BidResponse bidResponse) {
      if (logger.isDebugEnabled()) {
        double durationMillis = (System.nanoTime() - startTimeNanos) / 1e6d;
        logger.debug("Bidder " + bidderId + " returned bid in " + 
            durationMillis + "ms: " + bidResponse);
      }

      // Insert the bid into the map:
      if (!bids.containsKey(bidResponse.getMaxBidMicroCpm())) {
        bids.putIfAbsent(bidResponse.getMaxBidMicroCpm(), 
            new ConcurrentHashMap<CharSequence, BidResponse>(1));
      }
      bids.get(bidResponse.getMaxBidMicroCpm()).put(bidderId, bidResponse);

      auctionLatch.countDown();	// Always count down the latch
    }
  }
}
