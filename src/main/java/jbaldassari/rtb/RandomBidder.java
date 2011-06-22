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

import java.util.Random;

/**
 * A Bidder that generates random bids.
 */
public class RandomBidder extends AbstractBidder {
  private static final float DEFAULT_BID_PROBABILITY = 0.5f;
  private static final long DEFAULT_MIN_BID = 10000; // $0.10 CPM
  private static final long DEFAULT_MAX_BID = 10000000; // $10.0 CPM
  private static final Random random = new Random();
  private final float bidProbability;
  private final long minBid;
  private final long maxBid;
  
  /**
   * Creates a RandomBidder with an auto-generated ID and default settings.
   * @param snippet the creative snippet to return in the each non-zero bid.
   */
  public RandomBidder(CharSequence snippet) {
    this(snippet, DEFAULT_BID_PROBABILITY, DEFAULT_MIN_BID, DEFAULT_MAX_BID);
  }

  /**
   * Creates a RandomBidder with an auto-generated ID.
   * @param snippet the creative snippet to return in the each non-zero bid.
   * @param bidProbability value in the range [0, 1] that determines how often 
   * we bid a value greater than 0.
   * @param minBid the minimum bid we can return if we decide to bid.
   * @param maxBid the maximum bid we can return if we decide to bid.
   */
  public RandomBidder(CharSequence snippet, float bidProbability,
      long minBid, long maxBid) {
    super(snippet);
    if ((bidProbability < 0) || (bidProbability > 1)) {
      throw new IllegalArgumentException(
          "bidProbability " + bidProbability + " outside valid range [0, 1]");
    }
    if (minBid < 1) {
      throw new IllegalArgumentException("minBid must be >= 1");
    }
    if (maxBid < minBid) {
      throw new IllegalArgumentException("maxBid must be >= minBid");
    }
    this.bidProbability = bidProbability;
    this.minBid = minBid;
    this.maxBid = maxBid;
  }
  
  @Override
  protected long generateBid() {
    long bid = 0;
    if (random.nextFloat() >= bidProbability) {
      bid = Math.round((random.nextFloat() * (maxBid - minBid))) + minBid;
    }
    return bid;
  }
}
