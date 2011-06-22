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


/**
 * A Bidder that always bids the same amount.
 */
public class ConstantBidder extends AbstractBidder {
  private final long constantBid;
  
  /**
   * Creates a ConstantBidder.
   * @param snippet the creative snippet to return in the each non-zero bid.
   * @param constantBid the amount to bid for every bid response.
   */
  public ConstantBidder(CharSequence snippet, long constantBid) {
    this(null, snippet, constantBid);
  }
  
  /**
   * Creates a ConstantBidder.
   * @param bidderId the bidder ID to set.
   * @param snippet the creative snippet to return in the each non-zero bid.
   * @param constantBid the amount to bid for every bid response.
   */
  public ConstantBidder(CharSequence bidderId, CharSequence snippet, 
      long constantBid) {
    super(snippet, bidderId);
    if (constantBid < 0) {
      throw new IllegalArgumentException("constantBid must be >= 0");
    }
    this.constantBid = constantBid;
  }

  @Override
  protected long generateBid() {
    return constantBid;
  }
}
