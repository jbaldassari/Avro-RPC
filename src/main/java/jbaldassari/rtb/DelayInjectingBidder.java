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

import org.apache.avro.AvroRemoteException;
import org.apache.avro.util.Utf8;

/**
 * Decorates an existing Bidder by adding a delay to its response.
 */
public class DelayInjectingBidder implements Bidder {
  private final Bidder chainedBidder;
  private final long delayMillis;
  
  /**
   * Creates a DelayInjectingBidder.
   * @param chainedBidder the bidder to invoke after adding the delay.
   * @param delayMillis the delay to add, in milliseconds.
   */
  public DelayInjectingBidder(Bidder chainedBidder, long delayMillis) {
    if (chainedBidder == null) {
      throw new NullPointerException("chainedBidder is null");
    }
    if (delayMillis < 1) {
      throw new IllegalArgumentException("delayMillis must be >= 1");
    }
    this.chainedBidder = chainedBidder;
    this.delayMillis = delayMillis;
  }
  
  @Override
  public BidResponse bid(BidRequest bidRequest) throws AvroRemoteException,
      BidderError {
    try {
      Thread.sleep(delayMillis);
    } catch (InterruptedException e) {
      throw BidderError.newBuilder().setCode(-1).
        setMessage$(new Utf8("Interrupted while injecting delay")).build();
    }
    return chainedBidder.bid(bidRequest);
  }

  @Override
  public void notify(Notification notification) {
    chainedBidder.notify(notification);
  }
  
  @Override
  public boolean ping() throws AvroRemoteException {
    return chainedBidder.ping();
  }
}
