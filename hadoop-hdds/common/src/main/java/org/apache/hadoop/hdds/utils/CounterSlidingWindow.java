/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.ratis.util.TimeDuration;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 *
 * A sliding window implementation that combines time-based expiry with a
 * maximum size constraint. The window tracks event timestamps and maintains two
 * limits:
 * <ul>
 * <li>Time-based: Events older than the specified expiry duration are
 *     automatically removed
 * <li>Size-based: The window maintains at most windowSize latest events, removing
 *     older events when this limit is exceeded
 * </ul>
 *
 * The window is considered full when the number of non-expired events exceeds
 * the specified window size. Events are automatically pruned based on both
 * their age and the maximum size constraint.
 */
public class CounterSlidingWindow {
  private final Object lock = new Object();
  private final int windowSize;
  private final Deque<CounterEntry> counters;
  private final long expiryDurationMillis;
  private final Clock clock;

  /**
   * Default constructor that uses a monotonic clock.
   *
   * @param windowSize     the maximum number of events that are tracked
   * @param expiryDuration the duration after which an entry in the window expires
   */
  public CounterSlidingWindow(int windowSize, Duration expiryDuration) {
    this(windowSize, expiryDuration, new MonotonicClock());
  }

  /**
   * Constructor with a custom clock for testing.
   *
   * @param windowSize     the maximum number of events that are tracked
   * @param expiryDuration the duration after which an entry in the window expires
   * @param clock          the clock to use for time measurements
   */
  public CounterSlidingWindow(int windowSize, Duration expiryDuration, Clock clock) {
    if (windowSize < 0) {
      throw new IllegalArgumentException("Window size must be greater than 0");
    }
    if (expiryDuration.isNegative() || expiryDuration.isZero()) {
      throw new IllegalArgumentException("Expiry duration must be greater than 0");
    }
    this.windowSize = windowSize;
    this.expiryDurationMillis = expiryDuration.toMillis();
    this.clock = clock;
    this.counters = new ArrayDeque<>(windowSize + 1);
  }

  public void add(TimeDuration duration) {
    synchronized (lock) {
      removeExpired();
      counters.add(new CounterEntry(getCurrentTime(), duration));
    }
  }

  @VisibleForTesting
  public long getAccumulatedValueMs() {
    synchronized (lock) {
      removeExpired();
      return counters.stream().mapToLong(c -> c.getValue().toLong(TimeUnit.MILLISECONDS)).sum();
    }
  }

  private void removeExpired() {
    synchronized (lock) {
      long currentTime = getCurrentTime();
      long expirationThreshold = currentTime - expiryDurationMillis;

      while (!counters.isEmpty() && counters.peek().getTimestamp() < expirationThreshold) {
        counters.remove();
      }
    }
  }

  public int getWindowSize() {
    return windowSize;
  }

  private long getCurrentTime() {
    return clock.millis();
  }

  /**
   * Inner class to hold timestamp and counter value pairs.
   */
  private static final class CounterEntry {
    private final long timestamp;
    private final TimeDuration value;

    CounterEntry(long timestamp, TimeDuration value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public TimeDuration getValue() {
      return value;
    }
  }

  /**
   * A custom monotonic clock implementation.
   * Implementation of Clock that uses System.nanoTime() for real usage.
   * See {@see org.apache.ozone.test.TestClock}
   */
  private static final class MonotonicClock extends Clock {
    @Override
    public long millis() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    @Override
    public java.time.Instant instant() {
      return java.time.Instant.ofEpochMilli(millis());
    }

    @Override
    public java.time.ZoneId getZone() {
      return java.time.ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      // Ignore zone for monotonic clock
      throw new UnsupportedOperationException("Sliding Window class does not allow changing the timezone");
    }
  }
}
