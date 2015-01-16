/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.data.stream.service.heartbeat;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.StreamCoordinator;
import co.cask.cdap.data.stream.StreamPropertyListener;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.notifications.feeds.NotificationFeed;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedNotFoundException;
import co.cask.cdap.notifications.service.NotificationContext;
import co.cask.cdap.notifications.service.NotificationHandler;
import co.cask.cdap.notifications.service.NotificationService;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link StreamsHeartbeatsAggregator} in which heartbeats are received as notifications. This implementation
 * uses the {@link NotificationService} to subscribe to heartbeats sent by Stream writers.
 */
// TODO this guy should have a way to get the Threshold for the streams it is doing aggregation
public class NotificationHeartbeatsAggregator extends AbstractIdleService implements StreamsHeartbeatsAggregator {
  private static final Logger LOG = LoggerFactory.getLogger(NotificationHeartbeatsAggregator.class);

  private static final int AGGREGATION_EXECUTOR_POOL_SIZE = 10;

  private final Map<String, Cancellable> streamHeartbeatsSubscriptions;
  private final NotificationService notificationService;
  private final StreamCoordinator streamCoordinator;
  private final StreamAdmin streamAdmin;

  private ListeningScheduledExecutorService scheduledExecutor;

  @Inject
  public NotificationHeartbeatsAggregator(NotificationService notificationService, StreamCoordinator streamCoordinator,
                                          StreamAdmin streamAdmin) {
    this.notificationService = notificationService;
    this.streamCoordinator = streamCoordinator;
    this.streamAdmin = streamAdmin;
    this.streamHeartbeatsSubscriptions = Maps.newHashMap();
  }

  @Override
  protected void startUp() throws Exception {
    this.scheduledExecutor = MoreExecutors.listeningDecorator(
      Executors.newScheduledThreadPool(AGGREGATION_EXECUTOR_POOL_SIZE,
                                       Threads.createDaemonThreadFactory("streams-heartbeats-aggregator")));
  }

  @Override
  protected void shutDown() throws Exception {
    try {
      for (Cancellable subscription : streamHeartbeatsSubscriptions.values()) {
        subscription.cancel();
      }
    } finally {
      scheduledExecutor.shutdownNow();
    }
  }

  @Override
  public synchronized void listenToStreams(Collection<String> streamNames) {
    Set<String> alreadyListeningStreams = Sets.newHashSet(streamHeartbeatsSubscriptions.keySet());
    for (final String streamName : streamNames) {
      if (alreadyListeningStreams.remove(streamName)) {
        continue;
      }
      try {
        listenToStream(streamAdmin.getConfig(streamName));
      } catch (IOException e) {
        LOG.warn("Unable to listen to heartbeats of Stream {}", streamName);
      }
    }

    // Remove subscriptions to the heartbeats we used to listen to before the call to that method,
    // but don't anymore
    for (String outdatedStream : alreadyListeningStreams) {
      Cancellable cancellable = streamHeartbeatsSubscriptions.remove(outdatedStream);
      if (cancellable != null) {
        cancellable.cancel();
      }
    }
  }

  @Override
  public synchronized void listenToStream(StreamConfig streamConfig) throws IOException {
    if (streamHeartbeatsSubscriptions.containsKey(streamConfig.getName())) {
      return;
    }

    final Aggregator aggregator = new Aggregator(streamConfig);

    final Cancellable heartbeatsSubscription = subscribeToStreamHeartbeats(streamConfig.getName(), aggregator);
    final Cancellable truncationListener = streamCoordinator.addListener(streamConfig.getName(),
                                                                         new StreamPropertyListener() {
      @Override
      public void generationChanged(String streamName, int generation) {
        aggregator.reset();
      }
    });

    // Schedule aggregation logic
    final ScheduledFuture<?> scheduledFuture =
      scheduledExecutor.scheduleAtFixedRate(aggregator, Constants.Notification.Stream.INIT_AGGREGATION_DELAY,
                                            Constants.Notification.Stream.AGGREGATION_DELAY, TimeUnit.SECONDS);

    streamHeartbeatsSubscriptions.put(streamConfig.getName(), new Cancellable() {
      @Override
      public void cancel() {
        truncationListener.cancel();
        heartbeatsSubscription.cancel();
        scheduledFuture.cancel(false);
      }
    });
  }

  /**
   * Subscribe to the notification feed concerning heartbeats of the feed {@code streamName}.
   *
   * @param streamName stream with heartbeats to subscribe to
   * @param aggregator heartbeats aggregator for the {@code streamName}
   */
  private Cancellable subscribeToStreamHeartbeats(String streamName, final Aggregator aggregator) throws IOException {
    try {
      final NotificationFeed heartbeatFeed = new NotificationFeed.Builder()
        .setNamespace("default")
        .setCategory(Constants.Notification.Stream.STREAM_HEARTBEAT_FEED_CATEGORY)
        .setName(streamName)
        .build();

      return notificationService.subscribe(
        heartbeatFeed, new NotificationHandler<StreamWriterHeartbeat>() {
          @Override
          public Type getNotificationFeedType() {
            return StreamWriterHeartbeat.class;
          }

          @Override
          public void received(StreamWriterHeartbeat heartbeat, NotificationContext notificationContext) {
            if (heartbeat.getType().equals(StreamWriterHeartbeat.Type.INIT)) {
              // Init heartbeats don't describe "new" data, hence we consider
              // their data as part of the base count
              // TODO think more about this! If a writer is killed, a new one comes back up and
              // sends an init heartbeat. The leader should check if it already has a heartbeat from
              // this writer and only add the difference with the last heartbeat.
              // TODO UNIT TEST THAT!!
              StreamWriterHeartbeat lastHeartbeat = aggregator.getHeartbeats().get(heartbeat.getWriterID());
              long toAdd;
              if (lastHeartbeat == null) {
                toAdd = heartbeat.getAbsoluteDataSize();
              } else {
                // Recalibrate the leader's base count according to that particular writer's base count
                toAdd = heartbeat.getAbsoluteDataSize() - lastHeartbeat.getAbsoluteDataSize();
              }
              aggregator.getStreamBaseCount().addAndGet(toAdd);
            }
            aggregator.getHeartbeats().put(heartbeat.getWriterID(), heartbeat);
          }
        });
    } catch (NotificationFeedNotFoundException e) {
      throw new IOException(e);
    } catch (NotificationFeedException e) {
      throw new IOException(e);
    }
  }

  private long toB(int mb) {
    return (long) mb * 1000;
  }

  /**
   * Runnable scheduled to aggregate the sizes of all stream writers for one stream.
   * A notification is published if the aggregated size is higher than a threshold.
   */
  private final class Aggregator implements Runnable {

    private final Map<Integer, StreamWriterHeartbeat> heartbeats;
    private final NotificationFeed streamFeed;
    private final AtomicLong streamBaseCount;

    // This boolean will ensure that an extra Stream notification is sent at CDAP start-up.
    private boolean initNotificationSent;

    private int thresholdMB;

    protected Aggregator(StreamConfig streamConfig) {
      this.heartbeats = Maps.newHashMap();
      this.streamBaseCount = new AtomicLong(0);
      this.streamFeed = new NotificationFeed.Builder()
        .setNamespace("default")
        .setCategory(Constants.Notification.Stream.STREAM_FEED_CATEGORY)
        .setName(streamConfig.getName())
        .build();
      this.initNotificationSent = false;
      this.thresholdMB = streamConfig.getNotificationThresholdMB();

      // TODO add a listener to the streamCoordinator to get the new threshold if it changes.
      streamCoordinator.addListener(streamConfig.getName(), new StreamPropertyListener() {
        @Override
        public void generationChanged(String streamName, int generation) {
          super.generationChanged(streamName, generation);
        }
      })
    }

    public Map<Integer, StreamWriterHeartbeat> getHeartbeats() {
      return heartbeats;
    }

    public AtomicLong getStreamBaseCount() {
      return streamBaseCount;
    }

    public void reset() {
      heartbeats.clear();
      streamBaseCount.set(0);
    }

    @Override
    public void run() {
      // For now just count the last heartbeat present in the map, but we should set a sort of sliding window for
      // the aggregator, and it would look for the last heartbeat in that window only
      // TODO why should we remember more than the last heartbeat?
      int sum = 0;
      for (StreamWriterHeartbeat heartbeat : heartbeats.values()) {
        sum += heartbeat.getAbsoluteDataSize();
      }

      if (!initNotificationSent || sum - streamBaseCount.get() > toB(thresholdMB)) {
        try {
          initNotificationSent = true;
          publishNotification(sum);
        } finally {
          streamBaseCount.set(sum);
        }
      }
    }

    private void publishNotification(int updatedCount) {
      try {
        // TODO thing about the kind of notification to send here
        notificationService.publish(streamFeed, String.format("Has received %d bytes of data total", updatedCount))
          .get();
      } catch (NotificationFeedException e) {
        LOG.warn("Error with notification feed {}", streamFeed, e);
      } catch (Throwable t) {
        LOG.warn("Could not publish notification on feed {}", streamFeed.getId(), t);
      }
    }

  }
}
