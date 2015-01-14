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

package co.cask.cdap.data.stream;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.common.metrics.MetricsCollector;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.runtime.LocationStreamFileWriterFactory;
import co.cask.cdap.data.stream.service.StreamHttpService;
import co.cask.cdap.data.stream.service.StreamServiceRuntimeModule;
import co.cask.cdap.data.stream.service.heartbeat.HeartbeatPublisher;
import co.cask.cdap.data.stream.service.heartbeat.StreamWriterHeartbeat;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetService;
import co.cask.cdap.data2.datafabric.dataset.service.executor.DatasetOpExecutor;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.data2.transaction.stream.StreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamConsumerStateStoreFactory;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileAdmin;
import co.cask.cdap.data2.transaction.stream.leveldb.LevelDBStreamFileConsumerFactory;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.gateway.auth.AuthModule;
import co.cask.tephra.TransactionManager;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class StreamFileSizeManagerTest {
  private static final String API_KEY = "SampleTestApiKey";
  private static final byte[] TWO_BYTES = new byte[] { 'a', 'b' };

  private static final String HOSTNAME = "127.0.0.1";
  private static int port;
  private static CConfiguration conf;

  private static Injector injector;
  private static StreamHttpService streamHttpService;
  private static TransactionManager txService;
  private static DatasetOpExecutor dsOpService;
  private static DatasetService datasetService;
  private static MockHeartbeatPublisher heartbeatPublisher;
  private static TemporaryFolder tmpFolder = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws IOException {
    tmpFolder.create();
    conf = CConfiguration.create();
    conf.set(Constants.Router.ADDRESS, HOSTNAME);
    conf.setInt(Constants.Router.ROUTER_PORT, 0);
    conf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    injector = startGateway(conf);
  }

  @AfterClass
  public static void afterClass() {
    stopGateway(conf);
    tmpFolder.delete();
  }

  public static Injector startGateway(final CConfiguration conf) {
    // Set up our Guice injections
    injector = Guice.createInjector(
      Modules.override(
        new StreamServiceRuntimeModule().getStandaloneModules(),
        new DataFabricModules().getInMemoryModules(),
        new DataSetsModules().getLocalModule(),
        new DataSetServiceModules().getInMemoryModule(),
        new ConfigModule(conf, new Configuration()),
        new AuthModule(), new DiscoveryRuntimeModule().getInMemoryModules(),
        new LocationRuntimeModule().getInMemoryModules(),
        new ExploreClientModule()
      ).with(new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(MockMetricsCollectionService.class);

          bind(StreamConsumerStateStoreFactory.class)
            .to(LevelDBStreamConsumerStateStoreFactory.class).in(Singleton.class);
          bind(StreamAdmin.class).to(LevelDBStreamFileAdmin.class).in(Singleton.class);
          bind(StreamConsumerFactory.class).to(LevelDBStreamFileConsumerFactory.class).in(Singleton.class);
          bind(StreamFileWriterFactory.class).to(LocationStreamFileWriterFactory.class).in(Singleton.class);

          bind(HeartbeatPublisher.class).to(MockHeartbeatPublisher.class).in(Scopes.SINGLETON);
        }
      }));

    txService = injector.getInstance(TransactionManager.class);
    txService.startAndWait();
    dsOpService = injector.getInstance(DatasetOpExecutor.class);
    dsOpService.startAndWait();
    datasetService = injector.getInstance(DatasetService.class);
    datasetService.startAndWait();
    streamHttpService = injector.getInstance(StreamHttpService.class);
    streamHttpService.startAndWait();
    heartbeatPublisher = (MockHeartbeatPublisher) injector.getInstance(HeartbeatPublisher.class);

    port = streamHttpService.getBindAddress().getPort();

    return injector;
  }

  public static void stopGateway(CConfiguration conf) {
    streamHttpService.stopAndWait();
    datasetService.stopAndWait();
    dsOpService.stopAndWait();
    txService.stopAndWait();
    conf.clear();
  }

  private HttpURLConnection openURL(String location, HttpMethod method) throws IOException {
    URL url = new URL(location);
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setRequestMethod(method.getName());
    urlConn.setRequestProperty(Constants.Gateway.API_KEY, API_KEY);

    return urlConn;
  }

  @Test
  public void streamPublishesHeartbeatTest() throws Exception {
    // Create a new stream.
    HttpURLConnection urlConn = openURL(String.format("http://%s:%d/v2/streams/test_stream1", HOSTNAME, port),
                                        HttpMethod.PUT);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // Enqueue 10 entries
    for (int i = 0; i < 10; ++i) {
      urlConn = openURL(String.format("http://%s:%d/v2/streams/test_stream1", HOSTNAME, port), HttpMethod.POST);
      urlConn.setDoOutput(true);
      urlConn.addRequestProperty("test_stream1.header1", Integer.toString(i));
      urlConn.getOutputStream().write(TWO_BYTES);
      Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
      urlConn.disconnect();
    }

    // Wait to receive heartbeat
    TimeUnit.SECONDS.sleep(Constants.Stream.HEARTBEAT_DELAY + 1);
    StreamWriterHeartbeat heartbeat = heartbeatPublisher.getHeartbeat();
    Assert.assertNotNull(heartbeat);
    Assert.assertEquals(20, heartbeat.getAbsoluteDataSize());
    Assert.assertEquals(StreamWriterHeartbeat.Type.REGULAR, heartbeat.getType());

    // Truncate the stream
    urlConn = openURL(String.format("http://%s:%d/v2/streams/test_stream1/truncate", HOSTNAME, port),
                      HttpMethod.POST);
    Assert.assertEquals(HttpResponseStatus.OK.getCode(), urlConn.getResponseCode());
    urlConn.disconnect();

    // Wait to receive heartbeat
    TimeUnit.SECONDS.sleep(Constants.Stream.HEARTBEAT_DELAY + 1);
    heartbeat = heartbeatPublisher.getHeartbeat();
    Assert.assertNotNull(heartbeat);
    Assert.assertEquals(0, heartbeat.getAbsoluteDataSize());
    Assert.assertEquals(StreamWriterHeartbeat.Type.REGULAR, heartbeat.getType());
  }

  /**
   * Mock heartbeat publisher that allows to do assertions on the hearbeats being published.
   */
  private static final class MockHeartbeatPublisher extends AbstractIdleService implements HeartbeatPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(MockHeartbeatPublisher.class);
    private StreamWriterHeartbeat heartbeat = null;

    @Override
    protected void startUp() throws Exception {
      LOG.info("Starting Publisher.");
    }

    @Override
    protected void shutDown() throws Exception {
      LOG.info("Stopping Publisher.");
    }

    @Override
    public ListenableFuture<StreamWriterHeartbeat> sendHeartbeat(StreamWriterHeartbeat heartbeat) {
      LOG.info("Received heartbeat {}", heartbeat);
      this.heartbeat = heartbeat;
      return Futures.immediateFuture(heartbeat);
    }

    public StreamWriterHeartbeat getHeartbeat() {
      return heartbeat;
    }
  }

  private static final class MockMetricsCollectionService extends AbstractIdleService
    implements MetricsCollectionService {

    @Override
    protected void startUp() throws Exception {
      // No-op
    }

    @Override
    protected void shutDown() throws Exception {
      // No-op
    }

    @Override
    public MetricsCollector getCollector(MetricsScope scope, String context, String runId) {
      return new MetricsCollector() {
        @Override
        public void increment(String metricName, int value, String... tags) {
          // No-op
        }

        @Override
        public void gauge(String metricName, long value, String... tags) {
          // No-op
        }
      };
    }
  }
}
