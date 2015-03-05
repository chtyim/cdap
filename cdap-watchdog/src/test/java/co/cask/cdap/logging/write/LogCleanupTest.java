/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.logging.write;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.data2.datafabric.dataset.InMemoryDefinitionRegistryFactory;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.InMemoryDatasetFramework;
import co.cask.cdap.data2.dataset2.module.lib.inmemory.InMemoryTableModule;
import co.cask.cdap.logging.context.FlowletLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionManager;
import co.cask.tephra.runtime.TransactionModules;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Set;

/**
 * Test LogCleanup class.
 */
public class LogCleanupTest {
  private static final Logger LOG = LoggerFactory.getLogger(LogCleanupTest.class);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final int RETENTION_DURATION_MS = 100000;

  private static Injector injector;
  private static TransactionManager txManager;


  @BeforeClass
  public static void init() throws Exception {
    Configuration hConf = HBaseConfiguration.create();
    final CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      new LocationRuntimeModule().getInMemoryModules(),
      new TransactionModules().getInMemoryModules(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(DatasetFramework.class)
            .toInstance(new InMemoryDatasetFramework(new InMemoryDefinitionRegistryFactory(), cConf));
        }
      });

    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();

    DatasetFramework dsFramework = injector.getInstance(DatasetFramework.class);
    dsFramework.addModule(Id.DatasetModule.from(Constants.SYSTEM_NAMESPACE_ID, "table"),
                          new InMemoryTableModule());
  }

  @AfterClass
  public static void finish() {
    txManager.stopAndWait();
  }

  @Test
  public void testCleanup() throws Exception {
    FileMetaDataManager fileMetaDataManager = injector.getInstance(FileMetaDataManager.class);

    // Create base dir
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Location baseDir = locationFactory.create(TEMP_FOLDER.newFolder().toURI());

    // Deletion boundary
    long deletionBoundary = System.currentTimeMillis() - RETENTION_DURATION_MS;
    LOG.info("deletionBoundary = {}", deletionBoundary);

    // Setup directories
    LoggingContext dummyContext = new FlowletLoggingContext("act", "app", "flw", "flwt");

    Location contextDir = baseDir.append("act/app/flw");
    List<Location> toDelete = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-1"));
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-2"));
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-3"));
      toDelete.add(contextDir.append("2012-12-1" + i + "/del-4"));

      toDelete.add(contextDir.append("del-1"));
    }

    Assert.assertFalse(toDelete.isEmpty());

    List<Location> notDelete = Lists.newArrayList();
    for (int i = 0; i < 5; ++i) {
      toDelete.add(contextDir.append("2012-12-2" + i + "/del-5"));
      notDelete.add(contextDir.append("2012-12-2" + i + "/nodel-1"));
      notDelete.add(contextDir.append("2012-12-2" + i + "/nodel-2"));
      notDelete.add(contextDir.append("2012-12-2" + i + "/nodel-3"));
    }

    Assert.assertFalse(notDelete.isEmpty());

    int counter = 0;
    for (Location location : toDelete) {
      fileMetaDataManager.writeMetaData(dummyContext, deletionBoundary - counter - 10000,
                                        createFile(location));
      counter++;
    }

    for (Location location : notDelete) {
      fileMetaDataManager.writeMetaData(dummyContext, deletionBoundary + counter + 10000,
                                        createFile(location));
      counter++;
    }

    Assert.assertEquals(locationListsToString(toDelete, notDelete),
      toDelete.size() + notDelete.size(), fileMetaDataManager.listFiles(dummyContext).size());

    LogCleanup logCleanup = new LogCleanup(fileMetaDataManager, baseDir, RETENTION_DURATION_MS);
    logCleanup.run();
    logCleanup.run();

    for (Location location : toDelete) {
      Assert.assertFalse("Location " + location.toURI() + " is not deleted!", location.exists());
    }

    for (Location location : notDelete) {
      Assert.assertTrue("Location " + location.toURI() + " is deleted!", location.exists());
    }

    for (int i = 0; i < 5; ++i) {
      Location delDir = contextDir.append("2012-12-1" + i);
      Assert.assertFalse("Location " + delDir.toURI() + " is not deleted!", delDir.exists());
    }
  }

  @Test
  public void testDeleteEmptyDir1() throws Exception {
    // Create base dir
    Location baseDir = injector.getInstance(LocationFactory.class).create(TEMP_FOLDER.newFolder().toURI());

    // Create dirs with files
    Set<Location> files = Sets.newHashSet();
    Set<Location> nonEmptyDirs = Sets.newHashSet();
    for (int i = 0; i < 5; ++i) {
      files.add(createFile(baseDir.append(String.valueOf(i))));

      Location dir1 = createDir(baseDir.append("abc"));
      files.add(dir1);
      nonEmptyDirs.add(dir1);
      files.add(createFile(baseDir.append("abc/" + i)));
      files.add(createFile(baseDir.append("abc/def/" + i)));

      Location dir2 = createDir(baseDir.append("def"));
      files.add(dir2);
      nonEmptyDirs.add(dir2);
      files.add(createFile(baseDir.append("def/" + i)));
      files.add(createFile(baseDir.append("def/hij/" + i)));
    }

    // Create empty dirs
    Set<Location> emptyDirs = Sets.newHashSet();
    for (int i = 0; i < 5; ++i) {
      emptyDirs.add(createDir(baseDir.append("dir_" + i)));
      emptyDirs.add(createDir(baseDir.append("dir_" + i + "/emptyDir1")));
      emptyDirs.add(createDir(baseDir.append("dir_" + i + "/emptyDir2")));

      emptyDirs.add(createDir(baseDir.append("abc/dir_" + i)));
      emptyDirs.add(createDir(baseDir.append("abc/def/dir_" + i)));

      emptyDirs.add(createDir(baseDir.append("def/dir_" + i)));
      emptyDirs.add(createDir(baseDir.append("def/hij/dir_" + i)));
    }

    LogCleanup logCleanup = new LogCleanup(null, baseDir, RETENTION_DURATION_MS);
    for (Location location : Sets.newHashSet(Iterables.concat(nonEmptyDirs, emptyDirs))) {
      logCleanup.deleteEmptyDir(location);
    }

    // Assert non-empty dirs (and their files) are still present
    for (Location location : files) {
      Assert.assertTrue("Location " + location.toURI() + " is deleted!", location.exists());
    }

    // Assert empty dirs are deleted
    for (Location location : emptyDirs) {
      Assert.assertFalse("Dir " + location.toURI() + " is still present!", location.exists());
    }

    // Assert base dir exists
    Assert.assertTrue(baseDir.exists());
  }

  @Test
  public void testDeleteEmptyDir2() throws Exception {
    // Create base dir
    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    Location baseDir = locationFactory.create(TEMP_FOLDER.newFolder().toURI());

    LogCleanup logCleanup = new LogCleanup(null, baseDir, RETENTION_DURATION_MS);

    logCleanup.deleteEmptyDir(baseDir);
    // Assert base dir exists
    Assert.assertTrue(baseDir.exists());

    Location rootPath = locationFactory.create("/");
    rootPath.mkdirs();
    Assert.assertTrue(rootPath.exists());
    logCleanup.deleteEmptyDir(rootPath);
    // Assert root still exists
    Assert.assertTrue(rootPath.exists());

    Location tmpPath = locationFactory.create("/tmp");
    tmpPath.mkdirs();
    Assert.assertTrue(tmpPath.exists());
    logCleanup.deleteEmptyDir(tmpPath);
    // Assert tmp still exists
    Assert.assertTrue(tmpPath.exists());
  }

  private Location createFile(Location path) throws Exception {
    Location parent = Locations.getParent(path);
    Assert.assertNotNull(parent);
    parent.mkdirs();

    path.createNew();
    Assert.assertTrue(path.exists());
    return path;
  }

  private Location createDir(Location path) throws Exception {
    path.mkdirs();
    return path;
  }

  private String locationListsToString(List<Location> list1, List<Location> list2) {
    return ImmutableList.of(Lists.transform(list1, LOCATION_URI_FUNCTION),
                            Lists.transform(list2, LOCATION_URI_FUNCTION)).toString();
  }

  private static final Function<Location, URI> LOCATION_URI_FUNCTION =
    new Function<Location, URI>() {
      @Override
      public URI apply(Location input) {
        return input.toURI();
      }
    };
}
