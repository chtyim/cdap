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

package co.cask.cdap.etl.batch.mapreduce;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.batch.ETLBatchTestBase;
import co.cask.cdap.etl.batch.config.ETLBatchConfig;
import co.cask.cdap.etl.batch.source.FileBatchSource;
import co.cask.cdap.etl.common.Connection;
import co.cask.cdap.etl.common.ETLStage;
import co.cask.cdap.etl.common.Plugin;
import co.cask.cdap.etl.common.Properties;
import co.cask.cdap.etl.test.sink.MetaKVTableSink;
import co.cask.cdap.etl.test.source.MetaKVTableSource;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.S3NInMemoryFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests for ETLBatch.
 */
public class ETLMapReduceTestRun extends ETLBatchTestBase {

  @Test
  public void testInvalidTransformConfigFailsToDeploy() {
    Plugin sourceConfig =
      new Plugin("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table1"));
    Plugin sink =
      new Plugin("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table2"));

    Plugin transform = new Plugin("Script", ImmutableMap.of("script", "return x;"));
    List<ETLStage> transformList = Lists.newArrayList(new ETLStage("transform", transform));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", new ETLStage("source", sourceConfig),
                                                  new ETLStage("sink", sink), transformList);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKV");
    try {
      deployApplication(appId, appRequest);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testKVToKV() throws Exception {
    // kv table to kv table pipeline
    Plugin sourceConfig =
      new Plugin("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table1"));
    Plugin sinkConfig =
      new Plugin("KVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "table2"));
    Plugin transformConfig = new Plugin("Projection", ImmutableMap.<String, String>of());
    List<ETLStage> transformList = Lists.newArrayList(new ETLStage("transform", transformConfig));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", new ETLStage("source", sourceConfig),
                                                  new ETLStage("sink", sinkConfig), transformList);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKV");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<KeyValueTable> table1 = getDataset("table1");
    KeyValueTable inputTable = table1.get();
    for (int i = 0; i < 10000; i++) {
      inputTable.write("hello" + i, "world" + i);
    }
    table1.flush();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> table2 = getDataset("table2");
    try (KeyValueTable outputTable = table2.get()) {
      for (int i = 0; i < 10000; i++) {
        Assert.assertEquals("world" + i, Bytes.toString(outputTable.read("hello" + i)));
      }
    }
  }

  @Test
  public void testDAG() throws Exception {

    Schema schema = Schema.recordOf(
      "userNames",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("userid", Schema.of(Schema.Type.STRING))
    );
    Plugin sourceConfig = new Plugin("Table",
                                     ImmutableMap.of(
                                       Properties.BatchReadableWritable.NAME, "dagInputTable",
                                       Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                       Properties.Table.PROPERTY_SCHEMA, schema.toString()));

    Plugin sinkConfig1 = new Plugin("Table",
                             ImmutableMap.of(
                               Properties.BatchReadableWritable.NAME, "dagOutputTable1",
                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey"));
    Plugin sinkConfig2 = new Plugin("Table",
                                    ImmutableMap.of(
                                      Properties.BatchReadableWritable.NAME, "dagOutputTable2",
                                      Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey"));

    String validationScript = "function isValid(input, context) {  " +
      "var errCode = 0; var errMsg = 'none'; var isValid = true;" +
      "if (!coreValidator.maxLength(input.userid, 4)) " +
      "{ errCode = 10; errMsg = 'user name greater than 6 characters'; isValid = false; }; " +
      "return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
      "};";
    Plugin transformConfig = new Plugin("Validator",
                                        ImmutableMap.of("validators", "core",
                                                        "validationScript", validationScript));

    List<ETLStage> transformList = Lists.newArrayList(new ETLStage("transform", transformConfig));

    List<ETLStage> sinks = ImmutableList.of(new ETLStage("sink1", sinkConfig1), new ETLStage("sink2", sinkConfig2));

    List<Connection> connections = new ArrayList<>();
    connections.add(new Connection("source", "sink1"));
    connections.add(new Connection("source", "transform"));
    connections.add(new Connection("transform", "sink2"));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *", new ETLStage("source", sourceConfig),
                                                  sinks, transformList, connections, null, null);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "DagApp");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("dagInputTable");
    Table inputTable = inputManager.get();

    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      // valid record, user name "sam[0-9]" is 4 chars long for validator transform
      put.add("userid", "sam" + i);
      inputTable.put(put);
      inputManager.flush();

      Put put2 = new Put(Bytes.toBytes("row" + (i + 10)));
      // invalid record, user name "sam[10-19]" is 5 chars long and invalid according to validator transform
      put2.add("userid", "sam" + (i + 10));
      inputTable.put(put2);
      inputManager.flush();
    }


    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    // all records are passed to this table (validation not performed)
    DataSetManager<Table> outputManager1 = getDataset("dagOutputTable1");
    Table outputTable1 = outputManager1.get();
    for (int i = 0; i < 20; i++) {
      Row row = outputTable1.get(Bytes.toBytes("row" + i));
      Assert.assertEquals("sam" + i, row.getString("userid"));
    }

    // only 10 records are passed to this table (validation performed)
    DataSetManager<Table> outputManager2 = getDataset("dagOutputTable2");
    Table outputTable2 = outputManager2.get();
    for (int i = 0; i < 10; i++) {
      Row row = outputTable2.get(Bytes.toBytes("row" + i));
      Assert.assertEquals("sam" + i, row.getString("userid"));
    }
    for (int i = 10; i < 20; i++) {
      Row row = outputTable2.get(Bytes.toBytes("row" + i));
      Assert.assertNull(row.getString("userid"));
    }
  }

  @Test
  public void testKVToKVMeta() throws Exception {
    Plugin sourceConfig =
      new Plugin("MetaKVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "mtable1"));
    Plugin sinkConfig =
      new Plugin("MetaKVTable", ImmutableMap.of(Properties.BatchReadableWritable.NAME, "mtable2"));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *",
                                                  new ETLStage("source", sourceConfig),
                                                  new ETLStage("sink", sinkConfig));

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKVMeta");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> sourceMetaTable = getDataset(MetaKVTableSource.META_TABLE);
    KeyValueTable sourceTable = sourceMetaTable.get();
    Assert.assertEquals(MetaKVTableSource.PREPARE_RUN_KEY,
                        Bytes.toString(sourceTable.read(MetaKVTableSource.PREPARE_RUN_KEY)));
    Assert.assertEquals(MetaKVTableSource.FINISH_RUN_KEY,
                        Bytes.toString(sourceTable.read(MetaKVTableSource.FINISH_RUN_KEY)));

    DataSetManager<KeyValueTable> sinkMetaTable = getDataset(MetaKVTableSink.META_TABLE);
    try (KeyValueTable sinkTable = sinkMetaTable.get()) {
      Assert.assertEquals(MetaKVTableSink.PREPARE_RUN_KEY,
                          Bytes.toString(sinkTable.read(MetaKVTableSink.PREPARE_RUN_KEY)));
      Assert.assertEquals(MetaKVTableSink.FINISH_RUN_KEY,
                          Bytes.toString(sinkTable.read(MetaKVTableSink.FINISH_RUN_KEY)));
    }
  }

  // TODO : remove this test after UI changes for unique name support in ETLStage is implemented.
  @Test
  public void testKVToKVMetaWithoutStageNames() throws Exception {
    ETLBatchConfig etlConfig =
      new ETLBatchConfig("* * * * *",
                         new ETLStage("MetaKVTable",
                                      ImmutableMap.of(Properties.BatchReadableWritable.NAME, "mtable1"), null),
                         new ETLStage("MetaKVTable",
                                      ImmutableMap.of(Properties.BatchReadableWritable.NAME, "mtable2"), null));

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "KVToKVMeta");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<KeyValueTable> sourceMetaTable = getDataset(MetaKVTableSource.META_TABLE);
    KeyValueTable sourceTable = sourceMetaTable.get();
    Assert.assertEquals(MetaKVTableSource.PREPARE_RUN_KEY,
                        Bytes.toString(sourceTable.read(MetaKVTableSource.PREPARE_RUN_KEY)));
    Assert.assertEquals(MetaKVTableSource.FINISH_RUN_KEY,
                        Bytes.toString(sourceTable.read(MetaKVTableSource.FINISH_RUN_KEY)));

    DataSetManager<KeyValueTable> sinkMetaTable = getDataset(MetaKVTableSink.META_TABLE);
    try (KeyValueTable sinkTable = sinkMetaTable.get()) {
      Assert.assertEquals(MetaKVTableSink.PREPARE_RUN_KEY,
                          Bytes.toString(sinkTable.read(MetaKVTableSink.PREPARE_RUN_KEY)));
      Assert.assertEquals(MetaKVTableSink.FINISH_RUN_KEY,
                          Bytes.toString(sinkTable.read(MetaKVTableSink.FINISH_RUN_KEY)));
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  public void testTableToTableWithValidations() throws Exception {

    Schema schema = Schema.recordOf(
      "purchase",
      Schema.Field.of("rowkey", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("user", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("count", Schema.of(Schema.Type.INT)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("item", Schema.of(Schema.Type.STRING))
    );

    Plugin sourceConfig = new Plugin("Table",
                                     ImmutableMap.of(
                                       Properties.BatchReadableWritable.NAME, "inputTable",
                                       Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey",
                                       Properties.Table.PROPERTY_SCHEMA, schema.toString()));

    String validationScript = "function isValid(input) {  " +
      "var errCode = 0; var errMsg = 'none'; var isValid = true;" +
      "if (!coreValidator.maxLength(input.user, 6)) " +
      "{ errCode = 10; errMsg = 'user name greater than 6 characters'; isValid = false; }; " +
      "return {'isValid': isValid, 'errorCode': errCode, 'errorMsg': errMsg}; " +
      "};";
    Plugin transformConfig = new Plugin("Validator",
                                        ImmutableMap.of("validators", "core",
                                                        "validationScript", validationScript));
    ETLStage transform = new ETLStage("transform", transformConfig, "keyErrors");
    List<ETLStage> transformList = new ArrayList<>();
    transformList.add(transform);

    Plugin sink = new Plugin("Table",
                             ImmutableMap.of(
                               Properties.BatchReadableWritable.NAME, "outputTable",
                               Properties.Table.PROPERTY_SCHEMA_ROW_FIELD, "rowkey"));

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *",
                                                  new ETLStage("source", sourceConfig),
                                                  new ETLStage("sink", sink), transformList);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TableToTable");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset("inputTable");
    Table inputTable = inputManager.get();

    // valid record, user name "samuel" is 6 chars long
    Put put = new Put(Bytes.toBytes("row1"));
    put.add("user", "samuel");
    put.add("count", 5);
    put.add("price", 123.45);
    put.add("item", "scotch");
    inputTable.put(put);
    inputManager.flush();

    // valid record, user name "jackson" is > 6 characters
    put = new Put(Bytes.toBytes("row2"));
    put.add("user", "jackson");
    put.add("count", 10);
    put.add("price", 123456789d);
    put.add("item", "island");
    inputTable.put(put);
    inputManager.flush();

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset("outputTable");
    Table outputTable = outputManager.get();

    Row row = outputTable.get(Bytes.toBytes("row1"));
    Assert.assertEquals("samuel", row.getString("user"));
    Assert.assertEquals(5, (int) row.getInt("count"));
    Assert.assertTrue(Math.abs(123.45 - row.getDouble("price")) < 0.000001);
    Assert.assertEquals("scotch", row.getString("item"));

    row = outputTable.get(Bytes.toBytes("row2"));
    Assert.assertEquals(0, row.getColumns().size());

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("keyErrors");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, ETLMapReduce.ERROR_SCHEMA);
      Assert.assertEquals(1, records.size());
    }
  }

  @Test
  public void testS3toTPFS() throws Exception {
    String testPath = "s3n://test/";
    String testFile1 = "2015-06-17-00-00-00.txt";
    String testData1 = "Sample data for testing.";

    String testFile2 = "abc.txt";
    String testData2 = "Sample data for testing.";

    S3NInMemoryFileSystem fs = new S3NInMemoryFileSystem();
    Configuration conf = new Configuration();
    conf.set("fs.s3n.impl", S3NInMemoryFileSystem.class.getName());
    fs.initialize(URI.create("s3n://test/"), conf);
    fs.createNewFile(new Path(testPath));

    try (FSDataOutputStream fos1 = fs.create(new Path(testPath + testFile1))) {
      fos1.write(testData1.getBytes());
      fos1.flush();
    }

    try (FSDataOutputStream fos2 = fs.create(new Path(testPath + testFile2))) {
      fos2.write(testData2.getBytes());
      fos2.flush();
    }

    Method method = FileSystem.class.getDeclaredMethod("addFileSystemForTesting",
                                                       URI.class, Configuration.class, FileSystem.class);
    method.setAccessible(true);
    method.invoke(FileSystem.class, URI.create("s3n://test/"), conf, fs);
    Plugin sourceConfig = new Plugin("S3", ImmutableMap.<String, String>builder()
      .put(Properties.S3.ACCESS_KEY, "key")
      .put(Properties.S3.ACCESS_ID, "ID")
      .put(Properties.S3.PATH, testPath)
      .put(Properties.S3.FILE_REGEX, "abc.*")
      .build());

    Plugin sinkConfig = new Plugin("TPFSAvro",
                                   ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                   FileBatchSource.DEFAULT_SCHEMA.toString(),
                                                   Properties.TimePartitionedFileSetDataset.TPFS_NAME, "TPFSsink"));
    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *",
                                                  new ETLStage("source", sourceConfig),
                                                  new ETLStage("sink", sinkConfig), Lists.<ETLStage>newArrayList());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "S3ToTPFS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(2, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("TPFSsink");
    try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
      List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
      // Two input files, each with one input record were specified. However, only one file matches the regex,
      // so only one record should be found in the output.
      Assert.assertEquals(1, records.size());
      Assert.assertEquals(testData1, records.get(0).get("body").toString());
    }
  }

  @Test
  public void testFiletoMultipleTPFS() throws Exception {
    String filePath = "file:///tmp/test/text.txt";
    String testData = "String for testing purposes.";

    Path textFile = new Path(filePath);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream writeData = fs.create(textFile);
    writeData.write(testData.getBytes());
    writeData.flush();
    writeData.close();

    Plugin sourceConfig = new Plugin("File", ImmutableMap.<String, String>builder()
      .put(Properties.File.FILESYSTEM, "Text")
      .put(Properties.File.PATH, filePath)
      .build());

    Plugin sink1Config = new Plugin("TPFSAvro",
                                    ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                    FileBatchSource.DEFAULT_SCHEMA.toString(),
                                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink1"));
    Plugin sink2Config = new Plugin("TPFSParquet",
                                    ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                    FileBatchSource.DEFAULT_SCHEMA.toString(),
                                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink2"));

    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink1 = new ETLStage("sink1", sink1Config);
    ETLStage sink2 = new ETLStage("sink2", sink2Config);

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *",
                                                  source,
                                                  Lists.newArrayList(sink1, sink2),
                                                  Lists.<ETLStage>newArrayList(),
                                                  new ArrayList<Connection>(),
                                                  new Resources(),
                                                  Lists.<ETLStage>newArrayList());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "FileToTPFS");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    MapReduceManager mrManager = appManager.getMapReduceManager(ETLMapReduce.NAME);
    mrManager.start();
    mrManager.waitForFinish(2, TimeUnit.MINUTES);

    for (String sinkName : new String[] { "fileSink1", "fileSink2" }) {
      DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(sinkName);
      try (TimePartitionedFileSet fileSet = fileSetManager.get()) {
        List<GenericRecord> records = readOutput(fileSet, FileBatchSource.DEFAULT_SCHEMA);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals(testData, records.get(0).get("body").toString());
      }
    }
  }

  @Test(expected = Exception.class)
  public void testDuplicateStageNameInPipeline() throws Exception {
    String filePath = "file:///tmp/test/text.txt";
    String testData = "String for testing purposes.";

    Plugin sourceConfig = new Plugin("File", ImmutableMap.<String, String>builder()
      .put(Properties.File.FILESYSTEM, "Text")
      .put(Properties.File.PATH, filePath)
      .build());

    Plugin sink1Config = new Plugin("TPFSAvro",
                                    ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                    FileBatchSource.DEFAULT_SCHEMA.toString(),
                                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink1"));
    Plugin sink2Config = new Plugin("TPFSParquet",
                                    ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                                    FileBatchSource.DEFAULT_SCHEMA.toString(),
                                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, "fileSink2"));

    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink1 = new ETLStage("sink", sink1Config);
    // duplicate name for 2nd sink, should throw exception
    ETLStage sink2 = new ETLStage("sink", sink2Config);

    ETLBatchConfig etlConfig = new ETLBatchConfig("* * * * *",
                                                  source,
                                                  Lists.newArrayList(sink1, sink2),
                                                  Lists.<ETLStage>newArrayList(),
                                                  new ArrayList<Connection>(),
                                                  new Resources(),
                                                  Lists.<ETLStage>newArrayList());

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "FileToTPFS");

    // deploying would thrown an excpetion
    deployApplication(appId, appRequest);
  }
}
