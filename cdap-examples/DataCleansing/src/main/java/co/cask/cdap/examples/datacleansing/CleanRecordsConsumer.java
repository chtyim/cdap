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

package co.cask.cdap.examples.datacleansing;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.partitioned.KVTableStatePersistor;
import co.cask.cdap.api.dataset.lib.partitioned.PartitionBatchInput;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple MapReduce that reads records from the rawRecords PartitionedFileSet and writes all records
 * that match a particular {@link Schema} to the cleanRecords PartitionedFileSet. It also keeps track of its state of
 * which partitions it has processed, so that it only processes new partitions of data each time it runs.
 */
public class CleanRecordsConsumer extends AbstractMapReduce {
  protected static final String NAME = "CleanRecordsConsumer";

  private PartitionBatchInput.BatchPartitionCommitter partitionCommitter;

  @Override
  public void configure() {
    setName(NAME);
    setMapperResources(new Resources(1024));
    setReducerResources(new Resources(1024));
  }

  @Override
  public void beforeSubmit(MapReduceContext context) throws Exception {
    partitionCommitter =
      PartitionBatchInput.setInput(context, DataCleansing.CLEAN_RECORDS,
                                   new KVTableStatePersistor(DataCleansing.CONSUMING_STATE, "state.key.2"));

    Map<String, String> arguments = new HashMap<>();
    PartitionedFileSetArguments.setOutputPartitionKey(arguments,
                                                      PartitionKey.builder()
                                                        .addLongField("time", System.currentTimeMillis())
                                                        .build());
    context.addOutput(DataCleansing.INVALID_RECORDS, arguments);

    Job job = context.getHadoopJob();
    job.setMapperClass(SchemaMatchingFilter.class);
    job.setNumReduceTasks(0);
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    partitionCommitter.onFinish(succeeded);
  }

  /**
   * A Mapper which skips text that doesn't match a given schema.
   */
  public static class SchemaMatchingFilter extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      context.write(NullWritable.get(), value);
    }
  }
}
