/*
 * Copyright 2015 Cask Data, Inc.
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

package co.cask.cdap.metrics.store;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.metrics.store.cube.Aggregation;
import co.cask.cdap.metrics.store.cube.Cube;
import co.cask.cdap.metrics.store.cube.CubeDeleteQuery;
import co.cask.cdap.metrics.store.cube.CubeExploreQuery;
import co.cask.cdap.metrics.store.cube.CubeFact;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.cube.DefaultAggregation;
import co.cask.cdap.metrics.store.cube.DefaultCube;
import co.cask.cdap.metrics.store.cube.FactTableSupplier;
import co.cask.cdap.metrics.store.cube.TimeSeries;
import co.cask.cdap.metrics.store.timeseries.FactTable;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
import co.cask.cdap.metrics.store.timeseries.TagValue;
import co.cask.cdap.metrics.store.timeseries.TimeValue;
import co.cask.cdap.metrics.transport.MetricType;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DefaultMetricStore implements MetricStore {
  private static final String PROGRAM_LEVEL2 = "pr2";
  private static final String PROGRAM_LEVEL3 = "pr3";
  private static final String PROGRAM_LEVEL4 = "pr4";

  private final Supplier<Cube> cube;
  private final Map<String, String> tagMapping;

  @Inject
  public DefaultMetricStore(final MetricDatasetFactory dsFactory) {
    final FactTableSupplier factTableSupplier = new FactTableSupplier() {
      @Override
      public FactTable get(int resolution, int ignoredRollTime) {
        // roll time will be taken from configuration todo: clean this up
        return dsFactory.get(resolution);
      }
    };
    this.cube = Suppliers.memoize(new Supplier<Cube>() {
      @Override
      public Cube get() {
        // 1 sec, 1 min, 1 hour and "all time totals"
        return new DefaultCube(new int[] {1, 60, 3600, Integer.MAX_VALUE}, factTableSupplier, createAggregations());
      }
    });

    // NOTE: to reduce number of aggregations we rename some of the emitted tags to "canonical" names
    this.tagMapping = ImmutableMap.of(
      // flow
      Constants.Metrics.Tag.FLOWLET, PROGRAM_LEVEL2,
      Constants.Metrics.Tag.FLOWLET_QUEUE, PROGRAM_LEVEL3,
      // mapreduce
      Constants.Metrics.Tag.MR_TASK_TYPE, PROGRAM_LEVEL2,
      Constants.Metrics.Tag.INSTANCE_ID, PROGRAM_LEVEL3,
      // service
      Constants.Metrics.Tag.SERVICE_RUNNABLE, PROGRAM_LEVEL2
    );
  }

  private static List<Aggregation> createAggregations() {
    List<Aggregation> aggs = Lists.newLinkedList();

    // <cluster metrics>, e.g. storage used
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.CLUSTER_METRICS)));

    // app, prg type, prg name, ...
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
      // todo: do we even need program type? seems like program name unique within app across program types
      Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM, Constants.Metrics.Tag.RUN_ID,
      PROGRAM_LEVEL2, PROGRAM_LEVEL3, PROGRAM_LEVEL4, Constants.Metrics.Tag.DATASET),
                                    // i.e. for programs only
                                    ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.APP,
      Constants.Metrics.Tag.PROGRAM_TYPE, Constants.Metrics.Tag.PROGRAM)));

    // component, handler, method
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.COMPONENT,
      Constants.Metrics.Tag.HANDLER, Constants.Metrics.Tag.METHOD),
                                    // i.e. for components only
                                    ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.COMPONENT)));

    // component, handler, method, stream (for stream only) todo: seems like emitted context is wrong, review...
    aggs.add(new DefaultAggregation(ImmutableList.of(
      Constants.Metrics.Tag.NAMESPACE,
      Constants.Metrics.Tag.COMPONENT, Constants.Metrics.Tag.HANDLER, Constants.Metrics.Tag.METHOD,
      Constants.Metrics.Tag.STREAM),
                                    // i.e. for stream only
                                    ImmutableList.of(Constants.Metrics.Tag.STREAM)));

    // dataset
    aggs.add(new DefaultAggregation(ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.DATASET),
                                    // i.e. for datasets only
                                    ImmutableList.of(Constants.Metrics.Tag.NAMESPACE, Constants.Metrics.Tag.DATASET)));

    return aggs;
  }

  @Override
  public void add(MetricValue metricValue) throws Exception {
    String scope = metricValue.getTags().get(Constants.Metrics.Tag.SCOPE);
    String measureName = (scope == null ? "system." : scope + ".") + metricValue.getName();

    CubeFact fact = new CubeFact(replaceTagsIfNeeded(metricValue.getTags()),
                                 toMeasureType(metricValue.getType()), measureName,
                                 new TimeValue(metricValue.getTimestamp(), metricValue.getValue()));
    cube.get().add(fact);
  }

  @Override
  public Collection<TimeSeries> query(CubeQuery query) throws Exception {
    CubeQuery q =
      new CubeQuery(query, replaceTagsIfNeeded(query.getSliceByTags()), replaceTagsIfNeeded(query.getGroupByTags()));
    Collection<TimeSeries> cubeResult = cube.get().query(q);
    List<TimeSeries> result = Lists.newArrayList();
    for (TimeSeries timeSeries : cubeResult) {
      result.add(new TimeSeries(timeSeries, replaceTagsIfNeeded(timeSeries.getTagValues())));
    }
    return result;
  }

  @Override
  public void deleteBefore(long timestamp) throws Exception {
    // delete all data before the timestamp. null for MeasureName indicates match any MeasureName.
    // note: We are using 1 as start ts, so that we do not delete data from "totals". This method is applied in
    //       in-memory and standalone modes, so it is fine to keep totals in these cases during TTL
    //       todo: Cube and FactTable must use resolution when applying time range conditions
    CubeDeleteQuery query = new CubeDeleteQuery(1, timestamp, null, Maps.<String, String>newHashMap());
    cube.get().delete(query);
  }

  @Override
  public void delete(CubeDeleteQuery query) throws Exception {
    CubeDeleteQuery transformedQuery = new CubeDeleteQuery(query, replaceTagsIfNeeded(query.getSliceByTags()));
    cube.get().delete(transformedQuery);
  }

  private void replaceTagValuesIfNeeded(List<TagValue> tagValues) {
    for (int i = 0; i < tagValues.size(); i++) {
      TagValue tagValue = tagValues.get(i);
      String tagNameReplacement = tagMapping.get(tagValue.getTagName());
      if (tagNameReplacement != null) {
        tagValues.set(i, new TagValue(tagNameReplacement, tagValue.getValue()));
      }
    }
  }

  @Override
  public Collection<TagValue> findNextAvailableTags(CubeExploreQuery query) throws Exception {
    replaceTagValuesIfNeeded(query.getTagValues());
    return cube.get().findNextAvailableTags(query);
  }

  @Override
  public Collection<String> findMetricNames(CubeExploreQuery query) throws Exception {
    replaceTagValuesIfNeeded(query.getTagValues());
    return cube.get().getMeasureNames(query);
  }

  private Map<String, String> replaceTagsIfNeeded(Map<String, String> tagValues) {
    // replace emitted tag names to the ones expected by aggregations
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> tagValue : tagValues.entrySet()) {
      String tagNameReplacement = tagMapping.get(tagValue.getKey());
      result.put(tagNameReplacement == null ? tagValue.getKey() : tagNameReplacement, tagValue.getValue());
    }
    return result;
  }

  private List<String> replaceTagsIfNeeded(List<String> tagNames) {
    // replace emitted tag names to the ones expected by aggregations
    List<String> result = Lists.newArrayList();
    for (String tagName : tagNames) {
      String tagNameReplacement = tagMapping.get(tagName);
      result.add(tagNameReplacement == null ? tagName : tagNameReplacement);
    }
    return result;
  }

  private MeasureType toMeasureType(MetricType type) {
    switch (type) {
      case COUNTER:
        return MeasureType.COUNTER;
      case GAUGE:
        return MeasureType.GAUGE;
      default:
        // should never happen
        throw new IllegalArgumentException("Unknown MetricType: " + type);
    }
  }
}
