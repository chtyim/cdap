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

package co.cask.cdap.data2.metadata.lineage;

import co.cask.cdap.proto.Id;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Service to compute Lineage based on Dataset accesses of a Program stored in {@link LineageStore}.
 */
public class LineageService {
  private static final Logger LOG = LoggerFactory.getLogger(LineageService.class);

  private static final Function<Relation, Id.Program> RELATION_TO_PROGRAM_FUNCTION =
    new Function<Relation, Id.Program>() {
      @Override
      public Id.Program apply(Relation input) {
        return input.getProgram();
      }
    };

  private static final Function<Relation, Id.NamespacedId> RELATION_TO_DATA_FUNCTION =
    new Function<Relation, Id.NamespacedId>() {
      @Override
      public Id.NamespacedId apply(Relation input) {
        return input.getData();
      }
    };

  private final LineageStore lineageStore;

  @Inject
  LineageService(LineageStore lineageStore) {
    this.lineageStore = lineageStore;
  }

  /**
   * Computes lineage for a dataset between given time period.
   *
   * @param sourceDataset dataset to compute lineage for
   * @param start start time period
   * @param end end time period
   * @param levels number of levels to compute lineage for
   * @return lineage for sourceDataset
   */
  public Lineage computeLineage(final Id.DatasetInstance sourceDataset, long start, long end, int levels) {
    return doComputeLineage(sourceDataset, start, end, levels);
  }

  /**
   * Computes lineage for a stream between given time period.
   *
   * @param sourceStream stream to compute lineage for
   * @param start start time period
   * @param end end time period
   * @param levels number of levels to compute lineage for
   * @return lineage for sourceStream
   */
  public Lineage computeLineage(final Id.Stream sourceStream, long start, long end, int levels) {
    return doComputeLineage(sourceStream, start, end, levels);
  }

  private Lineage doComputeLineage(final Id.NamespacedId sourceData, long start, long end, int levels) {
    LOG.trace("Computing lineage for data {}, start {}, end {}, levels {}", sourceData, start, end, levels);
    Set<Relation> relations = new HashSet<>();
    Set<Id.NamespacedId> visitedDatasets = new HashSet<>();
    Set<Id.NamespacedId> toVisitDatasets = new HashSet<>();
    Set<Id.Program> visitedPrograms = new HashSet<>();
    Set<Id.Program> toVisitPrograms = new HashSet<>();

    toVisitDatasets.add(sourceData);
    for (int i = 0; i < levels; ++i) {
      LOG.trace("Level {}", i);
      toVisitPrograms.clear();
      for (Id.NamespacedId d : toVisitDatasets) {
        if (!visitedDatasets.contains(d)) {
          LOG.trace("Visiting dataset {}", d);
          visitedDatasets.add(d);
          // Fetch related programs
          Iterable<Relation> programRelations = getProgramRelations(d, start, end);
          LOG.trace("Got program relations {}", programRelations);
          Iterables.addAll(relations, programRelations);
          Iterables.addAll(toVisitPrograms, Iterables.transform(programRelations, RELATION_TO_PROGRAM_FUNCTION));
        }
      }

      toVisitDatasets.clear();
      for (Id.Program p : toVisitPrograms) {
        if (!visitedPrograms.contains(p)) {
          LOG.trace("Visiting program {}", p);
          visitedPrograms.add(p);
          // Fetch related datasets
          Iterable<Relation> datasetRelations = lineageStore.getRelations(p, start, end);
          LOG.trace("Got data relations {}", datasetRelations);
          Iterables.addAll(relations, datasetRelations);
          Iterables.addAll(toVisitDatasets,
                           Iterables.transform(datasetRelations, RELATION_TO_DATA_FUNCTION));
        }
      }
    }

    Lineage lineage = new Lineage(relations);
    LOG.trace("Got lineage {}", lineage);
    return lineage;
  }

  private Iterable<Relation> getProgramRelations(Id.NamespacedId data, long start, long end) {
    if (data instanceof Id.DatasetInstance) {
      return lineageStore.getRelations((Id.DatasetInstance) data, start, end);
    }

    if (data instanceof Id.Stream) {
      return lineageStore.getRelations((Id.Stream) data, start, end);
    }

    throw new IllegalStateException("Unknown data type " + data);
  }
}
