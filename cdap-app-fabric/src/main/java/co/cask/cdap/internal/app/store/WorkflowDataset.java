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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.PercentileInformation;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowStatistics;
import com.google.common.primitives.Longs;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.api.RunId;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Dataset for Completed Workflows and their associated programs
 */
public class WorkflowDataset extends AbstractDataset {

  private static final Gson GSON = new Gson();
  private static final byte[] RUNID = Bytes.toBytes("r");
  private static final byte[] TIME_TAKEN = Bytes.toBytes("t");
  private static final byte[] NODES = Bytes.toBytes("n");
  private static final Type PROGRAM_RUNS_TYPE = new TypeToken<List<ProgramRun>>() { }.getType();

  private final Table table;

  WorkflowDataset(Table table) {
    super("workflow.statistics", table);
    this.table = table;
  }

  void write(Id.Workflow id, RunRecordMeta runRecordMeta, List<ProgramRun> programRunList) {
    long start = runRecordMeta.getStartTs();

    MDSKey mdsKey = new MDSKey.Builder().add(id.getApplication().getNamespaceId())
      .add(id.getApplicationId()).add(id.getId()).add(start).build();
    byte[] rowKey = mdsKey.getKey();
    long timeTaken = runRecordMeta.getStopTs() - start;

    String value = GSON.toJson(programRunList, PROGRAM_RUNS_TYPE);

    table.put(rowKey, RUNID, Bytes.toBytes(runRecordMeta.getPid()));
    table.put(rowKey, TIME_TAKEN, Bytes.toBytes(timeTaken));
    table.put(rowKey, NODES, Bytes.toBytes(value));
  }

  /**
   * This function scans the workflow.stats dataset for a list of workflow runs in a time range.
   *
   * @param id The workflow id
   * @param timeRangeStart Start of the time range that the scan should begin from
   * @param timeRangeEnd End of the time range that the scan should end at
   * @return List of WorkflowRunRecords
   * @throws Exception
   */
  private List<WorkflowRunRecord> scan(Id.Workflow id, long timeRangeStart, long timeRangeEnd) throws Exception {
    byte[] startRowKey = new MDSKey.Builder().add(id.getApplication().getNamespaceId()).add(id.getApplicationId()).
      add(id.getId()).add(timeRangeStart).build().getKey();
    byte[] endRowKey = new MDSKey.Builder().add(id.getApplication().getNamespaceId()).add(id.getApplicationId()).
      add(id.getId()).add(timeRangeEnd).build().getKey();
    Scan scan = new Scan(startRowKey, endRowKey);

    Scanner scanner = table.scan(scan);
    Row indexRow;
    List<WorkflowRunRecord> workflowRunRecordList = new ArrayList<>();
    while ((indexRow = scanner.next()) != null) {
      Map<byte[], byte[]> columns = indexRow.getColumns();
      String workflowRunId = Bytes.toString(columns.get(RUNID));
      long timeTaken = Bytes.toLong(columns.get(TIME_TAKEN));

      List<ProgramRun> programRunList = GSON.fromJson(Bytes.toString(columns.get(NODES)), PROGRAM_RUNS_TYPE);
      WorkflowRunRecord workflowRunRecord = new WorkflowRunRecord(workflowRunId, timeTaken, programRunList);
      workflowRunRecordList.add(workflowRunRecord);
    }
    return workflowRunRecordList;
  }

  /**
   * This method returns the statistics for a corresponding workflow. The user has to
   * provide a time interval and a list of percentiles that are required.
   *
   * @param id The workflow id
   * @param startTime The start of the time range from where the user wants the statistics
   * @param endTime The end of the time range till where the user wants the statistics
   * @param percentiles The list of percentiles that the user wants information on
   * @return A statistics object that provides information about the workflow or null if there are no runs associated
   * with the workflow
   * @throws Exception
   */
  @Nullable
  public WorkflowStatistics getStatistics(Id.Workflow id, long startTime,
                                          long endTime, List<Double> percentiles) throws Exception {
    List<WorkflowRunRecord> workflowRunRecords = scan(id, startTime, endTime);
    int runs = workflowRunRecords.size();

    if (runs == 0) {
      return null;
    }

    double avgRunTime = 0.0;
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      avgRunTime += workflowRunRecord.getTimeTaken();
    }
    avgRunTime /= runs;

    workflowRunRecords = sort(workflowRunRecords);

    List<PercentileInformation> percentileInformationList = getPercentiles(workflowRunRecords, percentiles);

    Collection<ProgramRunDetails> programToRunRecord = getProgramRuns(workflowRunRecords);

    Map<String, Map<String, String>> programToStatistic = new HashMap<>();
    for (ProgramRunDetails entry : programToRunRecord) {
      double avgForProgram = 0;
      for (long value : entry.getProgramRunList()) {
        avgForProgram += value;
      }
      avgForProgram /= entry.getProgramRunList().size();
      Map<String, String> programMap = new HashMap<>();
      programMap.put("type", entry.getProgramType().toString());
      programMap.put("runs", Long.toString(entry.getProgramRunList().size()));
      programMap.put("avgRunTime", Double.toString(avgForProgram));
      programToStatistic.put(entry.getName(), programMap);
      List<Long> runList = entry.getProgramRunList();
      Collections.sort(runList);
      for (double percentile : percentiles) {
        long percentileValue = runList.get((int) ((percentile * runList.size()) / 100));
        programToStatistic.get(entry.getName()).put(Double.toString(percentile), Long.toString(percentileValue));
      }
    }

    return new WorkflowStatistics(startTime, endTime, runs, avgRunTime, percentileInformationList,
                                  programToStatistic);
  }

  private List<PercentileInformation> getPercentiles(List<WorkflowRunRecord> workflowRunRecords,
                                                     List<Double> percentiles) {
    int runs = workflowRunRecords.size();
    List<PercentileInformation> percentileInformationList = new ArrayList<>();
    for (double i : percentiles) {
      List<String> percentileRun = new ArrayList<>();
      int percentileStart = (int) ((i * runs) / 100);
      for (int j = percentileStart; j < runs; j++) {
        percentileRun.add(workflowRunRecords.get(j).getWorkflowRunId());
      }
      percentileInformationList.add(
        new PercentileInformation(i, workflowRunRecords.get(percentileStart).getTimeTaken(), percentileRun));
    }
    return percentileInformationList;
  }

  private List<WorkflowDataset.WorkflowRunRecord> sort(List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords) {
    Collections.sort(workflowRunRecords, new Comparator<WorkflowRunRecord>() {
      @Override
      public int compare(WorkflowDataset.WorkflowRunRecord o1, WorkflowDataset.WorkflowRunRecord o2) {
        return Longs.compare(o1.getTimeTaken(), o2.getTimeTaken());
      }
    });
    return workflowRunRecords;
  }

  private Collection<ProgramRunDetails> getProgramRuns(List<WorkflowDataset.WorkflowRunRecord> workflowRunRecords) {
    Map<String, ProgramRunDetails> programToRunRecord = new HashMap<>();
    for (WorkflowDataset.WorkflowRunRecord workflowRunRecord : workflowRunRecords) {
      for (WorkflowDataset.ProgramRun run : workflowRunRecord.getProgramRuns()) {
        ProgramRunDetails programRunDetails = programToRunRecord.get(run.getName());
        if (programRunDetails == null) {
          programRunDetails = new ProgramRunDetails(run.getName(), run.getProgramType(), new ArrayList<Long>());
          programToRunRecord.put(run.getName(), programRunDetails);
        }
        programRunDetails.addToProgramRunList(run.getTimeTaken());
      }
    }
    return programToRunRecord.values();
  }

  @Nullable
  WorkflowRunRecord getRecord(Id.Workflow id, String runId) {
    RunId pid = RunIds.fromString(runId);
    long startTime = RunIds.getTime(pid, TimeUnit.SECONDS);
    MDSKey mdsKey = new MDSKey.Builder().add(id.getNamespaceId())
      .add(id.getApplicationId()).add(id.getId()).add(startTime).build();
    byte[] startRowKey = mdsKey.getKey();
    Scan scan = new Scan(startRowKey, null);

    WorkflowRunRecord workflowRunRecord;
    Scanner scanner = table.scan(scan);
    Row indexRow = scanner.next();
    if (indexRow == null) {
      return null;
    }
    Map<byte[], byte[]> columns = indexRow.getColumns();
    String workflowRunId = Bytes.toString(columns.get(RUNID));
    long timeTaken = Bytes.toLong(columns.get(TIME_TAKEN));

    List<ProgramRun> actionRunsList = GSON.fromJson(Bytes.toString(columns.get(NODES)), PROGRAM_RUNS_TYPE);
    workflowRunRecord = new WorkflowRunRecord(workflowRunId, timeTaken, actionRunsList);

    if (!runId.equals(workflowRunRecord.getWorkflowRunId())) {
      return null;
    } else {
      return workflowRunRecord;
    }
  }

  Map<String, WorkflowRunRecord> getDetailsOfRange(Id.Workflow workflow, String runId, int count, long timeInterval) {
    Map<String, WorkflowRunRecord> mainRunRecords = getNeighbors(workflow, runId, count, timeInterval);
    WorkflowRunRecord workflowRunRecord = getRecord(workflow, runId);
    if (workflowRunRecord != null) {
      mainRunRecords.put(workflowRunRecord.getWorkflowRunId(), workflowRunRecord);
    }
    return mainRunRecords;
  }

  private Map<String, WorkflowRunRecord> getNeighbors(Id.Workflow id, String runId, int count, long timeInterval) {
    RunId pid = RunIds.fromString(runId);
    long startTime = RunIds.getTime(pid, TimeUnit.SECONDS);
    Map<String, WorkflowRunRecord> workflowRunRecords = new HashMap<>();
    for (int i = (-1 * count); i <= count; i++) {
      long prevStartTime = startTime + (i * timeInterval);
      MDSKey mdsKey = new MDSKey.Builder().add(id.getNamespaceId())
        .add(id.getApplicationId()).add(id.getId()).add(prevStartTime).build();
      byte[] startRowKey = mdsKey.getKey();
      Scan scan = new Scan(startRowKey, null);
      Scanner scanner = table.scan(scan);
      Row indexRow = scanner.next();
      if (indexRow == null) {
        return workflowRunRecords;
      }
      mdsKey = new MDSKey(indexRow.getRow());
      MDSKey.Splitter splitter = mdsKey.split();
      splitter.skipString();
      splitter.skipString();
      splitter.skipString();
      long time = splitter.getLong();
      if ((time >= (startTime - (count * timeInterval))) && time <= (startTime + (count * timeInterval))) {
        Map<byte[], byte[]> columns = indexRow.getColumns();
        String workflowRunId = Bytes.toString(columns.get(RUNID));
        long timeTaken = Bytes.toLong(columns.get(TIME_TAKEN));
        List<ProgramRun> programRunList = GSON.fromJson(Bytes.toString(columns.get(NODES)), PROGRAM_RUNS_TYPE);
        workflowRunRecords.put(workflowRunId, new WorkflowRunRecord(workflowRunId, timeTaken, programRunList));
      } else {
        break;
      }
    }
    return workflowRunRecords;
  }

  /**
   * Class to store the name, type and list of runs of the programs across all workflow runs
   */
  static class ProgramRunDetails {
    private final String name;
    private final ProgramType programType;
    private final List<Long> programRunList;

    public ProgramRunDetails(String name, ProgramType programType, List<Long> programRunList) {
      this.name = name;
      this.programType = programType;
      this.programRunList = programRunList;
    }

    public void addToProgramRunList(long time) {
      this.programRunList.add(time);
    }

    public String getName() {
      return name;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    public List<Long> getProgramRunList() {
      return programRunList;
    }
  }

  /**
   * Internal class to keep track of Workflow Run Records
   */
  public static class WorkflowRunRecord {
    private final String workflowRunId;
    private final long timeTaken;
    private final List<ProgramRun> programRuns;

    public WorkflowRunRecord(String workflowRunId, long timeTaken, List<ProgramRun> programRuns) {
      this.programRuns = programRuns;
      this.timeTaken = timeTaken;
      this.workflowRunId = workflowRunId;
    }

    public long getTimeTaken() {
      return timeTaken;
    }

    public List<ProgramRun> getProgramRuns() {
      return programRuns;
    }

    public String getWorkflowRunId() {
      return workflowRunId;
    }
  }

  /**
   * Internal Class for keeping track of programs in a workflow
   */
  public static class ProgramRun {
    private final String runId;
    private final long timeTaken;
    private final ProgramType programType;
    private final String name;

    public ProgramRun(String name, String runId, ProgramType programType, long timeTaken) {
      this.name = name;
      this.runId = runId;
      this.programType = programType;
      this.timeTaken = timeTaken;
    }

    public ProgramType getProgramType() {
      return programType;
    }

    public long getTimeTaken() {
      return timeTaken;
    }

    public String getName() {
      return name;
    }

    public String getRunId() {
      return runId;
    }
  }
}
