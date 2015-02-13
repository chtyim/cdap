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

package co.cask.cdap.internal.app.runtime.schedule;

import co.cask.cdap.AppWithWorkflow;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.schedule.StreamSizeSchedule;
import co.cask.cdap.api.schedule.TimeSchedule;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.proto.Id;
import co.cask.cdap.test.internal.AppFabricTestHelper;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class SchedulerServiceTest {
  public static SchedulerService schedulerService;
  public static NotificationFeedManager notificationFeedManager;
  public static StreamAdmin streamAdmin;

  private static final Id.Namespace account = new Id.Namespace(Constants.DEFAULT_NAMESPACE);
  private static final Id.Application appId = new Id.Application(account, AppWithWorkflow.NAME);
  private static final Id.Program program = new Id.Program(appId, AppWithWorkflow.SampleWorkflow.NAME);
  private static final SchedulableProgramType programType = SchedulableProgramType.WORKFLOW;
  private static final String STREAM_NAME = "stream";
  private static final Schedule timeSchedule1 = new TimeSchedule("Schedule1", "Every minute", "* * * * ?");
  private static final Schedule timeSchedule2 = new TimeSchedule("Schedule2", "Every Hour", "0 * * * ?");
  private static final Schedule dataSchedule1 = new StreamSizeSchedule("Schedule3", "Every 1M", STREAM_NAME, 1);
  private static final Schedule dataSchedule2 = new StreamSizeSchedule("Schedule4", "Every 10M", STREAM_NAME, 10);

  @BeforeClass
  public static void set() throws Exception {
    schedulerService = AppFabricTestHelper.getInjector().getInstance(SchedulerService.class);
    notificationFeedManager = AppFabricTestHelper.getInjector().getInstance(NotificationFeedManager.class);
    streamAdmin = AppFabricTestHelper.getInjector().getInstance(StreamAdmin.class);
    streamAdmin.create(STREAM_NAME);
  }

  @AfterClass
  public static void finish() {
    schedulerService.stopAndWait();
  }

  @Test
  public void testSchedulesAcrossNamespace() throws Exception {
    AppFabricTestHelper.deployApplication(AppWithWorkflow.class);
    schedulerService.schedule(program, programType, ImmutableList.of(timeSchedule1));

    Id.Program programInOtherNamespace =
      Id.Program.from(new Id.Application(new Id.Namespace("otherNamespace"), appId.getId()), program.getId());

    List<String> scheduleIds = schedulerService.getScheduleIds(program, programType);
    Assert.assertEquals(1, scheduleIds.size());

    List<String> scheduleIdsOtherNamespace = schedulerService.getScheduleIds(programInOtherNamespace, programType);
    Assert.assertEquals(0, scheduleIdsOtherNamespace.size());

    schedulerService.schedule(programInOtherNamespace, programType, ImmutableList.of(timeSchedule2));

    scheduleIdsOtherNamespace = schedulerService.getScheduleIds(programInOtherNamespace, programType);
    Assert.assertEquals(1, scheduleIdsOtherNamespace.size());

    Assert.assertNotEquals(scheduleIds.get(0), scheduleIdsOtherNamespace.get(0));

  }

  @Test
  public void testSimpleSchedulerLifecycle() throws Exception {
    AppFabricTestHelper.deployApplication(AppWithWorkflow.class);

    schedulerService.schedule(program, programType, ImmutableList.of(timeSchedule1));
    List<String> scheduleIds = schedulerService.getScheduleIds(program, programType);
    Assert.assertEquals(1, scheduleIds.size());
    checkState(Scheduler.ScheduleState.SCHEDULED, scheduleIds);

    schedulerService.schedule(program, programType, timeSchedule2);
    scheduleIds = schedulerService.getScheduleIds(program, programType);
    Assert.assertEquals(2, scheduleIds.size());
    checkState(Scheduler.ScheduleState.SCHEDULED, scheduleIds);

    schedulerService.schedule(program, programType, ImmutableList.of(dataSchedule1, dataSchedule2));
    scheduleIds = schedulerService.getScheduleIds(program, programType);
    Assert.assertEquals(4, scheduleIds.size());
    checkState(Scheduler.ScheduleState.SCHEDULED, scheduleIds);

    schedulerService.suspendSchedule(program, SchedulableProgramType.WORKFLOW, "Schedule1");
    schedulerService.suspendSchedule(program, SchedulableProgramType.WORKFLOW, "Schedule2");

    checkState(Scheduler.ScheduleState.SUSPENDED, ImmutableList.of("Schedule1", "Schedule2"));
    checkState(Scheduler.ScheduleState.SCHEDULED, ImmutableList.of("Schedule3", "Schedule4"));

    schedulerService.suspendSchedule(program, SchedulableProgramType.WORKFLOW, "Schedule3");
    schedulerService.suspendSchedule(program, SchedulableProgramType.WORKFLOW, "Schedule4");

    checkState(Scheduler.ScheduleState.SUSPENDED, scheduleIds);

    schedulerService.deleteSchedules(program, programType);
    Assert.assertEquals(0, schedulerService.getScheduleIds(program, programType).size());

    // Check the state of the old scheduleIds
    // (which should be deleted by the call to SchedulerService#delete(Program, ProgramType)
    checkState(Scheduler.ScheduleState.NOT_FOUND, scheduleIds);
  }

  private void checkState(Scheduler.ScheduleState expectedState, List<String> scheduleIds) {
    for (String scheduleId : scheduleIds) {
      int i = scheduleId.lastIndexOf(':');
      Assert.assertEquals(expectedState, schedulerService.scheduleState(program, SchedulableProgramType.WORKFLOW,
                                                                        scheduleId.substring(i + 1)));
    }
  }
}
