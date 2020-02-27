package com.github.quartz.impl.redisjobstore;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;
import java.util.TimeZone;

public class RedisJobStoreTest {

    private static Scheduler scheduler;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Properties properties = new Properties();
        properties.put("org.quartz.scheduler.instanceName", "MyRedisTestScheduler");
        properties.put("org.quartz.jobStore.class", "com.github.quartz.impl.redisjobstore.RedisJobStore");

        properties.put("org.quartz.scheduler.instanceId", "AUTO");
        properties.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
        properties.put("org.quartz.threadPool.threadCount", "25");
        properties.put("org.quartz.threadPool.threadPriority", "5");
        properties.put("org.quartz.jobStore.misfireThreshold", "60000");
        properties.put("org.quartz.jobStore.isClustered", true);
        properties.put("org.quartz.jobStore.clusterCheckinInterval", "20000");

        SchedulerFactory sf = new StdSchedulerFactory(properties);
        scheduler = sf.getScheduler();
        scheduler.clear();
        // scheduler.start();
    }

    @Test
    public void testAddJob() throws SchedulerException {
        JobKey jobKey = new JobKey("job1", "group1");
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .usingJobData("pi", 3.1415926)
                .withDescription("测试")
                .storeDurably(true)
                .requestRecovery(true)
                .build();
        scheduler.addJob(jobDetail, true);
        JobDetail newValue = scheduler.getJobDetail(jobKey);
        Assert.assertEquals(jobDetail.getKey(), newValue.getKey());
        Assert.assertEquals(jobDetail.getDescription(), newValue.getDescription());
        Assert.assertEquals(jobDetail.getJobClass(), newValue.getJobClass());
        Assert.assertEquals(jobDetail.getJobDataMap(), newValue.getJobDataMap());
        Assert.assertEquals(jobDetail.isDurable(), newValue.isDurable());
        Assert.assertEquals(jobDetail.requestsRecovery(), newValue.requestsRecovery());
        Assert.assertEquals(jobDetail.isConcurrentExectionDisallowed(), newValue.isConcurrentExectionDisallowed());
        Assert.assertEquals(jobDetail.isPersistJobDataAfterExecution(), newValue.isPersistJobDataAfterExecution());
        boolean delResult = scheduler.deleteJob(jobKey);
        Assert.assertTrue(delResult);
        JobDetail deletedValue = scheduler.getJobDetail(jobKey);
        Assert.assertNull(deletedValue);
    }

    @Test
    public void testDeleteNonExistentJob() throws SchedulerException {
        JobKey jobKey = new JobKey(String.valueOf(Math.random()), String.valueOf(Math.random()));
        boolean delResult = scheduler.deleteJob(jobKey);
        Assert.assertFalse(delResult);
    }

    @Test
    public void testSchedulerJobByCronTrigger() throws SchedulerException {
        JobKey jobKey = new JobKey("job1", "group1");
        TriggerKey triggerKey = new TriggerKey("trigger1", "group1");
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .usingJobData("pi", 3.1415926)
                .withDescription("测试")
                .storeDurably(false)
                .requestRecovery(true)
                .build();
        CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                .withSchedule(
                        CronScheduleBuilder.cronSchedule("0/10 * * * * ?")
                                .inTimeZone(TimeZone.getTimeZone("America/Los_Angeles"))
                                .withMisfireHandlingInstructionIgnoreMisfires())
                .withDescription("cron表达式触发器")
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
        CronTrigger newValue = (CronTrigger) scheduler.getTrigger(triggerKey);
        Assert.assertEquals(trigger.getKey(), newValue.getKey());
        Assert.assertEquals(trigger.getDescription(), newValue.getDescription());
        Assert.assertEquals(trigger.getJobDataMap(), newValue.getJobDataMap());
        Assert.assertEquals(trigger.getMisfireInstruction(), newValue.getMisfireInstruction());
        Assert.assertEquals(trigger.getPriority(), newValue.getPriority());
        Assert.assertEquals(trigger.getStartTime(), newValue.getStartTime());
        Assert.assertEquals(trigger.getEndTime(), newValue.getEndTime());

        Assert.assertEquals(trigger.getCronExpression(), newValue.getCronExpression());
        Assert.assertEquals(trigger.getTimeZone(), newValue.getTimeZone());
    }

    @Test
    public void testSchedulerJobBySimpleTrigger() throws SchedulerException {
        JobKey jobKey = new JobKey("job1", "group1");
        TriggerKey triggerKey = new TriggerKey("trigger1", "group1");
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .usingJobData("pi", 3.1415926)
                .withDescription("测试")
                .storeDurably(false)
                .requestRecovery(true)
                .build();
        SimpleTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                .withSchedule(
                        SimpleScheduleBuilder.simpleSchedule()
                                .withIntervalInSeconds(10)
                                .withRepeatCount(5)
                                .withMisfireHandlingInstructionNowWithExistingCount())
                .withDescription("simple触发器")
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
        SimpleTrigger newValue = (SimpleTrigger) scheduler.getTrigger(triggerKey);
        Assert.assertEquals(trigger.getKey(), newValue.getKey());
        Assert.assertEquals(trigger.getDescription(), newValue.getDescription());
        Assert.assertEquals(trigger.getJobDataMap(), newValue.getJobDataMap());
        Assert.assertEquals(trigger.getMisfireInstruction(), newValue.getMisfireInstruction());
        Assert.assertEquals(trigger.getPriority(), newValue.getPriority());
        Assert.assertEquals(trigger.getStartTime(), newValue.getStartTime());
        Assert.assertEquals(trigger.getEndTime(), newValue.getEndTime());

        Assert.assertEquals(trigger.getRepeatCount(), newValue.getRepeatCount());
        Assert.assertEquals(trigger.getRepeatInterval(), newValue.getRepeatInterval());
    }

    @Test
    public void testSchedulerJobByDailyTimeIntervalTrigger() throws SchedulerException {
        JobKey jobKey = new JobKey("job1", "group1");
        TriggerKey triggerKey = new TriggerKey("trigger1", "group1");
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .usingJobData("pi", 3.1415926)
                .withDescription("测试")
                .storeDurably(false)
                .requestRecovery(true)
                .build();
        DailyTimeIntervalTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                .withSchedule(
                        DailyTimeIntervalScheduleBuilder.dailyTimeIntervalSchedule()
                                .startingDailyAt(TimeOfDay.hourMinuteAndSecondOfDay(1, 1, 1))
                                .endingDailyAt(TimeOfDay.hourMinuteAndSecondOfDay(2, 2, 2))
                                .onDaysOfTheWeek(1, 3, 5)
                                .withMisfireHandlingInstructionFireAndProceed()
                                .withIntervalInSeconds(10)
                                .withRepeatCount(5))
                .withDescription("simple触发器")
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
        DailyTimeIntervalTrigger newValue = (DailyTimeIntervalTrigger) scheduler.getTrigger(triggerKey);
        Assert.assertEquals(trigger.getKey(), newValue.getKey());
        Assert.assertEquals(trigger.getDescription(), newValue.getDescription());
        Assert.assertEquals(trigger.getJobDataMap(), newValue.getJobDataMap());
        Assert.assertEquals(trigger.getMisfireInstruction(), newValue.getMisfireInstruction());
        Assert.assertEquals(trigger.getPriority(), newValue.getPriority());
        Assert.assertEquals(trigger.getStartTime(), newValue.getStartTime());
        Assert.assertEquals(trigger.getEndTime(), newValue.getEndTime());

        Assert.assertEquals(trigger.getRepeatCount(), newValue.getRepeatCount());
        Assert.assertEquals(trigger.getRepeatInterval(), newValue.getRepeatInterval());
        Assert.assertEquals(trigger.getDaysOfWeek(), newValue.getDaysOfWeek());
        Assert.assertEquals(trigger.getStartTimeOfDay(), newValue.getStartTimeOfDay());
        Assert.assertEquals(trigger.getEndTimeOfDay(), newValue.getEndTimeOfDay());
    }

    @Test
    public void testSchedulerJobByCalendarIntervalTrigger() throws SchedulerException {
        JobKey jobKey = new JobKey("job1", "group1");
        TriggerKey triggerKey = new TriggerKey("trigger1", "group1");
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .usingJobData("pi", 3.1415926)
                .withDescription("测试")
                .storeDurably(false)
                .requestRecovery(true)
                .build();
        CalendarIntervalTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                .withSchedule(
                        CalendarIntervalScheduleBuilder.calendarIntervalSchedule()
                                .inTimeZone(TimeZone.getTimeZone("America/Los_Angeles"))
                                .skipDayIfHourDoesNotExist(true)
                                .preserveHourOfDayAcrossDaylightSavings(true)
                                .withMisfireHandlingInstructionFireAndProceed()
                                .withIntervalInDays(10)
                                .withRepeatCount(5))
                .withDescription("simple触发器")
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
        CalendarIntervalTrigger newValue = (CalendarIntervalTrigger) scheduler.getTrigger(triggerKey);
        Assert.assertEquals(trigger.getKey(), newValue.getKey());
        Assert.assertEquals(trigger.getDescription(), newValue.getDescription());
        Assert.assertEquals(trigger.getJobDataMap(), newValue.getJobDataMap());
        Assert.assertEquals(trigger.getMisfireInstruction(), newValue.getMisfireInstruction());
        Assert.assertEquals(trigger.getPriority(), newValue.getPriority());
        Assert.assertEquals(trigger.getStartTime(), newValue.getStartTime());
        Assert.assertEquals(trigger.getEndTime(), newValue.getEndTime());

        Assert.assertEquals(trigger.getRepeatCount(), newValue.getRepeatCount());
        Assert.assertEquals(trigger.getRepeatInterval(), newValue.getRepeatInterval());
        Assert.assertEquals(trigger.getTimeZone(), newValue.getTimeZone());
        Assert.assertEquals(trigger.isSkipDayIfHourDoesNotExist(), newValue.isSkipDayIfHourDoesNotExist());
        Assert.assertEquals(trigger.isPreserveHourOfDayAcrossDaylightSavings(), newValue.isPreserveHourOfDayAcrossDaylightSavings());
    }

    @Test
    public void testSchedulerJobByCalendarOffsetTrigger() throws SchedulerException {
        JobKey jobKey = new JobKey("job1", "group1");
        TriggerKey triggerKey = new TriggerKey("trigger1", "group1");
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .usingJobData("pi", 3.1415926)
                .withDescription("测试")
                .storeDurably(false)
                .requestRecovery(true)
                .build();
        CalendarOffsetTrigger trigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                .withSchedule(
                        CalendarOffsetScheduleBuilder.calendarOffsetSchedule()
                                .startingDailyAt(TimeOfDay.hourMinuteAndSecondOfDay(1, 1, 1))
                                .withInnerOffset(1)
                                .withOuterOffset(1)
                                .withIntervalUnitOfWeek()
                                .reversed(true)
                                .withMisfireHandlingInstructionFireAndProceed()
                                .withRepeatCount(5))
                .withDescription("simple触发器")
                .build();
        scheduler.scheduleJob(jobDetail, trigger);
        CalendarOffsetTrigger newValue = (CalendarOffsetTrigger) scheduler.getTrigger(triggerKey);
        Assert.assertEquals(trigger.getKey(), newValue.getKey());
        Assert.assertEquals(trigger.getDescription(), newValue.getDescription());
        Assert.assertEquals(trigger.getJobDataMap(), newValue.getJobDataMap());
        Assert.assertEquals(trigger.getMisfireInstruction(), newValue.getMisfireInstruction());
        Assert.assertEquals(trigger.getPriority(), newValue.getPriority());
        Assert.assertEquals(trigger.getStartTime(), newValue.getStartTime());
        Assert.assertEquals(trigger.getEndTime(), newValue.getEndTime());

        Assert.assertEquals(trigger.getRepeatCount(), newValue.getRepeatCount());
        Assert.assertEquals(trigger.getStartTimeOfDay(), newValue.getStartTimeOfDay());
        Assert.assertEquals(trigger.getInnerOffset(), newValue.getInnerOffset());
        Assert.assertEquals(trigger.getOuterOffset(), newValue.getOuterOffset());
        Assert.assertEquals(trigger.getRepeatIntervalUnit(), newValue.getRepeatIntervalUnit());
        Assert.assertEquals(trigger.isReversed(), newValue.isReversed());
    }

}
