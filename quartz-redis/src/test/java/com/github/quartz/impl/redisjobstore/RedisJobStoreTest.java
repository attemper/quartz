package com.github.quartz.impl.redisjobstore;

import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

public class RedisJobStoreTest {

    private static Scheduler scheduler;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Properties properties = new Properties();
        properties.put("org.quartz.scheduler.instanceName", "MyRedisScheduler");
        properties.put("org.quartz.scheduler.instanceId", "AUTO");
        properties.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
        properties.put("org.quartz.threadPool.threadCount", "25");
        properties.put("org.quartz.threadPool.threadPriority", "5");
        properties.put("org.quartz.jobStore.class", "com.github.quartz.impl.redisjobstore.RedisJobStore");
        properties.put("org.quartz.jobStore.misfireThreshold", "60000");

        properties.put("org.quartz.jobStore.isClustered", true);
        properties.put("org.quartz.jobStore.clusterCheckinInterval", "20000");

        SchedulerFactory sf = new StdSchedulerFactory(properties);
        scheduler = sf.getScheduler();
        scheduler.start();
    }

    @Test
    public void testStoreJob() throws SchedulerException {
        JobKey jobKey = new JobKey("job1", "group1");
        JobDetail oldValue = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .usingJobData("pi", 3.1415926)
                .withDescription("测试")
                .storeDurably(true)
                .requestRecovery(true)
                .build();
        scheduler.addJob(oldValue, true);
        JobDetail newValue = scheduler.getJobDetail(jobKey);
        Assert.assertEquals(oldValue.getKey(), newValue.getKey());
        Assert.assertEquals(oldValue.getDescription(), newValue.getDescription());
        Assert.assertEquals(oldValue.getJobClass(), newValue.getJobClass());
        Assert.assertEquals(oldValue.getJobDataMap(), newValue.getJobDataMap());
        Assert.assertEquals(oldValue.isDurable(), newValue.isDurable());
        Assert.assertEquals(oldValue.requestsRecovery(), newValue.requestsRecovery());
        Assert.assertEquals(oldValue.isConcurrentExectionDisallowed(), newValue.isConcurrentExectionDisallowed());
        Assert.assertEquals(oldValue.isPersistJobDataAfterExecution(), newValue.isPersistJobDataAfterExecution());
    }
}
