package com.github.quartz.nosqljobstore;

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
        properties.put("org.quartz.jobStore.class", "com.github.quartz.nosql.redis.RedisJobStore");
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
        JobDetail jobDetail = JobBuilder.newJob(HelloJob.class)
                .withIdentity(jobKey)
                .usingJobData("name", "quartz")
                .storeDurably(true)
                .build();
        scheduler.addJob(jobDetail, true);
        JobDetail jobDetail1 = scheduler.getJobDetail(jobKey);
        System.out.println(jobDetail1);
    }
}
