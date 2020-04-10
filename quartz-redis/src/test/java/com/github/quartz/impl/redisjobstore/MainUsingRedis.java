package com.github.quartz.impl.redisjobstore;

import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

public class MainUsingRedis {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("org.quartz.scheduler.instanceName", "MyRedisScheduler");
        properties.put("org.quartz.jobStore.class", "com.github.quartz.impl.redisjobstore.RedisJobStore");

        properties.put("org.quartz.scheduler.instanceId", "AUTO");
        properties.put("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
        properties.put("org.quartz.threadPool.threadCount", "25");
        properties.put("org.quartz.threadPool.threadPriority", "5");
        properties.put("org.quartz.jobStore.misfireThreshold", "60000");
        properties.put("org.quartz.jobStore.isClustered", "true");
        //properties.put("org.quartz.jobStore.clusterCheckinInterval", "20000");

        SchedulerFactory sf = new StdSchedulerFactory(properties);
        Scheduler scheduler = sf.getScheduler();
        //scheduler.clear();
        scheduler.start();
    }

}
