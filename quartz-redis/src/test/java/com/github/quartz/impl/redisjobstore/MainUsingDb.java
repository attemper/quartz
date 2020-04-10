package com.github.quartz.impl.redisjobstore;

import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Properties;

public class MainUsingDb {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("org.quartz.scheduler.instanceName", "MyDbScheduler");
        properties.put("org.quartz.jobStore.class", "org.quartz.impl.jdbcjobstore.JobStoreTX");
        properties.put("org.quartz.jobStore.dataSource", "myDS");
        properties.put("org.quartz.dataSource.myDS.driver", "com.mysql.jdbc.Driver");
        properties.put("org.quartz.dataSource.myDS.URL", "jdbc:mysql://localhost:3306/quartz?characterEncoding=utf-8");
        properties.put("org.quartz.dataSource.myDS.user", "root");
        properties.put("org.quartz.dataSource.myDS.password", "d04l12l12x24DLLX");

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
