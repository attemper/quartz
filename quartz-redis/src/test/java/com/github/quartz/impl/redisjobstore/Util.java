package com.github.quartz.impl.redisjobstore;

import com.zaxxer.hikari.HikariDataSource;

import java.util.Properties;

public class Util {

    public static Properties getRedisProperties() {
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
        return properties;
    }

    public static HikariDataSource getDataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl("jdbc:mysql://localhost:3306/ext?characterEncoding=utf-8");
        dataSource.setUsername("root");
        dataSource.setPassword("Hn180301");
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setMinimumIdle(10);
        dataSource.setMaximumPoolSize(10);
        return dataSource;
    }
}
