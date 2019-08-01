package com.github.quartz.nosqljobstore;

import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

/**
 * @author ldang
 */
public class QuartzUsingRedis {

    public static void main(String[] args) throws SchedulerException {
        SchedulerFactory sf = new StdSchedulerFactory();
        Scheduler scheduler = sf.getScheduler();
/*        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger1", "group1") //定义name/group
                .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?"))
                .build();

        JobDetail job = JobBuilder.newJob(HelloJob.class)
                .withIdentity("job1", "group1")
                .usingJobData("name", "quartz")
                .build();

        scheduler.scheduleJob(job, trigger);*/
        scheduler.start();
    }

}
