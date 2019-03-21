package com.sse.quartz;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * @author ldang
 */
public class QuartzUsingRedis {

    public static void main(String[] args) throws SchedulerException {
        SchedulerFactory sf = new StdSchedulerFactory();
        Scheduler scheduler = sf.getScheduler();
        //定义一个Trigger
        /*Trigger trigger = TriggerBuilder.newTrigger().withIdentity("trigger4", "group1") //定义name/group
                .startNow()//一旦加入scheduler，立即生效
                .withSchedule(SimpleScheduleBuilder.simpleSchedule() //使用SimpleTrigger
                        .withIntervalInSeconds(17) //每隔30秒执行一次
                        .repeatForever()) //一直执行，奔腾到老不停歇
                .build();

        //定义一个JobDetail
        JobDetail job = JobBuilder.newJob(HelloJob.class) //定义Job类为HelloQuartz类，这是真正的执行逻辑所在
                .withIdentity("job4", "group1") //定义name/group
                .usingJobData("name", "quartz") //定义属性
                .build();

        //加入这个调度
        scheduler.scheduleJob(job, trigger);*/
        scheduler.start();
    }

}
