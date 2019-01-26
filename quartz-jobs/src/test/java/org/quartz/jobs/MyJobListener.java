package org.quartz.jobs;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;

public class MyJobListener implements JobListener {
    public volatile JobExecutionException jobException;
    public CyclicBarrier barrier = new CyclicBarrier(2);

    public String getName() {
        return "MyJobListener";
    }

    public void jobToBeExecuted(JobExecutionContext context) {
        //
    }

    public void jobExecutionVetoed(JobExecutionContext context) {
        //
    }

    public void jobWasExecuted(JobExecutionContext context,
            JobExecutionException jobException) {
        this.jobException = jobException;
        try {
            barrier.await(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
