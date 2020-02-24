package com.github.quartz.impl.redisjobstore;

import org.quartz.impl.jdbcjobstore.Constants;

public interface RedisConstants extends Constants {

    /**
     * job detail
     * 0 schedName
     * 1 jobName
     * 2 groupName
     */
    String KEY_JOB = "job@{0}#{1}${2}";

    /**
     * jobs set
     * 0 schedName
     */
    String KEY_JOBS = "jobs@{0}";

    /**
     * trigger info
     * 0 schedName
     * 1 triggerName
     * 2 groupName
     */
    String KEY_TRIGGER = "trigger@{0}#{1}${2}";

    /**
     * triggers set
     * 0 schedName
     */
    String KEY_TRIGGERS = "triggers@{0}";

    /**
     * waiting triggers set
     * 0 schedName
     */
    String KEY_WAIT_TRIGGERS = "wait_triggers@{0}";

    /**
     * job triggers set
     * 0 schedName
     * 1 jobName
     * 2 groupName
     * value triggerName groupName
     */
    String KEY_JOB_TRIGGERS = "job_triggers@{0}#{1}${2}";

    /**
     * fired jobs set
     * 0 schedName
     * 1 instanceId
     * value entryId
     */
    String KEY_FIRED_JOBS = "fired_jobs@{0}#{1}";

    /**
     * hash
     * 0 schedName
     * 1 entryId
     * value fired job info
     */
    String KEY_FIRED_JOB = "fired_job@{0}#{1}";

    /**
     * set
     * 0 schedName
     * value groupName
     */
    String KEY_PAUSED_TRIGGER_GROUPS = "paused_trigger_groups@{0}";

    /**
     * hash
     * 0 schedName
     * 1 calendarName
     * value calendar info
     */
    String KEY_CALENDAR = "calendar@{0}#{1}";

    /**
     * set
     * 0 schedName
     * value calendarName
     */
    String KEY_CALENDARS = "calendars@{0}";

    /**
     * set
     * 0 schedName
     * 1 calendarName
     * value triggerName groupName
     */
    String KEY_CALENDAR_TRIGGERS = "calendar_triggers@{0}#{1}";

    String VALUE_DELIMITER = "@@@";
}
