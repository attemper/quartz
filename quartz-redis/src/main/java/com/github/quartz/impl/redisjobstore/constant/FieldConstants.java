package com.github.quartz.impl.redisjobstore.constant;

public interface FieldConstants {

    // job
    String FIELD_JOB_NAME = "jobName";

    String FIELD_JOB_GROUP = "jobGroup";

    String FIELD_DURABLE = "durable";

    String FIELD_REQUESTS_RECOVERY = "requestsRecovery";

    // trigger
    String FIELD_STATE = "state";

    String FIELD_TYPE = "type";

    String FIELD_START_TIME = "startTime";

    String FIELD_END_TIME = "endTime";

    String FIELD_PREV_FIRE_TIME  = "prevFireTime";

    String FIELD_NEXT_FIRE_TIME = "nextFireTime";

    String FIELD_MISFIRE_INSTRUCTION = "misfireInstruction";

    // cron trigger
    String FIELD_CRON_EXPRESSION = "cronExpression";

}
