package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.quartz.impl.redisjobstore.constant.FieldConstants;
import org.quartz.JobKey;
import org.quartz.TriggerKey;

public abstract class FiredTriggerRecordMixIn implements FieldConstants {

    @JsonProperty(FIELD_ENTRY_ID)
    public abstract String getFireInstanceId();

    @JsonProperty(FIELD_FIRED_TIME)
    public abstract long getFireTimestamp();

    @JsonProperty(FIELD_SCHED_TIME)
    public abstract long getScheduleTimestamp();

    @JsonIgnore
    public abstract boolean isJobDisallowsConcurrentExecution();

    @JsonIgnore
    public abstract JobKey getJobKey();

    @JsonProperty(FIELD_INSTANCE_ID)
    public abstract String getSchedulerInstanceId();

    @JsonIgnore
    public abstract TriggerKey getTriggerKey();

    @JsonProperty(FIELD_STATE)
    public abstract String getFireInstanceState();

    @JsonProperty(FIELD_ENTRY_ID)
    public abstract void setFireInstanceId(String string);

    @JsonProperty(FIELD_FIRED_TIME)
    public abstract void setFireTimestamp(long l);

    @JsonProperty(FIELD_SCHED_TIME)
    public abstract void setScheduleTimestamp(long l);

    @JsonIgnore
    public abstract void setJobDisallowsConcurrentExecution(boolean b);

    @JsonIgnore
    public abstract void setJobKey(JobKey key);

    @JsonProperty(FIELD_INSTANCE_ID)
    public abstract void setSchedulerInstanceId(String string);

    @JsonIgnore
    public abstract void setTriggerKey(TriggerKey key);

    @JsonProperty(FIELD_STATE)
    public abstract void setFireInstanceState(String string);

    @JsonIgnore
    public abstract boolean isJobRequestsRecovery();

    @JsonIgnore
    public abstract void setJobRequestsRecovery(boolean b);

}
