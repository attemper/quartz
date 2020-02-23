package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.quartz.impl.redisjobstore.jackson.ObjectDeserializer;
import com.github.quartz.impl.redisjobstore.jackson.ObjectSerializer;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobKey;

public abstract class JobDetailMixIn {

    @JsonIgnore
    public abstract JobKey getKey();

    @JsonSerialize(using = ObjectSerializer.class)
    public abstract void setJobDataMap(JobDataMap jobDataMap);

    @JsonDeserialize(using = ObjectDeserializer.class)
    public abstract JobDataMap getJobDataMap();

    @JsonIgnore
    public abstract JobBuilder getJobBuilder();

    @JsonIgnore
    public abstract String getFullName();

    @JsonIgnore
    public abstract boolean isPersistJobDataAfterExecution();

    @JsonIgnore
    public abstract boolean isConcurrentExectionDisallowed();

    @JsonProperty("durable")
    public abstract void setDurability(boolean d);

    @JsonProperty("requestsRecovery")
    public abstract void setRequestsRecovery(boolean shouldRecover);

}
