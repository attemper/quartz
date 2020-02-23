package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.impl.BeanAsArrayDeserializer;
import com.fasterxml.jackson.databind.ser.impl.BeanAsArraySerializer;
import org.quartz.*;

import java.util.Date;

public abstract class TriggerMixIn {

    @JsonIgnore
    public abstract TriggerKey getKey();

    @JsonIgnore
    public abstract JobKey getJobKey();

    @JsonSerialize(using = BeanAsArraySerializer.class)
    public abstract void setJobDataMap(JobDataMap jobDataMap);

    @JsonDeserialize(using = BeanAsArrayDeserializer.class)
    public abstract JobDataMap getJobDataMap();

    @JsonIgnore
    public abstract boolean mayFireAgain();

    @JsonIgnore
    public abstract Date getStartTime();

    @JsonIgnore
    public abstract Date getEndTime();

    @JsonIgnore
    public abstract Date getFinalFireTime();

    @JsonIgnore
    public abstract String getFullName();

    @JsonIgnore
    public abstract String getFullJobName();

    @JsonIgnore
    public abstract TriggerBuilder getTriggerBuilder();

    @JsonIgnore
    public abstract ScheduleBuilder getScheduleBuilder();

}
