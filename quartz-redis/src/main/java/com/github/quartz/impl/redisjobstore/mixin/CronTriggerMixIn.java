package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.quartz.CronExpression;

public abstract class CronTriggerMixIn extends TriggerMixIn {
    @JsonIgnore
    public abstract String getExpressionSummary();

    @JsonIgnore
    public abstract void setCronExpression(CronExpression cron);

    @JsonProperty("cronExpression")
    public abstract void setCronExpression(String cronExpression);

    @JsonProperty("cronExpression")
    public abstract String getCronExpression();

}
