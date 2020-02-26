package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.github.quartz.impl.redisjobstore.jackson.IntegerSetDeserializer;
import com.github.quartz.impl.redisjobstore.jackson.IntegerSetSerializer;

import java.util.Set;

public abstract class DailyTimeIntervalTriggerMixIn extends TriggerMixIn {

    @JsonDeserialize(using = IntegerSetDeserializer.class)
    public abstract Set<Integer> getDaysOfWeek();

    @JsonSerialize(using = IntegerSetSerializer.class)
    public abstract void setDaysOfWeek(Set<Integer> daysOfWeek);
}
