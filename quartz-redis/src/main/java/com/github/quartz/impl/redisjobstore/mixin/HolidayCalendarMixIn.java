package com.github.quartz.impl.redisjobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

public class HolidayCalendarMixIn {
    @JsonProperty
    private TreeSet<Date> dates;


    @JsonIgnore
    public SortedSet<Date> getExcludedDates() {
        return null;
    }
}
