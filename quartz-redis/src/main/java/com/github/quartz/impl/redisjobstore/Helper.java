package com.github.quartz.impl.redisjobstore;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.quartz.impl.redisjobstore.mixin.CronTriggerMixIn;
import com.github.quartz.impl.redisjobstore.mixin.HolidayCalendarMixIn;
import com.github.quartz.impl.redisjobstore.mixin.JobDetailMixIn;
import com.github.quartz.impl.redisjobstore.mixin.TriggerMixIn;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.SimpleTrigger;
import org.quartz.impl.calendar.HolidayCalendar;

public class Helper {

    private static ObjectMapper objectMapper;

    static {
        objectMapper = new ObjectMapper()
                .addMixIn(CronTrigger.class, CronTriggerMixIn.class)
                .addMixIn(SimpleTrigger.class, TriggerMixIn.class)
                .addMixIn(JobDetail.class, JobDetailMixIn.class)
                .addMixIn(HolidayCalendar.class, HolidayCalendarMixIn.class)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
