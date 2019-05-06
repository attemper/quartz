/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at 
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0 
 *   
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations 
 * under the License.
 * 
 */
package org.quartz.impl.jdbcjobstore;

import org.quartz.CalendarOffsetScheduleBuilder;
import org.quartz.CalendarOffsetTrigger;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.TimeOfDay;
import org.quartz.impl.triggers.CalendarOffsetTriggerImpl;
import org.quartz.spi.OperableTrigger;

/**
 * Persist a CalendarOffsetTrigger by converting internal fields to and from
 * SimplePropertiesTriggerProperties.
 * 
 * @see CalendarOffsetScheduleBuilder
 * @see CalendarOffsetTrigger
 * 
 * @since 3.0.0-SNAPSHOT
 * 
 * @author ldang <ldang@sse.com.cn>
 */
public class CalendarOffsetTriggerPersistenceDelegate extends SimplePropertiesTriggerPersistenceDelegateSupport {

    public boolean canHandleTriggerType(OperableTrigger trigger) {
        return ((trigger instanceof CalendarOffsetTrigger) && !((CalendarOffsetTriggerImpl)trigger).hasAdditionalProperties());
    }

    public String getHandledTriggerTypeDiscriminator() {
        return TTYPE_CAL_OFFSET;
    }

    @Override
    protected SimplePropertiesTriggerProperties getTriggerProperties(OperableTrigger trigger) {
        CalendarOffsetTriggerImpl triggerImpl = (CalendarOffsetTriggerImpl)trigger;
        SimplePropertiesTriggerProperties props = new SimplePropertiesTriggerProperties();
        
        props.setString1(triggerImpl.getIntervalUnit().name());
        props.setInt1(triggerImpl.getInnerOffset());
        props.setInt2(triggerImpl.getTimesTriggered());

        StringBuilder timeOfDayBuffer = new StringBuilder();
        TimeOfDay startTimeOfDay = triggerImpl.getStartTimeOfDay();
        if (startTimeOfDay != null) {
            timeOfDayBuffer.append(startTimeOfDay.getHour()).append(",");
            timeOfDayBuffer.append(startTimeOfDay.getMinute()).append(",");
            timeOfDayBuffer.append(startTimeOfDay.getSecond()).append(",");
        } else {
            timeOfDayBuffer.append(",,,");
        }
        props.setString3(timeOfDayBuffer.toString());
        
        props.setLong1(triggerImpl.getRepeatCount());
        props.setLong2(triggerImpl.getOuterOffset());

        props.setBoolean1(triggerImpl.isReversed());

        return props;
    }

    @Override
    protected TriggerPropertyBundle getTriggerPropertyBundle(SimplePropertiesTriggerProperties props) {
        int repeatCount = (int)props.getLong1();
        int innerOffset = props.getInt1();
        long outerOffset = props.getLong2();
        boolean reversed = props.isBoolean1();
        String intervalUnitStr = props.getString1();
        String timeOfDayStr = props.getString3();

        IntervalUnit intervalUnit = IntervalUnit.valueOf(intervalUnitStr);
        CalendarOffsetScheduleBuilder scheduleBuilder = CalendarOffsetScheduleBuilder
                .calendarOffsetSchedule()
                .withIntervalUnit(intervalUnit)
                .withRepeatCount(repeatCount)
                .withInnerOffset(innerOffset)
                .withOuterOffset(Integer.valueOf(String.valueOf(outerOffset)))
                .reversed(reversed);
        
        if (timeOfDayStr != null) {
            String[] nums = timeOfDayStr.split(",");
            TimeOfDay startTimeOfDay;
            if (nums.length >= 3) {
                int hour = Integer.parseInt(nums[0]);
                int min = Integer.parseInt(nums[1]);
                int sec = Integer.parseInt(nums[2]);
                startTimeOfDay = new TimeOfDay(hour, min, sec);
            } else {
                startTimeOfDay = TimeOfDay.hourMinuteAndSecondOfDay(0, 0, 0);
            }
            scheduleBuilder.startingDailyAt(startTimeOfDay);
        } else {
            scheduleBuilder.startingDailyAt(TimeOfDay.hourMinuteAndSecondOfDay(0, 0, 0));
        }
        
        int timesTriggered = props.getInt2();
        String[] statePropertyNames = { "timesTriggered" };
        Object[] statePropertyValues = { timesTriggered };

        return new TriggerPropertyBundle(scheduleBuilder, statePropertyNames, statePropertyValues);
    }
}
