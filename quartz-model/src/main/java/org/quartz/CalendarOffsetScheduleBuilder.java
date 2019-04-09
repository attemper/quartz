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

package org.quartz;

import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.impl.triggers.CalendarOffsetTriggerImpl;
import org.quartz.spi.MutableTrigger;

/**
 * <code>CalendarOffsetScheduleBuilder</code> is a {@link ScheduleBuilder}
 * that defines calendar time (day, week, month, year) interval-based
 * schedules for <code>Trigger</code>s.
 *
 * <p>Quartz provides a builder-style API for constructing scheduling-related
 * entities via a Domain-Specific Language (DSL).  The DSL can best be
 * utilized through the usage of static imports of the methods on the classes
 * <code>TriggerBuilder</code>, <code>JobBuilder</code>,
 * <code>DateBuilder</code>, <code>JobKey</code>, <code>TriggerKey</code>
 * and the various <code>ScheduleBuilder</code> implementations.</p>
 *
 * <p>Client code can then use the DSL to write code such as this:</p>
 * <pre>
 *         JobDetail job = newJob(MyJob.class)
 *             .withIdentity("myJob")
 *             .build();
 *
 *         Trigger trigger = newTrigger()
 *             .withIdentity(triggerKey("myTrigger", "myTriggerGroup"))
 *             .withSchedule(withIntervalInDays(3))
 *             .startAt(futureDate(10, MINUTES))
 *             .build();
 *
 *         scheduler.scheduleJob(job, trigger);
 * <pre>
 *
 * @see DailyTimeIntervalScheduleBuilder
 * @see CronScheduleBuilder
 * @see ScheduleBuilder
 * @see SimpleScheduleBuilder
 * @see TriggerBuilder
 */
public class CalendarOffsetScheduleBuilder extends ScheduleBuilder<CalendarOffsetTrigger> {

    private IntervalUnit intervalUnit = IntervalUnit.WEEK;

    private int misfireInstruction = CalendarOffsetTrigger.MISFIRE_INSTRUCTION_SMART_POLICY;
    private TimeOfDay startTimeOfDay;
    private int innerOffset;
    private int outerOffset;
    private boolean reversed;
    private int repeatCount = CalendarOffsetTrigger.REPEAT_INDEFINITELY;

    protected CalendarOffsetScheduleBuilder() {
    }

    /**
     * Create a CalendarOffsetScheduleBuilder.
     *
     * @return the new CalendarOffsetScheduleBuilder
     */
    public static CalendarOffsetScheduleBuilder calendarOffsetSchedule() {
        return new CalendarOffsetScheduleBuilder();
    }

    /**
     * Build the actual Trigger -- NOT intended to be invoked by end users,
     * but will rather be invoked by a TriggerBuilder which this
     * ScheduleBuilder is given to.
     *
     * @see TriggerBuilder#withSchedule(ScheduleBuilder)
     */
    @Override
    public MutableTrigger build() {

        CalendarOffsetTriggerImpl st = new CalendarOffsetTriggerImpl();
        st.setIntervalUnit(intervalUnit);
        st.setMisfireInstruction(misfireInstruction);
        st.setRepeatCount(repeatCount);
        st.setStartTimeOfDay(startTimeOfDay);
        st.setInnerOffset(innerOffset);
        st.setOuterOffset(outerOffset);
        st.setReversed(reversed);

        return st;
    }

    /**
     * Specify the time unit and interval for the Trigger to be produced.
     *
     * @param unit  the time unit (IntervalUnit) of the interval.
     * @return the updated CalendarOffsetScheduleBuilder
     * @see CalendarOffsetTrigger#getIntervalUnit()
     */
    public CalendarOffsetScheduleBuilder withIntervalUnit(IntervalUnit unit) {
        if(unit == null)
            throw new IllegalArgumentException("TimeUnit must be specified.");
        this.intervalUnit = unit;
        return this;
    }

    /**
     * Specify an interval in the IntervalUnit.WEEK that the produced
     * Trigger will repeat at.
     *
     * @return the updated CalendarOffsetScheduleBuilder
     * @see CalendarOffsetTrigger#getIntervalUnit()
     */
    public CalendarOffsetScheduleBuilder withIntervalUnitOfWeek() {
        this.intervalUnit = IntervalUnit.WEEK;
        return this;
    }

    /**
     * Specify an interval in the IntervalUnit.MONTH that the produced
     * Trigger will repeat at.
     *
     * @return the updated CalendarOffsetScheduleBuilder
     * @see CalendarOffsetTrigger#getIntervalUnit()
     */
    public CalendarOffsetScheduleBuilder withIntervalUnitOfMonth() {
        this.intervalUnit = IntervalUnit.MONTH;
        return this;
    }

    /**
     * Specify an interval in the IntervalUnit.SEASON that the produced
     * Trigger will repeat at.
     *
     * @return the updated CalendarOffsetScheduleBuilder
     * @see CalendarOffsetTrigger#getIntervalUnit()
     */
    public CalendarOffsetScheduleBuilder withIntervalUnitOfSeason() {
        this.intervalUnit = IntervalUnit.SEASON;
        return this;
    }

    /**
     * Specify an interval in the IntervalUnit.HALF_YEAR that the produced
     * Trigger will repeat at.
     *
     * @return the updated CalendarOffsetScheduleBuilder
     * @see CalendarOffsetTrigger#getIntervalUnit()
     */
    public CalendarOffsetScheduleBuilder withIntervalUnitOfHalfYear() {
        this.intervalUnit = IntervalUnit.HALF_YEAR;
        return this;
    }

    /**
     * Specify an interval in the IntervalUnit.YEAR that the produced
     * Trigger will repeat at.
     *
     * @return the updated CalendarOffsetScheduleBuilder
     * @see CalendarOffsetTrigger#getIntervalUnit()
     */
    public CalendarOffsetScheduleBuilder withIntervalUnitOfYear() {
        this.intervalUnit = IntervalUnit.YEAR;
        return this;
    }

    /**
     * If the Trigger misfires, use the
     * {@link Trigger#MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY} instruction.
     *
     * @return the updated CronScheduleBuilder
     * @see Trigger#MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY
     */
    public CalendarOffsetScheduleBuilder withMisfireHandlingInstructionIgnoreMisfires() {
        misfireInstruction = Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY;
        return this;
    }

    /**
     * If the Trigger misfires, use the
     * {@link CalendarOffsetTrigger#MISFIRE_INSTRUCTION_DO_NOTHING} instruction.
     *
     * @return the updated CalendarOffsetScheduleBuilder
     * @see CalendarOffsetTrigger#MISFIRE_INSTRUCTION_DO_NOTHING
     */
    public CalendarOffsetScheduleBuilder withMisfireHandlingInstructionDoNothing() {
        misfireInstruction = CalendarOffsetTrigger.MISFIRE_INSTRUCTION_DO_NOTHING;
        return this;
    }

    /**
     * If the Trigger misfires, use the
     * {@link CalendarOffsetTrigger#MISFIRE_INSTRUCTION_FIRE_ONCE_NOW} instruction.
     *
     * @return the updated CalendarOffsetScheduleBuilder
     * @see CalendarOffsetTrigger#MISFIRE_INSTRUCTION_FIRE_ONCE_NOW
     */
    public CalendarOffsetScheduleBuilder withMisfireHandlingInstructionFireAndProceed() {
        misfireInstruction = CalendarOffsetTrigger.MISFIRE_INSTRUCTION_FIRE_ONCE_NOW;
        return this;
    }

    /**
     * Set number of times for interval to repeat.
     *
     * <p>Note: if you want total count = 1 (at start time) + repeatCount</p>
     *
     * @return the new CalendarOffsetScheduleBuilder
     */
    public CalendarOffsetScheduleBuilder withRepeatCount(int repeatCount) {
        this.repeatCount = repeatCount;
        return this;
    }

    /**
     * Set the trigger to begin firing at the given time.
     *
     * @return the updated CalendarOffsetScheduleBuilder
     */
    public CalendarOffsetScheduleBuilder startingDailyAt(TimeOfDay timeOfDay) {
        if (timeOfDay == null) {
            throw new IllegalArgumentException("Start time of day cannot be null!");
        }
        this.startTimeOfDay = timeOfDay;
        return this;
    }

    public CalendarOffsetScheduleBuilder withInnerOffset(int innerOffset) {
        this.innerOffset = innerOffset;
        return this;
    }

    public CalendarOffsetScheduleBuilder withOuterOffset(int outerOffset) {
        this.outerOffset = outerOffset;
        return this;
    }

    public CalendarOffsetScheduleBuilder reversed(boolean reversed) {
        this.reversed = reversed;
        return this;
    }

}
