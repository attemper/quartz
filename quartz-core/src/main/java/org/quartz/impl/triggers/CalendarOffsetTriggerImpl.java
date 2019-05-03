package org.quartz.impl.triggers;

import org.quartz.*;
import org.quartz.DateBuilder.IntervalUnit;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CalendarOffsetTriggerImpl extends AbstractTrigger<CalendarOffsetTrigger> implements CalendarOffsetTrigger, CoreTrigger {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constants.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    private static final long serialVersionUID = -2635982274232850343L;


    private static final int YEAR_TO_GIVEUP_SCHEDULING_AT = java.util.Calendar.getInstance().get(java.util.Calendar.YEAR) + 100;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Data members.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    private Date startTime = null;

    private Date endTime = null;

    private Date nextFireTime = null;

    private Date previousFireTime = null;

    private int repeatCount = REPEAT_INDEFINITELY;

    //private  int repeatInterval = 0;

    // WEEK/MONTH/SEAZON/HALF_YEAR/YEAR
    private IntervalUnit intervalUnit = IntervalUnit.WEEK;

    private int timesTriggered = 0;

    private boolean complete = false;

    private TimeOfDay startTimeOfDay;

    private int innerOffset;

    private int outerOffset;

    private boolean reversed;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constructors.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * <p>
     * Create a <code>DateIntervalTrigger</code> with no settings.
     * </p>
     */
    public CalendarOffsetTriggerImpl() {
        super();
    }

    /**
     * <p>
     * Create a <code>DateIntervalTrigger</code> that will occur immediately, and
     * repeat at the the given interval.
     * </p>
     */
    public CalendarOffsetTriggerImpl(String name, TimeOfDay startTimeOfDay,
                                     IntervalUnit intervalUnit) {
        this(name, null, startTimeOfDay, intervalUnit);
    }

    /**
     * <p>
     * Create a <code>DateIntervalTrigger</code> that will occur immediately, and
     * repeat at the the given interval.
     * </p>
     */
    public CalendarOffsetTriggerImpl(String name, String group, TimeOfDay startTimeOfDay,
                                     IntervalUnit intervalUnit) {
        this(name, group, new Date(), null, startTimeOfDay, intervalUnit);
    }

    /**
     * <p>
     * Create a <code>DateIntervalTrigger</code> that will occur at the given time,
     * and repeat at the the given interval until the given end time.
     * </p>
     *
     * @param startTime
     *          A <code>Date</code> set to the time for the <code>Trigger</code>
     *          to fire.
     * @param endTime
     *          A <code>Date</code> set to the time for the <code>Trigger</code>
     *          to quit repeat firing.
     * @param intervalUnit
     *          The repeat interval unit (minutes, days, months, etc).
     */
    public CalendarOffsetTriggerImpl(String name, Date startTime, Date endTime, TimeOfDay startTimeOfDay,
                                     IntervalUnit intervalUnit) {
        this(name, null, startTime, endTime, startTimeOfDay, intervalUnit);
    }

    /**
     * <p>
     * Create a <code>DateIntervalTrigger</code> that will occur at the given time,
     * and repeat at the the given interval until the given end time.
     * </p>
     *
     * @param startTime
     *          A <code>Date</code> set to the time for the <code>Trigger</code>
     *          to fire.
     * @param endTime
     *          A <code>Date</code> set to the time for the <code>Trigger</code>
     *          to quit repeat firing.
     * @param intervalUnit
     *          The repeat interval unit (minutes, days, months, etc).
     */
    public CalendarOffsetTriggerImpl(String name, String group, Date startTime, Date endTime,
                                     TimeOfDay startTimeOfDay, IntervalUnit intervalUnit) {
        super(name, group);

        setStartTime(startTime);
        setEndTime(endTime);
        setStartTimeOfDay(startTimeOfDay);
        setIntervalUnit(intervalUnit);
    }

    /**
     * <p>
     * Create a <code>DateIntervalTrigger</code> that will occur at the given time,
     * fire the identified <code>Job</code> and repeat at the the given
     * interval until the given end time.
     * </p>
     *
     * @param startTime
     *          A <code>Date</code> set to the time for the <code>Trigger</code>
     *          to fire.
     * @param endTime
     *          A <code>Date</code> set to the time for the <code>Trigger</code>
     *          to quit repeat firing.
     * @param intervalUnit
     *          The repeat interval unit (minutes, days, months, etc).
     */
    public CalendarOffsetTriggerImpl(String name, String group, String jobName, String jobGroup, Date startTime,
                                     Date endTime, TimeOfDay startTimeOfDay, IntervalUnit intervalUnit) {
        super(name, group, jobName, jobGroup);

        setStartTime(startTime);
        setEndTime(endTime);
        setStartTimeOfDay(startTimeOfDay);
        setIntervalUnit(intervalUnit);
    }

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Interface.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * <p>
     * Get the time at which the <code>DateIntervalTrigger</code> should occur.
     * </p>
     */
    @Override
    public Date getStartTime() {
        if(startTime == null)
            startTime = new Date();
        return startTime;
    }

    /**
     * <p>
     * Set the time at which the <code>DateIntervalTrigger</code> should occur.
     * </p>
     *
     * @exception IllegalArgumentException
     *              if startTime is <code>null</code>.
     */
    @Override
    public void setStartTime(Date startTime) {
        if (startTime == null) {
            throw new IllegalArgumentException("Start time cannot be null");
        }

        Date eTime = getEndTime();
        if (eTime != null && eTime.before(startTime)) {
            throw new IllegalArgumentException(
                    "End time cannot be before start time");
        }

        this.startTime = startTime;
    }

    /**
     * <p>
     * Get the time at which the <code>DateIntervalTrigger</code> should quit
     * repeating.
     * </p>
     *
     * @see #getFinalFireTime()
     */
    @Override
    public Date getEndTime() {
        return endTime;
    }

    /**
     * <p>
     * Set the time at which the <code>DateIntervalTrigger</code> should quit
     * repeating (and be automatically deleted).
     * </p>
     *
     * @exception IllegalArgumentException
     *              if endTime is before start time.
     */
    @Override
    public void setEndTime(Date endTime) {
        Date sTime = getStartTime();
        if (sTime != null && endTime != null && sTime.after(endTime)) {
            throw new IllegalArgumentException(
                    "End time cannot be before start time");
        }

        this.endTime = endTime;
    }

    /* (non-Javadoc)
     * @see org.quartz.DateIntervalTriggerI#getIntervalUnit()
     */
    public IntervalUnit getIntervalUnit() {
        return intervalUnit;
    }

    /**
     * <p>Set the interval unit - the time unit on with the interval applies.</p>
     */
    public void setIntervalUnit(IntervalUnit intervalUnit) {
        if (intervalUnit == null || !(intervalUnit.equals(IntervalUnit.WEEK)
                || intervalUnit.equals(IntervalUnit.MONTH) || intervalUnit.equals(IntervalUnit.SEASON)
                || intervalUnit.equals(IntervalUnit.HALF_YEAR) || intervalUnit.equals(IntervalUnit.YEAR))) {
            throw new IllegalArgumentException("TimeUnit is incorrect," +
                    "it must be week,month,season,half year or year,but you set " + intervalUnit);
        }
        this.intervalUnit = intervalUnit;
    }

    /* (non-Javadoc)
     * @see org.quartz.DateIntervalTriggerI#getTimesTriggered()
     */
    public int getTimesTriggered() {
        return timesTriggered;
    }

    /**
     * <p>
     * Set the number of times the <code>DateIntervalTrigger</code> has already
     * fired.
     * </p>
     */
    public void setTimesTriggered(int timesTriggered) {
        this.timesTriggered = timesTriggered;
    }

    @Override
    protected boolean validateMisfireInstruction(int misfireInstruction) {
        if (misfireInstruction < MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) {
            return false;
        }

        return misfireInstruction <= MISFIRE_INSTRUCTION_DO_NOTHING;
    }


    /**
     * <p>
     * Updates the <code>DateIntervalTrigger</code>'s state based on the
     * MISFIRE_INSTRUCTION_XXX that was selected when the <code>DateIntervalTrigger</code>
     * was created.
     * </p>
     *
     * <p>
     * If the misfire instruction is set to MISFIRE_INSTRUCTION_SMART_POLICY,
     * then the following scheme will be used: <br>
     * <ul>
     * <li>The instruction will be interpreted as <code>MISFIRE_INSTRUCTION_FIRE_ONCE_NOW</code>
     * </ul>
     * </p>
     */
    @Override
    public void updateAfterMisfire(Calendar cal) {
        int instr = getMisfireInstruction();

        if(instr == MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY)
            return;

        if (instr == MISFIRE_INSTRUCTION_SMART_POLICY) {
            instr = MISFIRE_INSTRUCTION_FIRE_ONCE_NOW;
        }

        if (instr == MISFIRE_INSTRUCTION_DO_NOTHING) {
            Date newFixedFireTime = getFireTimeAfter(new Date(), cal);
            setNextFireTime(new Date(newFixedFireTime.getTime() + outerOffset * 86400000));
        } else if (instr == MISFIRE_INSTRUCTION_FIRE_ONCE_NOW) {
            // fire once now...
            setNextFireTime(new Date());
            // the new fire time afterward will magically preserve the original
            // time of day for firing for day/week/month interval triggers,
            // because of the way getFireTimeAfter() works - in its always restarting
            // computation from the start time.
        }
    }

    /**
     *
     * @see org.quartz.spi.OperableTrigger#updateWithNewCalendar(Calendar, long)
     */
    @Override
    public void updateWithNewCalendar(Calendar calendar, long misfireThreshold)
    {
        if (calendar == null) {
            return;
        }
        Date fixedNextFireTime = getFireTimeAfter(
                new Date(previousFireTime.getTime() - outerOffset * 86400000), calendar);

        if (fixedNextFireTime == null) {
            return;
        }
        nextFireTime = new Date(fixedNextFireTime.getTime() + outerOffset * 86400000);
        Date now = new Date();
        if(nextFireTime.before(now) && now.getTime() - nextFireTime.getTime() >= misfireThreshold) {
            fixedNextFireTime = computeNextPeriod(fixedNextFireTime, calendar);
            nextFireTime = fixedNextFireTime == null ? null :
                    new Date(fixedNextFireTime.getTime() + outerOffset * 86400000);
        }
    }

    /**
     * <p>
     * Called by the scheduler at the time a <code>Trigger</code> is first
     * added to the scheduler, in order to have the <code>Trigger</code>
     * compute its first fire time, based on any associated calendar.
     * </p>
     *
     * <p>
     * After this method has been called, <code>getNextFireTime()</code>
     * should return a valid answer.
     * </p>
     *
     * @return the first time at which the <code>Trigger</code> will be fired
     *         by the scheduler, which is also the same value <code>getNextFireTime()</code>
     *         will return (until after the first firing of the <code>Trigger</code>).
     *         </p>
     */
    @Override
    public Date computeFirstFireTime(Calendar calendar) {
        Date fixedStartTime = new Date(getStartTime().getTime() - outerOffset * 86400000);
        Date fixedNextFireTime = getFireTimeAfter(fixedStartTime, calendar);
        if(fixedNextFireTime.before(fixedStartTime)) {
            fixedNextFireTime = computeNextPeriod(fixedNextFireTime, calendar);
            nextFireTime = fixedNextFireTime == null ? null :
                    new Date(fixedNextFireTime.getTime() + outerOffset * 86400000);
        } else {
            nextFireTime = fixedNextFireTime;
        }
        return nextFireTime;
    }

    private Date computeNextPeriod(Date fixedNextFireTime, Calendar calendar) {
        java.util.Calendar sTime = java.util.Calendar.getInstance();
        sTime.setTime(fixedNextFireTime);
        if(getIntervalUnit().equals(IntervalUnit.WEEK)) {
            sTime.set(java.util.Calendar.DAY_OF_WEEK, java.util.Calendar.SUNDAY);
            sTime.add(java.util.Calendar.WEEK_OF_YEAR, 1);
        } else if(getIntervalUnit().equals(IntervalUnit.MONTH)) {
            sTime.set(java.util.Calendar.DAY_OF_MONTH, 1);
            sTime.add(java.util.Calendar.MONTH, 1);
        } else if(getIntervalUnit().equals(IntervalUnit.SEASON)) {
            sTime.set(java.util.Calendar.DAY_OF_MONTH, 1);
            sTime.add(java.util.Calendar.MONTH, 3);
        } else if(getIntervalUnit().equals(IntervalUnit.HALF_YEAR)) {
            sTime.set(java.util.Calendar.DAY_OF_MONTH, 1);
            sTime.add(java.util.Calendar.MONTH, 6);
        } else if(getIntervalUnit().equals(IntervalUnit.YEAR)) {
            sTime.set(java.util.Calendar.DAY_OF_MONTH, 1);
            sTime.add(java.util.Calendar.YEAR, 1);
        }
        fixedNextFireTime = getFireTimeAfter(sTime.getTime(), calendar);
        return fixedNextFireTime;
    }

    private Date getFireTimeAfter(Date afterTime, Calendar calendar) {
        if (complete) {
            return null;
        }

        // Check repeatCount limit
        if (repeatCount != REPEAT_INDEFINITELY && timesTriggered > repeatCount) {
            return null;
        }

        // a. Increment afterTime by a second, so that we are comparing against a time after it!
        if (afterTime == null) {
            afterTime = new Date();
        }

        java.util.Calendar sTime = java.util.Calendar.getInstance();
        sTime.setTime(afterTime);
        sTime.setLenient(true);
        if (sTime.get(java.util.Calendar.YEAR) > YEAR_TO_GIVEUP_SCHEDULING_AT) {
            return null;
        }
        Date lowerDate, upperDate, time;
        List<Date> maybeTriggeredDateList = new ArrayList<Date>();
        if(getIntervalUnit().equals(IntervalUnit.WEEK)) {
            sTime.set(java.util.Calendar.DAY_OF_WEEK, java.util.Calendar.SUNDAY);
            lowerDate = getDateWithStartTimeOfDay(sTime);
            sTime.set(java.util.Calendar.DAY_OF_WEEK,  java.util.Calendar.SATURDAY);
            upperDate = getDateWithStartTimeOfDay(sTime);
            maybeTriggeredDateList = createMaybeTriggeredDateList(7, lowerDate, upperDate, calendar);
        }
        else if(getIntervalUnit().equals(IntervalUnit.MONTH)) {
            sTime.set(java.util.Calendar.DAY_OF_MONTH, 1);
            lowerDate = getDateWithStartTimeOfDay(sTime);
            sTime.set(java.util.Calendar.DAY_OF_MONTH, sTime.getActualMaximum(java.util.Calendar.DAY_OF_MONTH));
            upperDate = getDateWithStartTimeOfDay(sTime);
            maybeTriggeredDateList = createMaybeTriggeredDateList(31, lowerDate, upperDate, calendar);
        }
        else if(getIntervalUnit().equals(IntervalUnit.SEASON)) {
            int season = sTime.get(java.util.Calendar.MONTH)/3; // 0,1,2,3
            sTime.set(java.util.Calendar.MONTH, season * 3);
            sTime.set(java.util.Calendar.DAY_OF_MONTH, 1);
            lowerDate = getDateWithStartTimeOfDay(sTime);
            sTime.set(java.util.Calendar.MONTH, season * 3 + 2);
            sTime.set(java.util.Calendar.DAY_OF_MONTH, sTime.getActualMaximum(java.util.Calendar.DAY_OF_MONTH));
            upperDate = getDateWithStartTimeOfDay(sTime);
            maybeTriggeredDateList = createMaybeTriggeredDateList(92, lowerDate, upperDate, calendar);
        }
        else if(getIntervalUnit().equals(IntervalUnit.HALF_YEAR)) {
            int half = sTime.get(java.util.Calendar.MONTH)/6; // 0,1
            sTime.set(java.util.Calendar.MONTH, half * 6);
            sTime.set(java.util.Calendar.DAY_OF_MONTH, 1);
            lowerDate = getDateWithStartTimeOfDay(sTime);
            sTime.set(java.util.Calendar.MONTH, half * 6 + 5);
            sTime.set(java.util.Calendar.DAY_OF_MONTH, sTime.getActualMaximum(java.util.Calendar.DAY_OF_MONTH));
            upperDate = getDateWithStartTimeOfDay(sTime);
            maybeTriggeredDateList = createMaybeTriggeredDateList(184, lowerDate, upperDate, calendar);
        }
        else if(getIntervalUnit().equals(IntervalUnit.YEAR)) {
            sTime.set(java.util.Calendar.DAY_OF_YEAR, 1);
            lowerDate = getDateWithStartTimeOfDay(sTime);
            sTime.set(java.util.Calendar.DAY_OF_YEAR, sTime.getActualMaximum(java.util.Calendar.DAY_OF_YEAR));
            upperDate = getDateWithStartTimeOfDay(sTime);;
            maybeTriggeredDateList = createMaybeTriggeredDateList(366, lowerDate, upperDate, calendar);
        } else {
            throw new IllegalArgumentException("TimeUnit is illegal:" + getIntervalUnit().toString());
        }
        if (maybeTriggeredDateList.isEmpty() || maybeTriggeredDateList.size() <= innerOffset) {
            return computeNextPeriod(sTime.getTime(), calendar);
        }
        time = maybeTriggeredDateList.get(reversed ? maybeTriggeredDateList.size() - 1 - innerOffset : innerOffset);

        if (getEndTime() != null && getEndTime().before(new Date(time.getTime() + outerOffset * 86400000))) {
            return null;
        }

        return time;
    }

    private List<Date> createMaybeTriggeredDateList(int initialCapacity, Date lowerDate, Date upperDate,
                                                    Calendar calendar) {
        List<Date> maybeTriggeredDateList = new ArrayList<Date>(initialCapacity);
        Date tempDate = lowerDate;
        while (!upperDate.before(tempDate)) {
            if (calendar == null || calendar.isTimeIncluded(tempDate.getTime())) {
                maybeTriggeredDateList.add((Date) tempDate.clone());
            }
            tempDate.setTime(tempDate.getTime() + 86400000);
        }
        return maybeTriggeredDateList;
    }

    /**
     * <p>
     * Called when the <code>{@link Scheduler}</code> has decided to 'fire'
     * the trigger (execute the associated <code>Job</code>), in order to
     * give the <code>Trigger</code> a chance to update itself for its next
     * triggering (if any).
     * </p>
     *
     * @see #executionComplete(JobExecutionContext, JobExecutionException)
     */
    @Override
    public void triggered(Calendar calendar) {
        timesTriggered++;
        previousFireTime = nextFireTime;
        Date fixedNextFireTime = computeNextPeriod(new Date(previousFireTime.getTime() - outerOffset * 86400000), calendar);
        nextFireTime = fixedNextFireTime == null ? null :
                new Date(fixedNextFireTime.getTime() + outerOffset * 86400000);
    }

    /**
     * <p>
     * Returns the final time at which the <code>DateIntervalTrigger</code> will
     * fire, if there is no end time set, null will be returned.
     * </p>
     *
     * <p>
     * Note that the return time may be in the past.
     * TODO bug
     * </p>
     */
    @Override
    public Date getFinalFireTime() {
        if (complete || getEndTime() == null) {
            return null;
        }

        return new Date(getEndTime().getTime());
    }

    private Date getDateWithStartTimeOfDay(java.util.Calendar sTime) {
        sTime.set(java.util.Calendar.HOUR_OF_DAY, startTimeOfDay.getHour());
        sTime.set(java.util.Calendar.MINUTE, startTimeOfDay.getMinute());
        sTime.set(java.util.Calendar.SECOND, startTimeOfDay.getSecond());
        return sTime.getTime();
    }

    @Override
    public Date getFireTimeAfter(Date afterTime) {
        return null;
    }

    /**
     * <p>
     * Determines whether or not the <code>DateIntervalTrigger</code> will occur
     * again.
     * </p>
     */
    @Override
    public boolean mayFireAgain() {
        return (getNextFireTime() != null);
    }

    /**
     * Get a {@link ScheduleBuilder} that is configured to produce a
     * schedule identical to this trigger's schedule.
     *
     * @see #getTriggerBuilder()
     */
    @Override
    public ScheduleBuilder<CalendarOffsetTrigger> getScheduleBuilder() {

        CalendarOffsetScheduleBuilder cb = CalendarOffsetScheduleBuilder.calendarOffsetSchedule()
                .withIntervalUnit(getIntervalUnit()).startingDailyAt(getStartTimeOfDay());

        switch(getMisfireInstruction()) {
            case MISFIRE_INSTRUCTION_DO_NOTHING : cb.withMisfireHandlingInstructionDoNothing();
                break;
            case MISFIRE_INSTRUCTION_FIRE_ONCE_NOW : cb.withMisfireHandlingInstructionFireAndProceed();
                break;
        }

        return cb;
    }

    public boolean hasAdditionalProperties() {
        return false;
    }

    /**
     * <p>
     * Returns the next time at which the <code>Trigger</code> is scheduled to fire. If
     * the trigger will not fire again, <code>null</code> will be returned.  Note that
     * the time returned can possibly be in the past, if the time that was computed
     * for the trigger to next fire has already arrived, but the scheduler has not yet
     * been able to fire the trigger (which would likely be due to lack of resources
     * e.g. threads).
     * </p>
     *
     * <p>The value returned is not guaranteed to be valid until after the <code>Trigger</code>
     * has been added to the scheduler.
     * </p>
     */
    @Override
    public Date getNextFireTime() {
        return nextFireTime;
    }

    /**
     * <p>
     * Returns the previous time at which the <code>DateIntervalTrigger</code>
     * fired. If the trigger has not yet fired, <code>null</code> will be
     * returned.
     */
    @Override
    public Date getPreviousFireTime() {
        return previousFireTime;
    }

    /**
     * <p>
     * Set the next time at which the <code>DateIntervalTrigger</code> should fire.
     * </p>
     *
     * <p>
     * <b>This method should not be invoked by client code.</b>
     * </p>
     */
    public void setNextFireTime(Date nextFireTime) {
        this.nextFireTime = nextFireTime;
    }

    /**
     * <p>
     * Set the previous time at which the <code>DateIntervalTrigger</code> fired.
     * </p>
     *
     * <p>
     * <b>This method should not be invoked by client code.</b>
     * </p>
     */
    public void setPreviousFireTime(Date previousFireTime) {
        this.previousFireTime = previousFireTime;
    }

    @Override
    public int getRepeatCount() {
        return repeatCount;
    }

    public void setRepeatCount(int repeatCount) {
        if (repeatCount < 0 && repeatCount != REPEAT_INDEFINITELY) {
            throw new IllegalArgumentException("Repeat count must be >= 0, use the " +
                    "constant REPEAT_INDEFINITELY for infinite.");
        }

        this.repeatCount = repeatCount;
    }

    @Override
    public TimeOfDay getStartTimeOfDay() {
        return startTimeOfDay;
    }

    public void setStartTimeOfDay(TimeOfDay startTimeOfDay) {
        if (startTimeOfDay == null) {
            throw new IllegalArgumentException("Start time of day cannot be null");
        }

        this.startTimeOfDay = startTimeOfDay;
    }

    @Override
    public int getInnerOffset() {
        return innerOffset;
    }

    public void setInnerOffset(int innerOffset) {
        if (getIntervalUnit().equals(IntervalUnit.WEEK) && innerOffset >= 7) {
            throw new IllegalArgumentException("Inner offset must be < 7");
        } else if (getIntervalUnit().equals(IntervalUnit.MONTH) && innerOffset >= 31) {
            throw new IllegalArgumentException("Inner offset must be < 31");
        } else if (getIntervalUnit().equals(IntervalUnit.SEASON) && innerOffset >= 92) {
            throw new IllegalArgumentException("Inner offset must be < 92");
        } else if (getIntervalUnit().equals(IntervalUnit.HALF_YEAR) && innerOffset >= 184) {
            throw new IllegalArgumentException("Inner offset must be < 184");
        } else if (getIntervalUnit().equals(IntervalUnit.YEAR) && innerOffset >= 366) {
            throw new IllegalArgumentException("Inner offset must be < 366");
        }
        this.innerOffset = innerOffset;
    }

    @Override
    public int getOuterOffset() {
        return outerOffset;
    }

    public void setOuterOffset(int outerOffset) {
        this.outerOffset = outerOffset;
    }

    @Override
    public boolean isReversed() {
        return reversed;
    }

    public void setReversed(boolean reversed) {
        this.reversed = reversed;
    }
}
