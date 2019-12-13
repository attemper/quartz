package org.quartz;
import org.quartz.DateBuilder.IntervalUnit;

import java.util.Calendar;

public interface CalendarOffsetTrigger extends Trigger {

    /**
     * <p>
     * Used to indicate the 'repeat count' of the trigger is indefinite. Or in
     * other words, the trigger should repeat continually until the trigger's
     * ending timestamp.
     * </p>
     */
    public static final int REPEAT_INDEFINITELY = -1;

    /**
     * <p>
     * Instructs the <code>{@link Scheduler}</code> that upon a mis-fire
     * situation, the <code>{@link CalendarIntervalTrigger}</code> wants to be
     * fired now by <code>Scheduler</code>.
     * </p>
     */
    public static final int MISFIRE_INSTRUCTION_FIRE_ONCE_NOW = 1;
    /**
     * <p>
     * Instructs the <code>{@link Scheduler}</code> that upon a mis-fire
     * situation, the <code>{@link CalendarIntervalTrigger}</code> wants to have it's
     * next-fire-time updated to the next time in the schedule after the
     * current time (taking into account any associated <code>{@link Calendar}</code>,
     * but it does not want to be fired now.
     * </p>
     */
    public static final int MISFIRE_INSTRUCTION_DO_NOTHING = 2;

    /**
     * The time of day to start firing at the given interval.
     */
    public TimeOfDay getStartTimeOfDay();

    public int getInnerOffset();

    public int getOuterOffset();

    public boolean isReversed();

    /**
     * <p>Get the interval unit - the time unit on with the interval applies.</p>
     */
    public IntervalUnit getRepeatIntervalUnit();

    /**
     * <p>
     * Get the the number of times for interval this trigger should
     * repeat, after which it will be automatically deleted.
     * </p>
     *
     * @see #REPEAT_INDEFINITELY
     */
    public int getRepeatCount();

    /**
     * <p>
     * Get the number of times the <code>DateIntervalTrigger</code> has already
     * fired.
     * </p>
     */
    public int getTimesTriggered();

    TriggerBuilder<CalendarOffsetTrigger> getTriggerBuilder();
}
