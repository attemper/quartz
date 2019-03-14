package org.quartz.impl.triggers;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.quartz.DailyTimeIntervalTrigger;
import org.quartz.DateBuilder;
import org.quartz.TriggerUtils;

import java.util.Date;
import java.util.List;

import static org.quartz.DateBuilder.dateOf;

/**
 * Unit test for {@link CalendarIntervalTriggerImpl}.
 * @author ldang
 */
 public class CalendarIntervalTriggerImplTest extends TestCase {


    public void testRepeatCountInf() throws Exception {
        Date startTime = dateOf(0, 0, 0, 1, 1, 2011);
        CalendarIntervalTriggerImpl trigger = new CalendarIntervalTriggerImpl();
        trigger.setStartTime(startTime);
        trigger.setRepeatIntervalUnit(DateBuilder.IntervalUnit.DAY);
        trigger.setRepeatInterval(2);

        // Setting this (which is default) should make the trigger just as normal one.
        trigger.setRepeatCount(DailyTimeIntervalTrigger.REPEAT_INDEFINITELY);

        List<Date> fireTimes = TriggerUtils.computeFireTimes(trigger, null, 5);
        Assert.assertEquals(5, fireTimes.size());
        Assert.assertEquals(dateOf(0, 0, 0, 1, 1, 2011), fireTimes.get(0));
        Assert.assertEquals(dateOf(0, 0, 0, 9, 1, 2011), fireTimes.get(4));
    }

    public void testRepeatCount() throws Exception {
        Date startTime = dateOf(0, 0, 0, 1, 1, 2011);
        CalendarIntervalTriggerImpl trigger = new CalendarIntervalTriggerImpl();
        trigger.setStartTime(startTime);
        trigger.setRepeatIntervalUnit(DateBuilder.IntervalUnit.HOUR);
        trigger.setRepeatInterval(2);
        trigger.setRepeatCount(4);

        List<Date> fireTimes = TriggerUtils.computeFireTimes(trigger, null, 48);
        Assert.assertEquals(5, fireTimes.size());
        Assert.assertEquals(dateOf(0, 0, 0, 1, 1, 2011), fireTimes.get(0));
        Assert.assertEquals(dateOf(8, 0, 0, 1, 1, 2011), fireTimes.get(4));
    }

    public void testRepeatCount0() throws Exception {
        Date startTime = dateOf(0, 0, 0, 1, 1, 2011);
        CalendarIntervalTriggerImpl trigger = new CalendarIntervalTriggerImpl();
        trigger.setStartTime(startTime);
        trigger.setRepeatIntervalUnit(DateBuilder.IntervalUnit.MINUTE);
        trigger.setRepeatInterval(72);
        trigger.setRepeatCount(0);

        List<Date> fireTimes = TriggerUtils.computeFireTimes(trigger, null, 48);
        Assert.assertEquals(1, fireTimes.size());
        Assert.assertEquals(dateOf(0, 0, 0, 1, 1, 2011), fireTimes.get(0));
    }
}
