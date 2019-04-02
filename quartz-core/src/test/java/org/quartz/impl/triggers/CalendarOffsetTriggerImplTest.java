package org.quartz.impl.triggers;

import junit.framework.TestCase;
import org.quartz.DateBuilder;
import org.quartz.TimeOfDay;
import org.quartz.TriggerUtils;
import org.quartz.impl.calendar.HolidayCalendar;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Unit test for {@link CalendarOffsetTriggerImpl}.
 * @author ldang
 */
 public class CalendarOffsetTriggerImplTest extends TestCase {

    public void test() throws Exception {
        CalendarOffsetTriggerImpl trigger = new CalendarOffsetTriggerImpl();
        trigger.setIntervalUnit(DateBuilder.IntervalUnit.WEEK);
        trigger.setStartTimeOfDay(new TimeOfDay(11, 56, 23));
        trigger.setInnerOffset(2);
        HolidayCalendar holidayCalendar = new HolidayCalendar();
        Calendar cal = Calendar.getInstance();
        cal.set(2019, 2, 19);
        holidayCalendar.addExcludedDate(cal.getTime());
        cal.set(2019, 2, 20);
        holidayCalendar.addExcludedDate(cal.getTime());
        cal.set(2019, 2, 21);
        holidayCalendar.addExcludedDate(cal.getTime());
        cal.set(2019, 2, 22);
        holidayCalendar.addExcludedDate(cal.getTime());
        cal.set(2019, 2, 23);
        holidayCalendar.addExcludedDate(cal.getTime());
        //trigger.setReversed(true);
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Date> dates = TriggerUtils.computeFireTimes(trigger, holidayCalendar, 3);
        for (Date date : dates) {
            System.out.println(dateFormat.format(date));
        }
    }
}
