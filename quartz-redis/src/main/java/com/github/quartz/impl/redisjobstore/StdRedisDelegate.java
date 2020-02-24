package com.github.quartz.impl.redisjobstore;

import com.fasterxml.jackson.core.type.TypeReference;
import com.github.quartz.impl.redisjobstore.delegate.*;
import com.github.quartz.impl.redisjobstore.mixin.FieldConstants;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.sync.*;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.calendar.BaseCalendar;
import org.quartz.impl.jdbcjobstore.FiredTriggerRecord;
import org.quartz.impl.jdbcjobstore.NoSuchDelegateException;
import org.quartz.impl.jdbcjobstore.TriggerStatus;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.OperableTrigger;
import org.slf4j.Logger;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;

public class StdRedisDelegate implements RedisConstants, FieldConstants {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Data members.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    protected Logger logger = null;

    protected String schedName;

    protected String instanceId;

    protected ClassLoadHelper loadHelper;

    protected List<TriggerTypeDelegate> triggerTypeDelegates = new LinkedList<TriggerTypeDelegate>();

    protected AbstractRedisClient redisClient;

    protected StatefulConnection<String, String> statefulConnection;

    protected RedisKeyCommands<String, String> redisKeyCommands;

    protected RedisStringCommands<String, String> redisStringCommands;

    protected RedisScriptingCommands<String, String> redisScriptingCommands;

    protected RedisHashCommands<String, String> redisHashCommands;

    protected RedisSetCommands<String, String> redisSetCommands;

    protected RedisSortedSetCommands<String, String> redisSortedSetCommands;

    protected RedisServerCommands<String, String> redisServerCommands;

    private static long TIME_MILLS_2099 = DateBuilder.dateOf(0, 0, 0, 1, 1, 2099).getTime();

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Interface.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    /**
     * @param initString of the format: settingName=settingValue|otherSettingName=otherSettingValue|...
     * @throws NoSuchDelegateException
     */
    public void initialize(Logger logger, String schedName, String instanceId, ClassLoadHelper classLoadHelper, String initString) throws NoSuchDelegateException {

        this.logger = logger;
        this.schedName = schedName;
        this.instanceId = instanceId;
        this.loadHelper = classLoadHelper;
        addDefaultTriggerTypeDelegates();

        if(initString == null)
            return;

        String[] settings = initString.split("\\|");

        for(String setting: settings) {
            String[] parts = setting.split("=");
            String name = parts[0];
            if(parts.length == 1 || parts[1] == null || parts[1].equals(""))
                continue;

            if(name.equals("triggerTypeDelegateClasses")) {

                String[] trigDelegates = parts[1].split(",");

                for(String trigDelClassName: trigDelegates) {
                    try {
                        Class<?> trigDelClass = classLoadHelper.loadClass(trigDelClassName);
                        addTriggerTypeDelegate((TriggerTypeDelegate) trigDelClass.newInstance());
                    } catch (Exception e) {
                        throw new NoSuchDelegateException("Error instantiating TriggerPersistenceDelegate of type: " + trigDelClassName, e);
                    }
                }
            }
            else
                throw new NoSuchDelegateException("Unknown setting: '" + name + "'");
        }
    }

    protected void addDefaultTriggerTypeDelegates() {
        addTriggerTypeDelegate(new SimpleTriggerTypeDelegate());
        addTriggerTypeDelegate(new CronTriggerTypeDelegate());
        addTriggerTypeDelegate(new CalendarIntervalTriggerTypeDelegate());
        addTriggerTypeDelegate(new DailyTimeIntervalTriggerTypeDelegate());
        addTriggerTypeDelegate(new CalendarOffsetTriggerTypeDelegate());
    }

    public void addTriggerTypeDelegate(TriggerTypeDelegate delegate) {
        this.triggerTypeDelegates.add(delegate);
    }

    public StdRedisDelegate setRedisClient(AbstractRedisClient redisClient) {
        this.redisClient = redisClient;
        return this;
    }

    public StdRedisDelegate setStatefulConnection(StatefulConnection<String, String> statefulConnection) {
        this.statefulConnection = statefulConnection;
        return this;
    }

    public StdRedisDelegate setRedisKeyCommands(RedisKeyCommands<String, String> redisKeyCommands) {
        this.redisKeyCommands = redisKeyCommands;
        return this;
    }

    public StdRedisDelegate setRedisStringCommands(RedisStringCommands<String, String> redisStringCommands) {
        this.redisStringCommands = redisStringCommands;
        return this;
    }

    public StdRedisDelegate setRedisScriptingCommands(RedisScriptingCommands<String, String> redisScriptingCommands) {
        this.redisScriptingCommands = redisScriptingCommands;
        return this;
    }

    public StdRedisDelegate setRedisHashCommands(RedisHashCommands<String, String> redisHashCommands) {
        this.redisHashCommands = redisHashCommands;
        return this;
    }

    public StdRedisDelegate setRedisSetCommands(RedisSetCommands<String, String> redisSetCommands) {
        this.redisSetCommands = redisSetCommands;
        return this;
    }

    public StdRedisDelegate setRedisSortedSetCommands(RedisSortedSetCommands<String, String> redisSortedSetCommands) {
        this.redisSortedSetCommands = redisSortedSetCommands;
        return this;
    }

    public StdRedisDelegate setRedisServerCommands(RedisServerCommands<String, String> redisServerCommands) {
        this.redisServerCommands = redisServerCommands;
        return this;
    }

    /**
     * lock
     * @param key
     * @param value
     * @param timeout
     * @return
     */
    public boolean lock(String key, String value, long timeout) {
        return redisStringCommands.set(key, value, new SetArgs().px(timeout).nx()) != null;
    }

    /**
     * lua for release lock
     */
    private static final String UNLOCK_LUA =
            "if redis.call(\"get\",KEYS[1]) == ARGV[1] " +
                    "then " +
                    "    return redis.call(\"del\",KEYS[1]) " +
                    "else " +
                    "    return 0 " +
                    "end ";

    /**
     * release lock
     * @param key
     * @param value
     * @return
     */
    public boolean release(String key, String value) {
        return redisScriptingCommands.eval(UNLOCK_LUA, ScriptOutputType.BOOLEAN, new String[]{key}, value);
    }

    public void shutdown() {
        if (statefulConnection != null) {
            statefulConnection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    // key start
    protected boolean exists(String... key) {
        return redisKeyCommands.exists(key) > 0;
    }

    protected int del(String... keys) {
        return redisKeyCommands.del(keys).intValue();
    }
    // key end

    // string start
    protected String set(String key, String value) {
        return redisStringCommands.set(key, value);
    }

    protected String get(String key) {
        return redisStringCommands.get(key);
    }
    // string end

    // set start
    protected long sadd(String key, String... members) {
       return redisSetCommands.sadd(key, members);
    }

    protected boolean sismember(String key, String member) {
        return redisSetCommands.sismember(key, member);
    }

    protected Set<String> smembers(String key) {
        return redisSetCommands.smembers(key);
    }

    protected int scard(String key) {
        return redisSetCommands.scard(key).intValue();
    }

    protected int srem(String key, String... members) {
        return redisSetCommands.srem(key, members).intValue();
    }

    protected List<String> sscan(String key, String match) {
        return redisSetCommands.sscan(key, new ScanArgs().match(match)).getValues();
    }
    /*
    protected boolean sscan(String key, String match, int limit) {
        return redisSetCommands.sscan(key, new ScanArgs().match(match).limit(limit)).getValues().size() > 0;
    }*/
    /**
     * make set empty
     * @param key
     * @return
     */
    /*protected Set<String> spop(String key) {
        return redisSetCommands.spop(key, 0);
    }*/
    // set end

    // sorted set start
    protected long zadd(String key, double score, String member) {
        return redisSortedSetCommands.zadd(key, score, member);
    }

    protected List<String> zrangebyscore(String key, long includedEnd) {
        return redisSortedSetCommands.zrangebyscore(key, Range.create(0, includedEnd));
    }

    protected int zcard(String key) {
        return redisSortedSetCommands.zcard(key).intValue();
    }

    protected int zrem(String key, String... members) {
        return redisSortedSetCommands.zrem(key, members).intValue();
    }

    protected List<ScoredValue<String>> zscan(String key, String match) {
        return redisSortedSetCommands.zscan(key, new ScanArgs().match(match)).getValues();
    }
    // sorted set end

    // hash(obj) start
    protected String hmset(String key, Map<String, String> map) {
        return redisHashCommands.hmset(key, map);
    }

    protected boolean hset(String key, String field, String value) {
        return redisHashCommands.hset(key, field, value);
    }

    protected Map<String, String> hgetall(String key) {
        return redisHashCommands.hgetall(key);
    }

    protected String hget(String key, String field) {
        return redisHashCommands.hget(key, field);
    }

    protected List<KeyValue<String, String>> hmget(String key, String... fields) {
        return redisHashCommands.hmget(key, fields);
    }

    protected int hdel(String key, String... fields) {
        return redisHashCommands.hdel(key, fields).intValue();
    }
    // hash(obj) end

    // server start
    protected String flushdb() {
        return redisServerCommands.flushdb();
    }
    // server end
    protected String keyOfJob(JobKey jobKey) {
        return MessageFormat.format(KEY_JOB, schedName, jobKey.getName(), jobKey.getGroup());
    }

    protected String keyOfJobs() {
        return MessageFormat.format(KEY_JOBS, schedName);
    }

    protected String keyOfTrigger(TriggerKey triggerKey) {
        return MessageFormat.format(KEY_TRIGGER, schedName, triggerKey.getName(), triggerKey.getGroup());
    }

    protected String keyOfTriggers() {
        return MessageFormat.format(KEY_TRIGGERS, schedName);
    }

    protected String keyOfWaitTriggers() {
        return MessageFormat.format(KEY_WAIT_TRIGGERS, schedName);
    }

    protected String keyOfJobTriggers(JobKey jobKey) {
        return MessageFormat.format(KEY_JOB_TRIGGERS, schedName, jobKey.getName(), jobKey.getGroup());
    }

    protected String keyOfFiredJobs() {
        return MessageFormat.format(KEY_FIRED_JOBS, schedName, instanceId);
    }

    protected String keyOfFiredJob(String entryId) {
        return MessageFormat.format(KEY_FIRED_JOB, schedName, entryId);
    }

    protected String keyOfPausedTriggerGroups() {
        return MessageFormat.format(KEY_PAUSED_TRIGGER_GROUPS, schedName);
    }

    protected String keyOfCalendar(String calendarName) {
        return MessageFormat.format(KEY_CALENDAR, schedName, calendarName);
    }

    protected String keyOfCalendars() {
        return MessageFormat.format(KEY_CALENDARS, schedName);
    }

    protected String keyOfCalendarTriggers(String calendarName) {
        return MessageFormat.format(KEY_CALENDAR_TRIGGERS, schedName, calendarName);
    }

    protected String[] splitValue(String originValue) {
        return originValue.split(VALUE_DELIMITER);
    }

    protected String joinValue(TriggerKey triggerKey) {
        return joinValue(triggerKey.getName(), triggerKey.getGroup());
    }

    protected String joinValue(JobKey jobKey) {
        return joinValue(jobKey.getName(), jobKey.getGroup());
    }

    protected String joinValue(String elementName, String groupName) {
        return new StringBuilder(elementName).append(VALUE_DELIMITER).append(groupName).toString();
    }

    protected TriggerTypeDelegate findTriggerTypeDelegate(OperableTrigger trigger)  {
        for(TriggerTypeDelegate delegate: triggerTypeDelegates) {
            if(delegate.canHandleTriggerType(trigger))
                return delegate;
        }

        return null;
    }

    //---------------------------------------------------------------------------
    // startup / recovery
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Select all of the triggers in a given state.
     * </p>
     *
     * @param conn
     *          the DB Connection
     * @param state
     *          the state the triggers must be in
     * @return an array of trigger <code>Key</code> s
     */
    /** TDOO ldang
    public List<TriggerKey> selectTriggersInState(Connection conn, String state)
            throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(SELECT_TRIGGERS_IN_STATE));
            ps.setString(1, state);
            rs = ps.executeQuery();

            LinkedList<TriggerKey> list = new LinkedList<TriggerKey>();
            while (rs.next()) {
                list.add(triggerKey(rs.getString(1), rs.getString(2)));
            }

            return list;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }
     */

    /**
     * <p>
     * Get the names of all of the triggers in the given state that have
     * misfired - according to the given timestamp.  No more than count will
     * be returned.
     * </p>
     *
     * @param conn The DB Connection
     * @param count The most misfired triggers to return, negative for all
     * @param resultList Output parameter.  A List of
     *      <code>{@link org.quartz.utils.Key}</code> objects.  Must not be null.
     *
     * @return Whether there are more misfired triggers left to find beyond
     *         the given count.
     */
    /** TDOO ldang
    public boolean hasMisfiredTriggersInState(Connection conn, String state1,
                                              long ts, int count, List<TriggerKey> resultList) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(SELECT_HAS_MISFIRED_TRIGGERS_IN_STATE));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, state1);
            rs = ps.executeQuery();

            boolean hasReachedLimit = false;
            while (rs.next() && (hasReachedLimit == false)) {
                if (resultList.size() == count) {
                    hasReachedLimit = true;
                } else {
                    String triggerName = rs.getString(COL_TRIGGER_NAME);
                    String groupName = rs.getString(COL_TRIGGER_GROUP);
                    resultList.add(triggerKey(triggerName, groupName));
                }
            }

            return hasReachedLimit;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }
     */

    /**
     * <p>
     * Get the number of triggers in the given states that have
     * misfired - according to the given timestamp.
     * </p>
     *
     * @param conn the DB Connection
     */
    /** TDOO ldang
    public int countMisfiredTriggersInState(
            Connection conn, String state1, long ts) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(rtp(COUNT_MISFIRED_TRIGGERS_IN_STATE));
            ps.setBigDecimal(1, new BigDecimal(String.valueOf(ts)));
            ps.setString(2, state1);
            rs = ps.executeQuery();

            if (rs.next()) {
                return rs.getInt(1);
            }

            throw new SQLException("No misfired trigger count returned.");
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }
     */

    /**
     * <p>
     * Select all of the triggers for jobs that are requesting recovery. The
     * returned trigger objects will have unique "recoverXXX" trigger names and
     * will be in the <code>{@link
     * org.quartz.Scheduler}.DEFAULT_RECOVERY_GROUP</code>
     * trigger group.
     * </p>
     *
     * <p>
     * In order to preserve the ordering of the triggers, the fire time will be
     * set from the <code>COL_FIRED_TIME</code> column in the <code>TABLE_FIRED_TRIGGERS</code>
     * table. The caller is responsible for calling <code>computeFirstFireTime</code>
     * on each returned trigger. It is also up to the caller to insert the
     * returned triggers to ensure that they are fired.
     * </p>
     *
     * @param conn
     *          the DB Connection
     * @return an array of <code>{@link org.quartz.Trigger}</code> objects
     */
    /** TDOO ldang
    public List<OperableTrigger> selectTriggersForRecoveringJobs(Connection conn)
            throws SQLException, IOException, ClassNotFoundException {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn
                    .prepareStatement(rtp(SELECT_INSTANCES_RECOVERABLE_FIRED_TRIGGERS));
            ps.setString(1, instanceId);
            setBoolean(ps, 2, true);
            rs = ps.executeQuery();

            long dumId = System.currentTimeMillis();
            LinkedList<OperableTrigger> list = new LinkedList<OperableTrigger>();
            while (rs.next()) {
                String jobName = rs.getString(COL_JOB_NAME);
                String jobGroup = rs.getString(COL_JOB_GROUP);
                String trigName = rs.getString(COL_TRIGGER_NAME);
                String trigGroup = rs.getString(COL_TRIGGER_GROUP);
                long firedTime = rs.getLong(COL_FIRED_TIME);
                long scheduledTime = rs.getLong(COL_SCHED_TIME);
                int priority = rs.getInt(COL_PRIORITY);
                @SuppressWarnings("deprecation")
                SimpleTriggerImpl rcvryTrig = new SimpleTriggerImpl("recover_"
                        + instanceId + "_" + String.valueOf(dumId++),
                        Scheduler.DEFAULT_RECOVERY_GROUP, new Date(scheduledTime));
                rcvryTrig.setJobName(jobName);
                rcvryTrig.setJobGroup(jobGroup);
                rcvryTrig.setPriority(priority);
                rcvryTrig.setMisfireInstruction(SimpleTrigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY);

                JobDataMap jd = selectTriggerJobDataMap(conn, trigName, trigGroup);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_NAME, trigName);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_GROUP, trigGroup);
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_FIRETIME_IN_MILLISECONDS, String.valueOf(firedTime));
                jd.put(Scheduler.FAILED_JOB_ORIGINAL_TRIGGER_SCHEDULED_FIRETIME_IN_MILLISECONDS, String.valueOf(scheduledTime));
                rcvryTrig.setJobDataMap(jd);

                list.add(rcvryTrig);
            }
            return list;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }
    */

    /**
     * <p>
     * Delete all fired triggers.
     * </p>
     *
     * @param conn
     *          the DB Connection
     * @return the number of rows deleted
     */
    /** TDOO ldang
    public int deleteFiredTriggers(Connection conn) throws SQLException {
        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(rtp(DELETE_FIRED_TRIGGERS));

            return ps.executeUpdate();
        } finally {
            closeStatement(ps);
        }
    }

    public int deleteFiredTriggers(Connection conn, String theInstanceId)
            throws SQLException {
        PreparedStatement ps = null;

        try {
            ps = conn.prepareStatement(rtp(DELETE_INSTANCES_FIRED_TRIGGERS));
            ps.setString(1, theInstanceId);

            return ps.executeUpdate();
        } finally {
            closeStatement(ps);
        }
    }
    */

    /**
     * Clear (delete!) all scheduling data - all {@link Job}s, {@link Trigger}s
     * {@link Calendar}s.
     *
     */
    public void clearData() {
        flushdb();
    }

    //---------------------------------------------------------------------------
    // jobs
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Insert the job detail record.
     * </p>
     *
     * @param job
     *          the job to insert
     * @return number of rows inserted
     */
    public void insertJobDetail(JobDetail job) {
        sadd(keyOfJobs(), joinValue(job.getKey()));
        updateJobDetail(job);
    }

    /**
     * <p>
     * Update the job detail record.
     * </p>
     *
     * @param job
     *          the job to update
     */
    public void updateJobDetail(JobDetail job) {
        String keyOfJobDetail = keyOfJob(job.getKey());
        Map<String, String> map = Helper.getObjectMapper()
                .convertValue(job, new TypeReference<HashMap<String, String>>() {});
        map.put(FieldConstants.FIELD_REQUESTS_RECOVERY, String.valueOf(job.requestsRecovery()));
        hmset(keyOfJobDetail, map);
    }

    /**
     * <p>
     * Get the names of all of the triggers associated with the given job.
     * </p>
     *
     * @return an array of <code>{@link
     * org.quartz.utils.Key}</code> objects
     */
    public List<TriggerKey> selectTriggerKeysForJob(JobKey jobKey) {
        LinkedList<TriggerKey> list = new LinkedList<TriggerKey>();

        Set<String> groupWithNames = smembers(keyOfJobTriggers(jobKey));
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            list.add(new TriggerKey(array[0], array[1]));
        }
        return list;
    }

    /**
     * <p>
     * Delete the job detail record for the given job.
     * </p>
     *
     * @return the number of rows deleted
     */
    public long deleteJobDetail(JobKey jobKey) {
        String kjt = keyOfJobTriggers(jobKey);
        if (exists(kjt)) {
            srem(kjt);
        }
        del(keyOfJob(jobKey));
        return srem(keyOfJobs(), joinValue(jobKey));
    }

    /**
     * <p>
     * Check whether or not the given job exists.
     * </p>
     *
     * @return true if the job exists, false otherwise
     */
    protected boolean jobExists(JobKey jobKey) {
        String key = keyOfJob(jobKey);
        return exists(key);
    }

    /**
     * <p>
     * Update the job data map for the given job.
     * </p>
     *
     * @param job
     *          the job to update
     */
    public void updateJobData(JobDetail job)
            throws IOException {
        JobDataMap jobDataMap = job.getJobDataMap();
        String s = Helper.getObjectMapper().convertValue(jobDataMap, String.class);
        hset(keyOfJob(job.getKey()), COL_JOB_DATAMAP, s);
    }

    /**
     * <p>
     * Select the JobDetail object for a given job name / group name.
     * </p>
     *
     * @return the populated JobDetail object
     *           the job class could not be found
     */
    public JobDetail selectJobDetail(JobKey jobKey) {
        if (!jobExists(jobKey)) {
            return null;
        }
        Map<String, String> jobDetailMap = hgetall(keyOfJob(jobKey));
        return Helper.getObjectMapper().convertValue(jobDetailMap, JobDetailImpl.class);
    }

    /**
     * <p>
     * Select the total number of jobs stored.
     * </p>
     *
     * @return the total number of jobs stored
     */
    public int selectNumJobs() {
        return scard(keyOfJobs());
    }

    /**
     * <p>
     * Select all of the job group names that are stored.
     * </p>
     *
     * @return an array of <code>String</code> group names
     */
    public List<String> selectJobGroups() {
        Set<String> groupWithNames = smembers(keyOfJobs());
        Set<String> groups = new HashSet<>();
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            groups.add(array[1]);
        }
        return new LinkedList<>(groups);
    }

    /**
     * <p>
     * Select all of the jobs contained in a given group.
     * </p>
     *
     * @param matcher
     *          the groupMatcher to evaluate the jobs against
     * @return an array of <code>String</code> job names
     */
    public Set<JobKey> selectJobsInGroup(GroupMatcher<JobKey> matcher) {
        List<String> groupWithNames = sscan(keyOfJobs(), toGroupLikeClause(matcher));
        Set<JobKey> results = new HashSet<JobKey>(groupWithNames.size());
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            results.add(new JobKey(array[0], array[1]));
        }
        return results;
    }

    protected String toGroupLikeClause(final GroupMatcher<?> matcher) {
        String groupName;
        switch(matcher.getCompareWithOperator()) {
            case EQUALS:
                groupName = "*" + VALUE_DELIMITER + matcher.getCompareToValue();
                break;
            case CONTAINS:
                groupName = "*" + VALUE_DELIMITER + "*" + matcher.getCompareToValue() + "*";
                break;
            case ENDS_WITH:
                groupName = "*" + VALUE_DELIMITER + "*" + matcher.getCompareToValue();
                break;
            case STARTS_WITH:
                groupName = "*" + VALUE_DELIMITER + matcher.getCompareToValue() + "*";
                break;
            case ANYTHING:
                groupName = "*";
                break;
            default:
                throw new UnsupportedOperationException("Don't know how to translate " + matcher.getCompareWithOperator() + " into SQL");
        }
        return groupName;
    }

    //---------------------------------------------------------------------------
    // triggers
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Insert the base trigger data.
     * </p>
     *
     * @param trigger
     *          the trigger to insert
     * @param state
     *          the state that the trigger should be stored in
     * @return the number of rows inserted
     */
    public void insertTrigger(OperableTrigger trigger, String state, JobDetail job) {
        updateTrigger(trigger, state, job);
        sadd(keyOfTriggers(), joinValue(trigger.getKey()));
    }

    /**
     * <p>
     * Insert the blob trigger data.
     * </p>
     *
     * @param trigger
     *          the trigger to insert
     * @return the number of rows inserted
     */
    public void updateTrigger(OperableTrigger trigger, String state, JobDetail job) {
        Map<String, String> triggerMap = Helper.getObjectMapper()
                .convertValue(trigger, new TypeReference<HashMap<String, String>>() {});

        long prevFireTime = -1;
        if (trigger.getPreviousFireTime() != null) {
            prevFireTime = trigger.getPreviousFireTime().getTime();
        }
        triggerMap.put(COL_PREV_FIRE_TIME, String.valueOf(prevFireTime));
        triggerMap.put(COL_TRIGGER_STATE, state);

        TriggerTypeDelegate tDel = findTriggerTypeDelegate(trigger);
        String type;
        if(tDel != null) {
            type = tDel.getHandledTriggerTypeDiscriminator();
        } else {
            type = TTYPE_BLOB;
        }
        triggerMap.put(COL_TRIGGER_TYPE, type);
        triggerMap.put(COL_START_TIME, String.valueOf(trigger
                .getStartTime().getTime()));
        long endTime = 0;
        if (trigger.getEndTime() != null) {
            endTime = trigger.getEndTime().getTime();
        }
        triggerMap.put(COL_END_TIME, String.valueOf(endTime));
        hmset(keyOfTrigger(trigger.getKey()), triggerMap);
        sadd(keyOfJobTriggers(job.getKey()), joinValue(trigger.getKey()));
        if (STATE_WAITING.equals(state) && trigger.getNextFireTime() != null) {
            zadd(keyOfWaitTriggers(),
                    trigger.getNextFireTime().getTime(),
                    joinValue(trigger.getKey()));
        } else {
            zrem(keyOfWaitTriggers(), joinValue(trigger.getKey()));
        }
        /*
        int insertResult = 0;

        try {
            ps = conn.prepareStatement(rtp(INSERT_TRIGGER));
            ps.setString(1, trigger.getKey().getName());
            ps.setString(2, trigger.getKey().getGroup());
            ps.setString(3, trigger.getJobKey().getName());
            ps.setString(4, trigger.getJobKey().getGroup());
            ps.setString(5, trigger.getDescription());
            if(trigger.getNextFireTime() != null)
                ps.setBigDecimal(6, new BigDecimal(String.valueOf(trigger
                        .getNextFireTime().getTime())));
            else
                ps.setBigDecimal(6, null);
            long prevFireTime = -1;
            if (trigger.getPreviousFireTime() != null) {
                prevFireTime = trigger.getPreviousFireTime().getTime();
            }
            ps.setBigDecimal(7, new BigDecimal(String.valueOf(prevFireTime)));
            ps.setString(8, state);

            TriggerPersistenceDelegate tDel = findTriggerPersistenceDelegate(trigger);

            String type = TTYPE_BLOB;
            if(tDel != null)
                type = tDel.getHandledTriggerTypeDiscriminator();
            ps.setString(9, type);

            ps.setBigDecimal(10, new BigDecimal(String.valueOf(trigger
                    .getStartTime().getTime())));
            long endTime = 0;
            if (trigger.getEndTime() != null) {
                endTime = trigger.getEndTime().getTime();
            }
            ps.setBigDecimal(11, new BigDecimal(String.valueOf(endTime)));
            ps.setString(12, trigger.getCalendarName());
            ps.setInt(13, trigger.getMisfireInstruction());
            setBytes(ps, 14, baos);
            ps.setInt(15, trigger.getPriority());

            insertResult = ps.executeUpdate();

            if(tDel == null)
                insertBlobTrigger(conn, trigger);
            else
                tDel.insertExtendedTriggerProperties(conn, trigger, state, jobDetail);

        } finally {
            closeStatement(ps);
        }

        return insertResult;*/
    }

    /**
     * <p>
     * Check whether or not a trigger exists.
     * </p>
     *
     * @return true if the trigger exists, false otherwise
     */
    protected boolean triggerExists(TriggerKey triggerKey) {
        String key = keyOfTrigger(triggerKey);
        return exists(key);
    }

    /**
     * <p>
     * Update the state for a given trigger.
     * </p>
     *
     * @param state
     *          the new state for the trigger
     * @return the number of rows updated
     */
    public boolean updateTriggerState(TriggerKey triggerKey,
                                  String state) {
        return hset(keyOfTrigger(triggerKey), COL_TRIGGER_STATE, state);
    }

    /**
     * <p>
     * Update the given trigger to the given new state, if it is one of the
     * given old states.
     * </p>
     *
     * @param newState
     *          the new state for the trigger
     * @param oldState1
     *          one of the old state the trigger must be in
     * @param oldState2
     *          one of the old state the trigger must be in
     * @param oldState3
     *          one of the old state the trigger must be in
     * @return int the number of rows updated
     */
    protected int updateTriggerStateFromOtherStates(TriggerKey triggerKey, String newState, String oldState1,
                                                 String oldState2, String oldState3) {
        String keyOfTrigger = keyOfTrigger(triggerKey);
        String triggerState = hget(keyOfTrigger, COL_TRIGGER_STATE);
        if (triggerState != null && (triggerState.equals(oldState1) ||
                triggerState.equals(oldState2) || triggerState.equals(oldState3))) {
            hset(keyOfTrigger, COL_TRIGGER_STATE, newState);
            return 1;
        }
        return 0;
    }

    /**
     * <p>
     * Update all triggers in the given group to the given new state, if they
     * are in one of the given old states.
     * </p>
     *
     * @param matcher
     *          the groupMatcher to evaluate the triggers against
     * @param newState
     *          the new state for the trigger
     * @param oldState1
     *          one of the old state the trigger must be in
     * @param oldState2
     *          one of the old state the trigger must be in
     * @param oldState3
     *          one of the old state the trigger must be in
     * @return int the number of rows updated
     */
    public void updateTriggerGroupStateFromOtherStates(GroupMatcher<TriggerKey> matcher, String newState, String oldState1,
                                                      String oldState2, String oldState3) {
        Set<TriggerKey> triggerKeys = selectTriggersInGroup(matcher);
        for (TriggerKey triggerKey : triggerKeys) {
            updateTriggerStateFromOtherStates(triggerKey, newState, oldState1, oldState2, oldState3);
        }
    }

    /**
     * <p>
     * Update the given trigger to the given new state, if it is in the given
     * old state.
     * </p>
     *
     * @param newState
     *          the new state for the trigger
     * @param oldState
     *          the old state the trigger must be in
     * @return int the number of rows updated
     */
    public int updateTriggerStateFromOtherState(TriggerKey triggerKey, String newState, String oldState) {
        String keyOfTrigger = keyOfTrigger(triggerKey);
        String triggerState = hget(keyOfTrigger, COL_TRIGGER_STATE);
        if (triggerState != null && triggerState.equals(oldState)) {
            hset(keyOfTrigger, COL_TRIGGER_STATE, newState);
            return 1;
        }
        return 0;
    }

    /**
     * <p>
     * Update all of the triggers of the given group to the given new state, if
     * they are in the given old state.
     * </p>
     *
     * @param matcher
     *          the groupMatcher to evaluate the triggers against
     * @param newState
     *          the new state for the trigger group
     * @param oldState
     *          the old state the triggers must be in
     * @return int the number of rows updated
     */
    public void updateTriggerGroupStateFromOtherState(GroupMatcher<TriggerKey> matcher, String newState, String oldState) {
        Set<TriggerKey> triggerKeys = selectTriggersInGroup(matcher);
        for (TriggerKey triggerKey : triggerKeys) {
            updateTriggerStateFromOtherState(triggerKey, newState, oldState);
        }
    }

    /**
     * <p>
     * Update the states of all triggers associated with the given job.
     * </p>
     *
     * @param state
     *          the new state for the triggers
     * @return the number of rows updated
     */
    public void updateTriggerStatesForJob(JobKey jobKey,
                                         String state) {
        Set<String> groupWithNames = smembers(keyOfJobTriggers(jobKey));
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            updateTriggerState(new TriggerKey(array[0], array[1]), state);
        }
    }

    public void updateTriggerStatesForJobFromOtherState(JobKey jobKey, String state, String oldState) {
        Set<String> groupWithNames = smembers(keyOfJobTriggers(jobKey));
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            updateTriggerStateFromOtherState(new TriggerKey(array[0], array[1]), state, oldState);
        }
    }

    /**
     * <p>
     * Delete the base trigger data for a trigger.
     * </p>
     *
     * @return the number of rows deleted
     */
    public long deleteTrigger(TriggerKey triggerKey) {
        String value = joinValue(triggerKey);
        List<KeyValue<String, String>> keyValues = hmget(keyOfTrigger(triggerKey), FIELD_JOB_NAME, FIELD_JOB_GROUP);
        String jobName = null, jobGroup = null;
        for (KeyValue<String, String> keyValue : keyValues) {
            if (FIELD_JOB_NAME.equals(keyValue.getKey())) {
                jobName = keyValue.getValue();
            } else if (FIELD_JOB_GROUP.equals(keyValue.getKey())) {
                jobGroup = keyValue.getValue();
            }
        }
        srem(keyOfJobTriggers(new JobKey(jobName, jobGroup)), value);  // remove trigger in job-triggers
        del(keyOfTrigger(triggerKey)); // remove trigger
        zrem(keyOfWaitTriggers(), value); // remove waiting triggers
        return srem(keyOfTriggers(), value); // remove trigger in triggers
    }

    /**
     * <p>
     * Select the number of triggers associated with a given job.
     * </p>
     *
     * @return the number of triggers for the given job
     */
    public int selectNumTriggersForJob(JobKey jobKey) {
        return scard(keyOfJobTriggers(jobKey));
    }

    /**
     * <p>
     * Select the job to which the trigger is associated.
     * </p>
     *
     * @return the <code>{@link org.quartz.JobDetail}</code> object
     *         associated with the given trigger
     * @throws ClassNotFoundException
     */
    public JobDetail selectJobForTrigger(TriggerKey triggerKey) throws ClassNotFoundException {
        return selectJobForTrigger(triggerKey, true);
    }

    /**
     * <p>
     * Select the job to which the trigger is associated. Allow option to load actual job class or not. When case of
     * remove, we do not need to load the class, which in many cases, it's no longer exists.
     *
     * </p>
     *
     * @return the <code>{@link org.quartz.JobDetail}</code> object
     *         associated with the given trigger
     * @throws ClassNotFoundException
     */
    public JobDetail selectJobForTrigger(TriggerKey triggerKey, boolean loadJobClass) throws ClassNotFoundException {
        String jobName = hget(keyOfTrigger(triggerKey), FIELD_JOB_NAME);
        Map<String, String> jobDetailMap = hgetall(keyOfJob(new JobKey(jobName, triggerKey.getGroup())));
        JobDetailImpl job = Helper.getObjectMapper().convertValue(jobDetailMap, JobDetailImpl.class);
        return job;
    }

    /**
     * <p>
     * Select the triggers for a job
     * </p>
     *
     * @return an array of <code>(@link org.quartz.Trigger)</code> objects
     *         associated with a given job.
     */
    public List<OperableTrigger> selectTriggersForJob(JobKey jobKey) {

        LinkedList<OperableTrigger> trigList = new LinkedList<OperableTrigger>();

        Set<String> groupWithNames = smembers(keyOfJobTriggers(jobKey));
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            OperableTrigger t = selectTrigger(new TriggerKey(array[0], array[1]));
            if(t != null) {
                trigList.add(t);
            }
        }

        return trigList;
    }

    public List<OperableTrigger> selectTriggersForCalendar(String calName)
            throws JobPersistenceException {

        LinkedList<OperableTrigger> trigList = new LinkedList<OperableTrigger>();
        Set<String> triggerGroupWithNames = smembers(keyOfCalendarTriggers(calName));
        for (String groupWithName : triggerGroupWithNames) {
            String[] array = splitValue(groupWithName);
            trigList.add(selectTrigger(new TriggerKey(array[0], array[1])));
        }
        return trigList;
    }

    /**
     * <p>
     * Select a trigger.
     * </p>
     *
     * @return the <code>{@link org.quartz.Trigger}</code> object
     * @throws JobPersistenceException
     */
    public OperableTrigger selectTrigger(TriggerKey triggerKey) {
        Map<String, String> triggerMap = hgetall(keyOfTrigger(triggerKey));
        String type = triggerMap.get(COL_TRIGGER_TYPE);
        return null;
        /**
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            OperableTrigger trigger = null;

            ps = conn.prepareStatement(rtp(SELECT_TRIGGER));
            ps.setString(1, triggerKey.getName());
            ps.setString(2, triggerKey.getGroup());
            rs = ps.executeQuery();

            if (rs.next()) {
                String jobName = rs.getString(COL_JOB_NAME);
                String jobGroup = rs.getString(COL_JOB_GROUP);
                String description = rs.getString(COL_DESCRIPTION);
                long nextFireTime = rs.getLong(COL_NEXT_FIRE_TIME);
                long prevFireTime = rs.getLong(COL_PREV_FIRE_TIME);
                String triggerType = rs.getString(COL_TRIGGER_TYPE);
                long startTime = rs.getLong(COL_START_TIME);
                long endTime = rs.getLong(COL_END_TIME);
                String calendarName = rs.getString(COL_CALENDAR_NAME);
                int misFireInstr = rs.getInt(COL_MISFIRE_INSTRUCTION);
                int priority = rs.getInt(COL_PRIORITY);

                Map<?, ?> map = null;
                if (canUseProperties()) {
                    map = getMapFromProperties(rs);
                } else {
                    map = (Map<?, ?>) getObjectFromBlob(rs, COL_JOB_DATAMAP);
                }

                Date nft = null;
                if (nextFireTime > 0) {
                    nft = new Date(nextFireTime);
                }

                Date pft = null;
                if (prevFireTime > 0) {
                    pft = new Date(prevFireTime);
                }
                Date startTimeD = new Date(startTime);
                Date endTimeD = null;
                if (endTime > 0) {
                    endTimeD = new Date(endTime);
                }

                if (triggerType.equals(TTYPE_BLOB)) {
                    rs.close(); rs = null;
                    ps.close(); ps = null;

                    ps = conn.prepareStatement(rtp(SELECT_BLOB_TRIGGER));
                    ps.setString(1, triggerKey.getName());
                    ps.setString(2, triggerKey.getGroup());
                    rs = ps.executeQuery();

                    if (rs.next()) {
                        trigger = (OperableTrigger) getObjectFromBlob(rs, COL_BLOB);
                    }
                }
                else {
                    TriggerPersistenceDelegate tDel = findTriggerPersistenceDelegate(triggerType);

                    if(tDel == null)
                        throw new JobPersistenceException("No TriggerPersistenceDelegate for trigger discriminator type: " + triggerType);

                    TriggerPersistenceDelegate.TriggerPropertyBundle triggerProps = null;
                    try {
                        triggerProps = tDel.loadExtendedTriggerProperties(conn, triggerKey);
                    } catch (IllegalStateException isex) {
                        if (isTriggerStillPresent(ps)) {
                            throw isex;
                        } else {
                            // QTZ-386 Trigger has been deleted
                            return null;
                        }
                    }

                    TriggerBuilder<?> tb = newTrigger()
                            .withDescription(description)
                            .withPriority(priority)
                            .startAt(startTimeD)
                            .endAt(endTimeD)
                            .withIdentity(triggerKey)
                            .modifiedByCalendar(calendarName)
                            .withSchedule(triggerProps.getScheduleBuilder())
                            .forJob(jobKey(jobName, jobGroup));

                    if (null != map) {
                        tb.usingJobData(new JobDataMap(map));
                    }

                    trigger = (OperableTrigger) tb.build();

                    trigger.setMisfireInstruction(misFireInstr);
                    trigger.setNextFireTime(nft);
                    trigger.setPreviousFireTime(pft);

                    setTriggerStateProperties(trigger, triggerProps);
                }
            }

            return trigger;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }*/

    }

    /**
     * <p>
     * Select a trigger' state value.
     * </p>
     *
     * @return the <code>{@link org.quartz.Trigger}</code> object
     */
    public String selectTriggerState(TriggerKey triggerKey) {
        String state = hget(keyOfTrigger(triggerKey), COL_TRIGGER_STATE);
        return (state != null ? state : STATE_DELETED).intern();
    }

    /**
     * <p>
     * Select a trigger' status (state & next fire time).
     * </p>
     *
     * @return a <code>TriggerStatus</code> object, or null
     */
    public TriggerStatus selectTriggerStatus(TriggerKey triggerKey) {
        Map<String, String> triggerMap = hgetall(keyOfTrigger(triggerKey));
        String state = triggerMap.get(COL_TRIGGER_STATE);
        long nextFireTime = Long.parseLong(triggerMap.get(COL_NEXT_FIRE_TIME));
        String jobName = triggerMap.get(COL_JOB_NAME);
        String jobGroup = triggerMap.get(COL_JOB_GROUP);

        Date nft = null;
        if (nextFireTime > 0) {
            nft = new Date(nextFireTime);
        }

        TriggerStatus status = new TriggerStatus(state, nft);
        status.setKey(triggerKey);
        status.setJobKey(new JobKey(jobName, jobGroup));

        return status;

    }

    /**
     * <p>
     * Select the total number of triggers stored.
     * </p>
     *
     * @return the total number of triggers stored
     */
    public int selectNumTriggers() {
        return scard(keyOfTriggers());
    }

    /**
     * <p>
     * Select all of the trigger group names that are stored.
     * </p>
     *
     * @return an array of <code>String</code> group names
     */
    public List<String> selectTriggerGroups() {
        Set<String> groupWithNames = smembers(keyOfTriggers());
        Set<String> groups = new HashSet<>();
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            groups.add(array[1]);
        }
        return new LinkedList<>(groups);
    }

    public List<String> selectTriggerGroups(GroupMatcher<TriggerKey> matcher) {
        List<String> groupWithNames = sscan(keyOfTriggers(), toGroupLikeClause(matcher));
        List<String> results = new LinkedList<String>();
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            results.add(array[1]);
        }
        return results;
    }

    /**
     * <p>
     * Select all of the triggers contained in a given group.
     * </p>
     *
     * @param matcher
     *          to evaluate against known triggers
     * @return a Set of <code>TriggerKey</code>s
     */
    public Set<TriggerKey> selectTriggersInGroup(GroupMatcher<TriggerKey> matcher) {
        List<String> groupWithNames = sscan(keyOfTriggers(), toGroupLikeClause(matcher));
        Set<TriggerKey> results = new HashSet<TriggerKey>(groupWithNames.size());
        for (String item : groupWithNames) {
            String[] array = splitValue(item);
            results.add(new TriggerKey(array[0], array[1]));
        }
        return results;
    }

    public long insertPausedTriggerGroup(String groupName) {
        return sadd(keyOfPausedTriggerGroups(), groupName);
    }

    public int deletePausedTriggerGroup(String groupName) {
        return srem(keyOfPausedTriggerGroups(), groupName);
    }

    public void deletePausedTriggerGroup(GroupMatcher<TriggerKey> matcher) {
        String key = keyOfPausedTriggerGroups();
        List<String> groupNames = sscan(key, toGroupLikeClause(matcher));
        srem(key, groupNames.toArray(new String[]{}));
    }

    public boolean isTriggerGroupPaused(String groupName) {
        return sismember(keyOfPausedTriggerGroups(), groupName);
    }

    //---------------------------------------------------------------------------
    // calendars
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Update a calendar.
     * </p>
     *
     * @param calendarName
     *          the name for the new calendar
     * @param calendar
     *          the calendar
     * @return the number of rows updated
     * @throws IOException
     *           if there were problems serializing the calendar
     */
    public String updateCalendar(String calendarName,
                              Calendar calendar) throws IOException {
        String calendarInfo = Helper.getObjectMapper().writeValueAsString(calendar);
        return set(keyOfCalendar(calendarName), calendarInfo);
    }

    /**
     * <p>
     * Update a calendar.
     * </p>
     *
     * @param calendarName
     *          the name for the new calendar
     * @param calendar
     *          the calendar
     * @return the number of rows updated
     * @throws IOException
     *           if there were problems serializing the calendar
     */
    public String insertCalendar(String calendarName,
                                 Calendar calendar) throws IOException {
        sadd(keyOfCalendars(), calendarName);
        return updateCalendar(calendarName, calendar);
    }

    /**
     * <p>
     * Check whether or not a calendar exists.
     * </p>
     *
     * @param calendarName
     *          the name of the calendar
     * @return true if the trigger exists, false otherwise
     */
    public boolean calendarExists(String calendarName) {
        return exists(keyOfCalendar(calendarName));
    }

    /**
     * <p>
     * Select a calendar.
     * </p>
     *
     * @param calendarName
     *          the name of the calendar
     * @return the Calendar
     * @throws ClassNotFoundException
     *           if a class found during deserialization cannot be found be
     *           found
     * @throws IOException
     *           if there were problems deserializing the calendar
     */
    public Calendar selectCalendar(String calendarName)
            throws ClassNotFoundException, IOException {
        String calendarInfo = get(keyOfCalendar(calendarName));
        return Helper.getObjectMapper().readValue(calendarInfo, BaseCalendar.class);
        /*PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String selCal = rtp(SELECT_CALENDAR);
            ps = conn.prepareStatement(selCal);
            ps.setString(1, calendarName);
            rs = ps.executeQuery();

            Calendar cal = null;
            if (rs.next()) {
                cal = (Calendar) getObjectFromBlob(rs, COL_CALENDAR);
            }
            if (null == cal) {
                logger.warn("Couldn't find calendar with name '" + calendarName
                        + "'.");
            }
            return cal;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }*/
    }

    /**
     * <p>
     * Check whether or not a calendar is referenced by any triggers.
     * </p>
     *
     * @param calendarName
     *          the name of the calendar
     * @return true if any triggers reference the calendar, false otherwise
     */
    public boolean calendarIsReferenced(String calendarName) {
        return exists(keyOfCalendarTriggers(calendarName));
    }

    /**
     * <p>
     * Delete a calendar.
     * </p>
     *
     * @param calendarName
     *          the name of the trigger
     * @return the number of rows deleted
     */
    public long deleteCalendar(String calendarName) {
        return del(calendarName);
    }

    /**
     * <p>
     * Select the total number of calendars stored.
     * </p>
     *
     * @return the total number of calendars stored
     */
    public int selectNumCalendars() {
        return scard(keyOfCalendars());
    }

    /**
     * <p>
     * Select all of the stored calendars.
     * </p>
     *
     * @return an array of <code>String</code> calendar names
     */
    public List<String> selectCalendars() {
        return new LinkedList<>(smembers(keyOfCalendars()));
    }

    //---------------------------------------------------------------------------
    // trigger firing
    //---------------------------------------------------------------------------

    /**
     * <p>
     * Select the next trigger which will fire to fire between the two given timestamps
     * in ascending order of fire time, and then descending by priority.
     * </p>
     *
     * @param noLaterThan
     *          highest value of <code>getNextFireTime()</code> of the triggers (exclusive)
     * @param noEarlierThan
     *          highest value of <code>getNextFireTime()</code> of the triggers (inclusive)
     * @param maxCount
     *          maximum number of trigger keys allow to acquired in the returning list.
     *
     * @return A (never null, possibly empty) list of the identifiers (Key objects) of the next triggers to be fired.
     */
    public List<TriggerKey> selectTriggerToAcquire(long noLaterThan, long noEarlierThan, int maxCount) {

        List<TriggerKey> nextTriggers = new LinkedList<TriggerKey>();

        // Set max rows to retrieve
        if (maxCount < 1)
            maxCount = 1; // we want at least one trigger back.
        List<String> groupWithTriggerNames = zrangebyscore(keyOfWaitTriggers(), noLaterThan);
        for (int i = 0; i < groupWithTriggerNames.size(); i++) {
            if (i < maxCount) {
                String[] array = splitValue(groupWithTriggerNames.get(i));
                TriggerKey triggerKey = new TriggerKey(array[0], array[1]);
                Map<String, String> triggerMap = hgetall(keyOfTrigger(triggerKey));
                String misInstStr = triggerMap.get(COL_MISFIRE_INSTRUCTION);
                String nextFireTimeStr = triggerMap.get(COL_NEXT_FIRE_TIME);
                if ("-1".equals(misInstStr) || (nextFireTimeStr != null && Long.parseLong(nextFireTimeStr) >= noEarlierThan)) {
                    nextTriggers.add(triggerKey);
                }
            }
        }
        return nextTriggers;
    }

    /**
     * <p>
     * Insert a fired trigger.
     * </p>
     *
     * @param trigger
     *          the trigger
     * @param state
     *          the state that the trigger should be stored in
     */
    public void insertFiredTrigger(OperableTrigger trigger,
                                  String state, JobDetail job) {
        updateFiredTrigger(trigger, state, job);
        sadd(keyOfFiredJobs(), trigger.getFireInstanceId());
    }

    /**
     * <p>
     * Update a fired trigger.
     * </p>
     *
     * @param trigger
     *          the trigger
     * @param state
     *          the state that the trigger should be stored in
     */
    public void updateFiredTrigger(OperableTrigger trigger,
                                  String state, JobDetail job) {
        Map<String, String> map = new HashMap<>();
        //map.put(COL_ENTRY_ID, trigger.getFireInstanceId());
        map.put(COL_TRIGGER_NAME, trigger.getKey().getName());
        map.put(COL_TRIGGER_GROUP, trigger.getKey().getGroup());
        map.put(COL_INSTANCE_NAME, instanceId);
        map.put(COL_FIRED_TIME, String.valueOf(System.currentTimeMillis()));
        map.put(COL_SCHED_TIME, String.valueOf(trigger.getNextFireTime().getTime()));
        map.put(COL_ENTRY_STATE, state);
        if (job != null) {
            map.put(COL_JOB_NAME, trigger.getJobKey().getName());
            map.put(COL_JOB_GROUP, trigger.getJobKey().getGroup());
            map.put(COL_IS_NONCONCURRENT, String.valueOf(job.isConcurrentExectionDisallowed()));
            map.put(COL_REQUESTS_RECOVERY, String.valueOf(job.requestsRecovery()));
        } else {
            map.put(COL_JOB_NAME, null);
            map.put(COL_JOB_GROUP, null);
            map.put(COL_IS_NONCONCURRENT, String.valueOf(false));
            map.put(COL_REQUESTS_RECOVERY, String.valueOf(false));
        }
        map.put(COL_PRIORITY, String.valueOf(trigger.getPriority()));
        hmset(keyOfFiredJob(trigger.getFireInstanceId()), map);
    }

    /**
     * <p>
     * Select the states of all fired-trigger records for a given trigger, or
     * trigger group if trigger name is <code>null</code>.
     * </p>
     *
     * @return a List of FiredTriggerRecord objects.
     */
    /** TODO ldang264 doCheckin
    public List<FiredTriggerRecord> selectFiredTriggerRecords(String triggerName, String groupName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            List<FiredTriggerRecord> lst = new LinkedList<FiredTriggerRecord>();

            if (triggerName != null) {
                ps = conn.prepareStatement(rtp(SELECT_FIRED_TRIGGER));
                ps.setString(1, triggerName);
                ps.setString(2, groupName);
            } else {
                ps = conn.prepareStatement(rtp(SELECT_FIRED_TRIGGER_GROUP));
                ps.setString(1, groupName);
            }
            rs = ps.executeQuery();

            while (rs.next()) {
                FiredTriggerRecord rec = new FiredTriggerRecord();

                rec.setFireInstanceId(rs.getString(COL_ENTRY_ID));
                rec.setFireInstanceState(rs.getString(COL_ENTRY_STATE));
                rec.setFireTimestamp(rs.getLong(COL_FIRED_TIME));
                rec.setScheduleTimestamp(rs.getLong(COL_SCHED_TIME));
                rec.setPriority(rs.getInt(COL_PRIORITY));
                rec.setSchedulerInstanceId(rs.getString(COL_INSTANCE_NAME));
                rec.setTriggerKey(triggerKey(rs.getString(COL_TRIGGER_NAME), rs
                        .getString(COL_TRIGGER_GROUP)));
                if (!rec.getFireInstanceState().equals(STATE_ACQUIRED)) {
                    rec.setJobDisallowsConcurrentExecution(getBoolean(rs, COL_IS_NONCONCURRENT));
                    rec.setJobRequestsRecovery(rs
                            .getBoolean(COL_REQUESTS_RECOVERY));
                    rec.setJobKey(jobKey(rs.getString(COL_JOB_NAME), rs
                            .getString(COL_JOB_GROUP)));
                }
                lst.add(rec);
            }

            return lst;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }
     */

    public List<FiredTriggerRecord> selectInstancesFiredTriggerRecords() {

        List<FiredTriggerRecord> lst = new LinkedList<FiredTriggerRecord>();

        Set<String> entryIds = smembers(keyOfFiredJobs());
        for (String entryId : entryIds) {
            Map<String, String> hgetall = hgetall(keyOfFiredJob(entryId));
            FiredTriggerRecord record = Helper.getObjectMapper().convertValue(hgetall, FiredTriggerRecord.class);
            lst.add(record);
        }

        return lst;
    }

    /**
     * <p>
     * Select the distinct instance names of all fired-trigger records.
     * </p>
     *
     * <p>
     * This is useful when trying to identify orphaned fired triggers (a
     * fired trigger without a scheduler state record.)
     * </p>
     *
     * @return a Set of String objects.
     */
    /** TODO ldang264 doCheckin
    public Set<String> selectFiredTriggerInstanceNames(Connection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            Set<String> instanceNames = new HashSet<String>();

            ps = conn.prepareStatement(rtp(SELECT_FIRED_TRIGGER_INSTANCE_NAMES));
            rs = ps.executeQuery();

            while (rs.next()) {
                instanceNames.add(rs.getString(COL_INSTANCE_NAME));
            }

            return instanceNames;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }
    }
    */

    /**
     * <p>
     * Delete a fired trigger.
     * </p>
     *
     * @param entryId
     *          the fired trigger entry to delete
     * @return the number of rows deleted
     */
    public void deleteFiredTrigger(String entryId) {
        del(keyOfFiredJob(entryId));
        srem(keyOfFiredJobs(), entryId);
    }

    /* TODO ldang264 doCheckin
    public int insertSchedulerState(String theInstanceId,
                                    long checkInTime, long interval) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(rtp(INSERT_SCHEDULER_STATE));
            ps.setString(1, theInstanceId);
            ps.setLong(2, checkInTime);
            ps.setLong(3, interval);

            return ps.executeUpdate();
        } finally {
            closeStatement(ps);
        }
    }

    public int deleteSchedulerState(String theInstanceId) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(rtp(DELETE_SCHEDULER_STATE));
            ps.setString(1, theInstanceId);

            return ps.executeUpdate();
        } finally {
            closeStatement(ps);
        }
    }

    public int updateSchedulerState(String theInstanceId, long checkInTime) {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(rtp(UPDATE_SCHEDULER_STATE));
            ps.setLong(1, checkInTime);
            ps.setString(2, theInstanceId);

            return ps.executeUpdate();
        } finally {
            closeStatement(ps);
        }
    }

    public List<SchedulerStateRecord> selectSchedulerStateRecords(String theInstanceId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            List<SchedulerStateRecord> lst = new LinkedList<SchedulerStateRecord>();

            if (theInstanceId != null) {
                ps = conn.prepareStatement(rtp(SELECT_SCHEDULER_STATE));
                ps.setString(1, theInstanceId);
            } else {
                ps = conn.prepareStatement(rtp(SELECT_SCHEDULER_STATES));
            }
            rs = ps.executeQuery();

            while (rs.next()) {
                SchedulerStateRecord rec = new SchedulerStateRecord();

                rec.setSchedulerInstanceId(rs.getString(COL_INSTANCE_NAME));
                rec.setCheckinTimestamp(rs.getLong(COL_LAST_CHECKIN_TIME));
                rec.setCheckinInterval(rs.getLong(COL_CHECKIN_INTERVAL));

                lst.add(rec);
            }

            return lst;
        } finally {
            closeResultSet(rs);
            closeStatement(ps);
        }

    }
*/


    //---------------------------------------------------------------------------
    // other
    //---------------------------------------------------------------------------

    public Set<String> selectPausedTriggerGroups() {
        return smembers(keyOfPausedTriggerGroups());
    }

}
