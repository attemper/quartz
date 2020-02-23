package com.github.quartz.impl.redisjobstore;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.impl.DefaultThreadExecutor;
import org.quartz.impl.jdbcjobstore.Constants;
import org.quartz.impl.jdbcjobstore.FiredTriggerRecord;
import org.quartz.impl.jdbcjobstore.NoSuchDelegateException;
import org.quartz.impl.jdbcjobstore.TriggerStatus;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class RedisJobStore implements JobStore, RedisConstants {

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Constants.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    protected static final String LOCK_TRIGGER_ACCESS = "TRIGGER_ACCESS";

    protected static final String LOCK_STATE_ACCESS = "STATE_ACCESS";

    /**
     * Connection URL. Overrides host, port, and password. User is ignored. Example:
     * redis://user:password@example.com:6379
     */
    protected String url;

    protected String clusterNodes;

    protected String sentinelNodes;

    protected String sentinelMaster;

    /**
     * Database index used by the connection factory.
     */
    protected int database = 0;

    /**
     * Redis server host.
     */
    protected String host = "localhost";

    /**
     * Login password of the redis server.
     */
    protected String password;

    /**
     * Redis server port.
     */
    protected int port = 6379;

    /**
     * Whether to enable SSL support.
     */
    protected boolean ssl;

    /**
     * default expire milliseconds
     */
    protected long timeout = 500;

    protected String instanceId;

    protected String instanceName;

    protected String delegateInitString;

    protected HashMap<String, Calendar> calendarCache = new HashMap<String, Calendar>();

    protected StdRedisDelegate delegate;

    private long misfireThreshold = 60000L; // one minute

    protected boolean isClustered = false;

    protected long clusterCheckinInterval = 7500L;

    // private ClusterManager clusterManagementThread = null;

    // private MisfireHandler misfireHandler = null;

    private ClassLoadHelper classLoadHelper;

    private SchedulerSignaler schedSignaler;

    private boolean acquireTriggersWithinLock = true;

    private long retryInterval = 15000L; // 15 secs

    private final Logger log = LoggerFactory.getLogger(getClass());

    private ThreadExecutor threadExecutor = new DefaultThreadExecutor();

    private volatile boolean schedulerRunning = false;

    private volatile boolean shutdown = false;

    /*
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     *
     * Interface.
     *
     * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
     */

    public void setUrl(String url) {
        this.url = url;
    }

    public void setClusterNodes(String clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public void setSentinelNodes(String sentinelNodes) {
        this.sentinelNodes = sentinelNodes;
    }

    public void setSentinelMaster(String sentinelMaster) {
        this.sentinelMaster = sentinelMaster;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * <p>
     * Set the instance Id of the Scheduler (must be unique within a cluster).
     * </p>
     */
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    /**
     * <p>
     * Get the instance Id of the Scheduler (must be unique within a cluster).
     * </p>
     */
    public String getInstanceId() {

        return instanceId;
    }

    /**
     * Set the instance name of the Scheduler (must be unique within this server instance).
     */
    @Override
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    @Override
    public void setThreadPoolSize(int poolSize) {
        // nothing to do
    }

    public void setThreadExecutor(ThreadExecutor threadExecutor) {
        this.threadExecutor = threadExecutor;
    }

    public ThreadExecutor getThreadExecutor() {
        return threadExecutor;
    }

    /**
     * Get the instance name of the Scheduler (must be unique within this server instance).
     */
    public String getInstanceName() {

        return instanceName;
    }

    /**
     * How long (in milliseconds) the <code>JobStore</code> implementation
     * estimates that it will take to release a trigger and acquire a new one.
     */
    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        return 70;
    }

    /**
     * <p>
     * Set whether this instance is part of a cluster.
     * </p>
     */
    public void setIsClustered(boolean isClustered) {
        this.isClustered = isClustered;
    }

    /**
     * <p>
     * Get whether this instance is part of a cluster.
     * </p>
     */
    @Override
    public boolean isClustered() {
        return isClustered;
    }

    /**
     * <p>
     * Get the frequency (in milliseconds) at which this instance "checks-in"
     * with the other instances of the cluster. -- Affects the rate of
     * detecting failed instances.
     * </p>
     */
    public long getClusterCheckinInterval() {
        return clusterCheckinInterval;
    }

    /**
     * <p>
     * Set the frequency (in milliseconds) at which this instance "checks-in"
     * with the other instances of the cluster. -- Affects the rate of
     * detecting failed instances.
     * </p>
     */
    public void setClusterCheckinInterval(long l) {
        clusterCheckinInterval = l;
    }

    /**
     * @return Returns the retryInterval.
     */
    public long getRetryInterval() {
        return retryInterval;
    }
    /**
     * @param retryInterval The retryInterval to set.
     */
    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public long getMisfireThreshold() {
        return misfireThreshold;
    }

    /**
     * The the number of milliseconds by which a trigger must have missed its
     * next-fire-time, in order for it to be considered "misfired" and thus
     * have its misfire instruction applied.
     *
     * @param misfireThreshold the misfire threshold to use, in millis
     */
    public void setMisfireThreshold(long misfireThreshold) {
        if (misfireThreshold < 1) {
            throw new IllegalArgumentException(
                    "Misfirethreshold must be larger than 0");
        }
        this.misfireThreshold = misfireThreshold;
    }

    /**
     * Whether or not the query and update to acquire a Trigger for firing
     * should be performed after obtaining an explicit DB lock (to avoid
     * possible race conditions on the trigger's db row).  This is the
     * behavior prior to Quartz 1.6.3, but is considered unnecessary for most
     * databases (due to the nature of the SQL update that is performed),
     * and therefore a superfluous performance hit.
     */
    public boolean isAcquireTriggersWithinLock() {
        return acquireTriggersWithinLock;
    }

    /**
     * Whether or not the query and update to acquire a Trigger for firing
     * should be performed after obtaining an explicit DB lock.  This is the
     * behavior prior to Quartz 1.6.3, but is considered unnecessary for most
     * databases, and therefore a superfluous performance hit.
     *
     * However, if batch acquisition is used, it is important for this behavior
     * to be used for all dbs.
     */
    public void setAcquireTriggersWithinLock(boolean acquireTriggersWithinLock) {
        this.acquireTriggersWithinLock = acquireTriggersWithinLock;
    }

    /**
     * <p>
     * Set the JDBC driver delegate's initialization string.
     * </p>
     *
     * @param delegateInitString
     *          the delegate init string
     */
    public void setDriverDelegateInitString(String delegateInitString) {
        this.delegateInitString = delegateInitString;
    }

    /**
     * <p>
     * Get the JDBC driver delegate's initialization string.
     * </p>
     *
     * @return the delegate init string
     */
    public String getDriverDelegateInitString() {
        return delegateInitString;
    }

    protected ClassLoadHelper getClassLoadHelper() {
        return classLoadHelper;
    }
    @Override
    public long getAcquireRetryDelay(int failureCount) {
        return 7500L;
    }

    protected Logger getLog() {
        return log;
    }

    /**
     * Called by the QuartzScheduler before the <code>JobStore</code> is
     * used, in order to give the it a chance to initialize.
     *
     * @param loadHelper class loader helper
     * @param signaler schedule signaler object
     */
    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        this.classLoadHelper = loadHelper;
        this.schedSignaler = signaler;

        delegate = new StdRedisDelegate();
        try {
            delegate.initialize(getLog(), instanceName, instanceId, loadHelper, getDriverDelegateInitString());
        } catch (NoSuchDelegateException e) {
            throw new RuntimeException(e);
        }

        if (clusterNodes != null && clusterNodes.trim().length() > 0) {
            String[] uris = clusterNodes.split(",");
            List<RedisURI> redisURIs = new ArrayList<>(uris.length);
            for (String uri : uris) {
                RedisURI redisURI = RedisURI.create(uri);
                redisURI.setSsl(ssl);
                redisURI.setDatabase(database);
                if (password != null && !"".equals(password.trim())) {
                    redisURI.setPassword(password);
                }
                redisURIs.add(redisURI);
            }
            RedisClusterClient redisClient = RedisClusterClient.create(redisURIs);
            //this.handler = new ClusterHandler(redisClusterClient);
            StatefulRedisClusterConnection<String, String> connection = redisClient.connect();
            RedisAdvancedClusterCommands<String, String> sync = connection.sync();
            delegate.setRedisClient(redisClient)
                    .setStatefulConnection(connection)
                    .setRedisKeyCommands(sync)
                    .setRedisStringCommands(sync)
                    .setRedisScriptingCommands(sync)
                    .setRedisHashCommands(sync)
                    .setRedisSetCommands(sync)
                    .setRedisSortedSetCommands(sync)
                    .setRedisServerCommands(sync);
        } else if (sentinelNodes != null && sentinelNodes.trim().length() > 0) {
            String[] uris = sentinelNodes.split(",");
            RedisURI.Builder builder = RedisURI.builder()
                    .withSsl(ssl)
                    .withDatabase(database)
                    .withSentinelMasterId(sentinelMaster);
            if (password != null && !"".equals(password.trim())) {
                builder.withPassword(password);
            }
            String[] sentinel1 = uris[0].split(":");
            if (sentinel1.length > 1) {
                builder.withSentinel(sentinel1[0], Integer.parseInt(sentinel1[1]));
            } else {
                builder.withSentinel(sentinel1[0]);
            }
            if (uris.length > 1) {
                for (int i = 1; i < uris.length; i++) {
                    String[] sentinel2toN = uris[i].split(":");
                    if (sentinel2toN.length > 1) {
                        builder.withSentinel(sentinel2toN[0], Integer.parseInt(sentinel2toN[1]));
                    } else {
                        builder.withSentinel(sentinel2toN[0]);
                    }
                }
            }
            RedisClient redisClient = RedisClient.create(builder.build());
            //this.handler = new NonClusterHandler(redisClient);
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            RedisCommands<String, String> sync = connection.sync();
            delegate.setRedisClient(redisClient)
                    .setStatefulConnection(connection)
                    .setRedisKeyCommands(sync)
                    .setRedisStringCommands(sync)
                    .setRedisScriptingCommands(sync)
                    .setRedisHashCommands(sync)
                    .setRedisSetCommands(sync)
                    .setRedisSortedSetCommands(sync)
                    .setRedisServerCommands(sync);
        } else if (url != null && url.trim().length() > 0) {
            RedisClient redisClient = RedisClient.create(url);
            //this.handler = new NonClusterHandler(redisClient);
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            RedisCommands<String, String> sync = connection.sync();
            delegate.setRedisClient(redisClient)
                    .setStatefulConnection(connection)
                    .setRedisKeyCommands(sync)
                    .setRedisStringCommands(sync)
                    .setRedisScriptingCommands(sync)
                    .setRedisHashCommands(sync)
                    .setRedisSetCommands(sync)
                    .setRedisSortedSetCommands(sync)
                    .setRedisServerCommands(sync);
        } else {
            RedisURI redisURI = RedisURI.builder().redis(host, port).withSsl(ssl).withDatabase(database).build();
            if (password != null && !"".equals(password.trim())) {
                redisURI.setPassword(password);
            }
            RedisClient redisClient = RedisClient.create(redisURI);
            //this.handler = new NonClusterHandler(redisClient);
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            RedisCommands<String, String> sync = connection.sync();
            delegate.setRedisClient(redisClient)
                    .setStatefulConnection(connection)
                    .setRedisKeyCommands(sync)
                    .setRedisStringCommands(sync)
                    .setRedisScriptingCommands(sync)
                    .setRedisHashCommands(sync)
                    .setRedisSetCommands(sync)
                    .setRedisSortedSetCommands(sync)
                    .setRedisServerCommands(sync);
        }

        Helper.getObjectMapper().setTypeFactory(
                Helper.getObjectMapper().getTypeFactory().withClassLoader(loadHelper.getClassLoader()));
    }

    /**
     * Called by the QuartzScheduler to inform the <code>JobStore</code> that
     * the scheduler has started.
     */
    @Override
    public void schedulerStarted() throws SchedulerException {
        // TODO ldang264
        schedulerRunning = true;
    }

    /**
     * Called by the QuartzScheduler to inform the <code>JobStore</code> that
     * the scheduler has been paused.
     */
    @Override
    public void schedulerPaused() {
        schedulerRunning = false;
    }

    /**
     * Called by the QuartzScheduler to inform the <code>JobStore</code> that
     * the scheduler has resumed after being paused.
     */
    @Override
    public void schedulerResumed() {
        schedulerRunning = true;
    }

    /**
     * Called by the QuartzScheduler to inform the <code>JobStore</code> that
     * it should free up all of it's resources because the scheduler is
     * shutting down.
     */
    @Override
    public void shutdown() {
        getDelegate().shutdown();
    }

    @Override
    public boolean supportsPersistence() {
        return true;
    }

    //---------------------------------------------------------------------------
    // helper methods for subclasses
    //---------------------------------------------------------------------------

    protected long getMisfireTime() {
        long misfireTime = System.currentTimeMillis();
        if (getMisfireThreshold() > 0) {
            misfireTime -= getMisfireThreshold();
        }

        return (misfireTime > 0) ? misfireTime : 0;
    }

    protected boolean updateMisfiredTrigger(TriggerKey triggerKey, String newStateIfNotComplete, boolean forceState)
            throws JobPersistenceException {
        try {

            OperableTrigger trig = retrieveTrigger(triggerKey);

            long misfireTime = System.currentTimeMillis();
            if (getMisfireThreshold() > 0) {
                misfireTime -= getMisfireThreshold();
            }

            if (trig.getNextFireTime().getTime() > misfireTime) {
                return false;
            }

            doUpdateOfMisfiredTrigger(trig, forceState, newStateIfNotComplete, false);

            return true;

        } catch (Exception e) {
            throw new JobPersistenceException(
                    "Couldn't update misfired trigger '" + triggerKey + "': " + e.getMessage(), e);
        }
    }

    private void doUpdateOfMisfiredTrigger(OperableTrigger trig, boolean forceState, String newStateIfNotComplete, boolean recovering) throws JobPersistenceException {
        Calendar cal = null;
        if (trig.getCalendarName() != null) {
            cal = retrieveCalendar(trig.getCalendarName());
        }

        schedSignaler.notifyTriggerListenersMisfired(trig);

        trig.updateAfterMisfire(cal);

        if (trig.getNextFireTime() == null) {
            storeTrigger(trig,
                    null, true, STATE_COMPLETE, forceState, recovering);
            schedSignaler.notifySchedulerListenersFinalized(trig);
        } else {
            storeTrigger(trig, null, true, newStateIfNotComplete,
                    forceState, recovering);
        }
    }

    /**
     * Store the given <code>{@link org.quartz.JobDetail}</code> and <code>{@link org.quartz.Trigger}</code>.
     *
     * @param newJob     The <code>JobDetail</code> to be stored.
     * @param newTrigger The <code>Trigger</code> to be stored.
     * @throws org.quartz.ObjectAlreadyExistsException if a <code>Job</code> with the same name/group already
     *                                                 exists.
     */
    @Override
    public void storeJobAndTrigger(final JobDetail newJob, final OperableTrigger newTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
        executeInLock(LOCK_TRIGGER_ACCESS, new VoidCallback() {
            @Override
            void executeVoid() throws JobPersistenceException {
                storeJobIntern(newJob, false);
                storeTrigger(newTrigger, newJob, false,
                        Constants.STATE_WAITING, false, false);
            }
        });
    }

    /**
     * <p>
     * Store the given <code>{@link org.quartz.JobDetail}</code>.
     * </p>
     *
     * @param newJob
     *          The <code>JobDetail</code> to be stored.
     * @param replaceExisting
     *          If <code>true</code>, any <code>Job</code> existing in the
     *          <code>JobStore</code> with the same name & group should be
     *          over-written.
     * @throws ObjectAlreadyExistsException
     *           if a <code>Job</code> with the same name/group already
     *           exists, and replaceExisting is set to false.
     */
    @Override
    public void storeJob(final JobDetail newJob,
                         final boolean replaceExisting) throws JobPersistenceException {
        executeInLock(
                replaceExisting ? LOCK_TRIGGER_ACCESS : null,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        storeJobIntern(newJob, replaceExisting);
                    }
                });
    }

    protected void storeJobIntern(JobDetail newJob, boolean replaceExisting) throws JobPersistenceException {
        boolean existingJob = jobExists(newJob.getKey());
        if (existingJob && !replaceExisting) {
            throw new ObjectAlreadyExistsException(newJob);
        }
        if (existingJob) {
            if (!replaceExisting) {
                throw new ObjectAlreadyExistsException(newJob);
            }
            getDelegate().updateJobDetail(newJob);
        } else {
            getDelegate().insertJobDetail(newJob);
        }
    }

    protected boolean jobExists(JobKey jobKey) {
        return getDelegate().jobExists(jobKey);
    }

    /**
     * <p>
     * Store the given <code>{@link org.quartz.Trigger}</code>.
     * </p>
     *
     * @param newTrigger
     *          The <code>Trigger</code> to be stored.
     * @param replaceExisting
     *          If <code>true</code>, any <code>Trigger</code> existing in
     *          the <code>JobStore</code> with the same name & group should
     *          be over-written.
     * @throws ObjectAlreadyExistsException
     *           if a <code>Trigger</code> with the same name/group already
     *           exists, and replaceExisting is set to false.
     */
    @Override
    public void storeTrigger(final OperableTrigger newTrigger,
                             final boolean replaceExisting) throws JobPersistenceException {
        executeInLock(
                replaceExisting ? LOCK_TRIGGER_ACCESS : null,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        storeTrigger(newTrigger, null, replaceExisting,
                                STATE_WAITING, false, false);
                    }
                });
    }

    protected void storeTrigger(OperableTrigger newTrigger, JobDetail job, boolean replaceExisting, String state, boolean forceState, boolean recovering) throws JobPersistenceException {
        boolean existingTrigger = triggerExists(newTrigger.getKey());
        if (existingTrigger && !replaceExisting) {
            throw new ObjectAlreadyExistsException(newTrigger);
        }
        boolean shouldBepaused;
        if (!forceState) {
            shouldBepaused = getDelegate().isTriggerGroupPaused(
                    newTrigger.getKey().getGroup());

            if(!shouldBepaused) {
                shouldBepaused = getDelegate().isTriggerGroupPaused(
                        ALL_GROUPS_PAUSED);
                if (shouldBepaused) {
                    getDelegate().insertPausedTriggerGroup(newTrigger.getKey().getGroup());
                }
            }

            if (shouldBepaused && (state.equals(STATE_WAITING) || state.equals(STATE_ACQUIRED))) {
                state = STATE_PAUSED;
            }
        }

        if(job == null) {
            job = retrieveJob(newTrigger.getJobKey());
        }
        if (job == null) {
            throw new JobPersistenceException("The job ("
                    + newTrigger.getJobKey()
                    + ") referenced by the trigger does not exist.");
        }

        if (job.isConcurrentExectionDisallowed() && !recovering) {
            state = checkBlockedState(job.getKey(), state);
        }
        if (existingTrigger) {
            getDelegate().updateTrigger(newTrigger, state, job);
        } else {
            getDelegate().insertTrigger(newTrigger, state, job);
        }
    }

    protected boolean triggerExists(TriggerKey triggerKey) {
        return getDelegate().triggerExists(triggerKey);
    }

    /**
     * <p>
     * Remove (delete) the <code>{@link org.quartz.Job}</code> with the given
     * name, and any <code>{@link org.quartz.Trigger}</code> s that reference
     * it.
     * </p>
     *
     * <p>
     * If removal of the <code>Job</code> results in an empty group, the
     * group should be removed from the <code>JobStore</code>'s list of
     * known group names.
     * </p>
     *
     * @return <code>true</code> if a <code>Job</code> with the given name &
     *         group was found and removed from the store.
     */
    @Override
    public boolean removeJob(final JobKey jobKey) throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Boolean>() {
                    public Boolean execute() throws JobPersistenceException {
                        return removeJobIntern(jobKey);
                    }
                });
    }

    protected boolean removeJobIntern(final JobKey jobKey)
            throws JobPersistenceException {
        List<TriggerKey> jobTriggers = getDelegate().selectTriggerKeysForJob(jobKey);
        for (TriggerKey jobTrigger: jobTriggers) {
            deleteTriggerAndChildren(jobTrigger);
        }

        return deleteJobAndChildren(jobKey);
    }

    @Override
    public boolean removeJobs(final List<JobKey> jobKeys) throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Boolean>() {
                    public Boolean execute() throws JobPersistenceException {
                        boolean allFound = true;

                        // FUTURE_TODO: make this more efficient with a true bulk operation...
                        for (JobKey jobKey : jobKeys)
                            allFound = removeJobIntern(jobKey) && allFound;

                        return allFound;
                    }
                });
    }

    @Override
    public boolean removeTriggers(final List<TriggerKey> triggerKeys) throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Boolean>() {
                    public Boolean execute() throws JobPersistenceException {
                        boolean allFound = true;

                        // FUTURE_TODO: make this more efficient with a true bulk operation...
                        for (TriggerKey triggerKey : triggerKeys)
                            allFound = removeTriggerIntern(triggerKey) && allFound;

                        return allFound;
                    }
                });
    }

    @Override
    public void storeJobsAndTriggers(
            final Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, final boolean replace)
            throws JobPersistenceException {

        executeInLock(
                replace ? LOCK_TRIGGER_ACCESS : null,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {

                        // FUTURE_TODO: make this more efficient with a true bulk operation...
                        for(JobDetail job: triggersAndJobs.keySet()) {
                            storeJobIntern(job, replace);
                            for(Trigger trigger: triggersAndJobs.get(job)) {
                                storeTrigger((OperableTrigger) trigger, job, replace,
                                        Constants.STATE_WAITING, false, false);
                            }
                        }
                    }
                });
    }

    /**
     * Delete a job and its listeners.
     *
     * @see #removeJobIntern(org.quartz.JobKey)
     * @see #removeTrigger(TriggerKey)
     */
    private boolean deleteJobAndChildren(JobKey key) {

        return (getDelegate().deleteJobDetail(key) > 0);
    }

    /**
     * Delete a trigger, its listeners, and its Simple/Cron/BLOB sub-table entry.
     *
     * @see #removeJobIntern(org.quartz.JobKey)
     * @see #removeTrigger(TriggerKey)
     * @see #replaceTrigger(TriggerKey, OperableTrigger)
     */
    private boolean deleteTriggerAndChildren(TriggerKey key) {

        return (getDelegate().deleteTrigger(key) > 0);
    }

    @Override
    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return getDelegate().selectJobDetail(jobKey/*,
                getClassLoadHelper()*/);
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the
     * given key.
     * <p/>
     * <p>
     * If removal of the <code>Trigger</code> results in an empty group, the
     * group should be removed from the <code>JobStore</code>'s list of
     * known group names.
     * </p>
     * <p/>
     * <p>
     * If removal of the <code>Trigger</code> results in an 'orphaned' <code>Job</code>
     * that is not 'durable', then the <code>Job</code> should be deleted
     * also.
     * </p>
     *
     * @param triggerKey the key of the trigger to be removed
     * @return <code>true</code> if a <code>Trigger</code> with the given
     * name & group was found and removed from the store.
     */
    @Override
    public boolean removeTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Boolean>() {
                    public Boolean execute() throws JobPersistenceException {
                        return removeTriggerIntern(triggerKey);
                    }
                });
    }

    protected boolean removeTriggerIntern(TriggerKey key)
            throws JobPersistenceException {
        boolean removedTrigger;
        try {
            // this must be called before we delete the trigger, obviously
            JobDetail job = getDelegate().selectJobForTrigger(
                    key, false);

            removedTrigger =
                    deleteTriggerAndChildren(key);

            if (null != job && !job.isDurable()) {
                int numTriggers = getDelegate().selectNumTriggersForJob(
                        job.getKey());
                if (numTriggers == 0) {
                    // Don't call removeJob() because we don't want to check for
                    // triggers again.
                    deleteJobAndChildren(job.getKey());
                }
            }
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException("Couldn't remove trigger: "
                    + e.getMessage(), e);
        }

        return removedTrigger;
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the
     * given key, and store the new given one - which must be associated
     * with the same job.
     *
     * @param triggerKey the key of the trigger to be replaced
     * @param newTrigger The new <code>Trigger</code> to be stored.
     * @return <code>true</code> if a <code>Trigger</code> with the given
     * name & group was found and removed from the store.
     */
    @Override
    public boolean replaceTrigger(final TriggerKey triggerKey, final OperableTrigger newTrigger) throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Boolean>() {
                    public Boolean execute() throws JobPersistenceException {
                        try {
                            // this must be called before we delete the trigger, obviously
                            JobDetail job = getDelegate().selectJobForTrigger(
                                    triggerKey);

                            if (job == null) {
                                return false;
                            }

                            if (!newTrigger.getJobKey().equals(job.getKey())) {
                                throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
                            }

                            boolean removedTrigger =
                                    deleteTriggerAndChildren(triggerKey);

                            storeTrigger(newTrigger, job, false, STATE_WAITING, false, false);

                            return removedTrigger;
                        } catch (ClassNotFoundException e) {
                            throw new JobPersistenceException("Couldn't remove trigger: "
                                    + e.getMessage(), e);
                        }
                    }
                });
    }

    /**
     * Retrieve the given <code>{@link org.quartz.Trigger}</code>.
     *
     * @param triggerKey the key of the desired trigger
     * @return The desired <code>Trigger</code>, or null if there is no
     * match.
     */
    @Override
    public OperableTrigger retrieveTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
        return getDelegate().selectTrigger(triggerKey);
    }

    /**
     * Get the current state of the identified <code>{@link org.quartz.Trigger}</code>.
     *
     * @param triggerKey the key of the trigger for which to retrieve state
     * @see org.quartz.Trigger.TriggerState
     */
    @Override
    public Trigger.TriggerState getTriggerState(final TriggerKey triggerKey) throws JobPersistenceException {
        String ts = getDelegate().selectTriggerState(triggerKey);

        if (ts == null) {
            return Trigger.TriggerState.NONE;
        }

        if (ts.equals(STATE_DELETED)) {
            return Trigger.TriggerState.NONE;
        }

        if (ts.equals(STATE_COMPLETE)) {
            return Trigger.TriggerState.COMPLETE;
        }

        if (ts.equals(STATE_PAUSED)) {
            return Trigger.TriggerState.PAUSED;
        }

        if (ts.equals(STATE_PAUSED_BLOCKED)) {
            return Trigger.TriggerState.PAUSED;
        }

        if (ts.equals(STATE_ERROR)) {
            return Trigger.TriggerState.ERROR;
        }

        if (ts.equals(STATE_BLOCKED)) {
            return Trigger.TriggerState.BLOCKED;
        }

        return Trigger.TriggerState.NORMAL;
    }

    /**
     * Reset the current state of the identified <code>{@link Trigger}</code>
     * from {@link Trigger.TriggerState#ERROR} to {@link Trigger.TriggerState#NORMAL} or
     * {@link Trigger.TriggerState#PAUSED} as appropriate.
     *
     * <p>Only affects triggers that are in ERROR state - if identified trigger is not
     * in that state then the result is a no-op.</p>
     *
     * <p>The result will be the trigger returning to the normal, waiting to
     * be fired state, unless the trigger's group has been paused, in which
     * case it will go into the PAUSED state.</p>
     */
    @Override
    public void resetTriggerFromErrorState(TriggerKey triggerKey) throws JobPersistenceException {
        executeInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        String newState = STATE_WAITING;

                        if(getDelegate().isTriggerGroupPaused(triggerKey.getGroup())) {
                            newState = STATE_PAUSED;
                        }

                        getDelegate().updateTriggerStateFromOtherState(triggerKey, newState, STATE_ERROR);

                        getLog().info("Trigger " + triggerKey + " reset from ERROR state to: " + newState);
                    }
                });
    }

    /**
     * <p>
     * Store the given <code>{@link org.quartz.Calendar}</code>.
     * </p>
     *
     * @param calName
     *          The name of the calendar.
     * @param calendar
     *          The <code>Calendar</code> to be stored.
     * @param replaceExisting
     *          If <code>true</code>, any <code>Calendar</code> existing
     *          in the <code>JobStore</code> with the same name & group
     *          should be over-written.
     * @throws ObjectAlreadyExistsException
     *           if a <code>Calendar</code> with the same name already
     *           exists, and replaceExisting is set to false.
     */
    @Override
    public void storeCalendar(final String calName,
                              final Calendar calendar, final boolean replaceExisting, final boolean updateTriggers)
            throws JobPersistenceException {
        executeInLock(
                updateTriggers ? LOCK_TRIGGER_ACCESS : null,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        try {
                            boolean existingCal = calendarExists(calName);
                            if (existingCal && !replaceExisting) {
                                throw new ObjectAlreadyExistsException(
                                        "Calendar with name '" + calName + "' already exists.");
                            }

                            if (existingCal) {
                                getDelegate().updateCalendar(calName, calendar);

                                if(updateTriggers) {
                                    List<OperableTrigger> trigs = getDelegate().selectTriggersForCalendar(calName);

                                    for(OperableTrigger trigger: trigs) {
                                        trigger.updateWithNewCalendar(calendar, getMisfireThreshold());
                                        storeTrigger(trigger, null, true, STATE_WAITING, false, false);
                                    }
                                }
                            } else {
                                getDelegate().insertCalendar(calName, calendar);
                            }

                            if (!isClustered) {
                                calendarCache.put(calName, calendar); // lazy-cache
                            }

                        } catch (IOException e) {
                            throw new JobPersistenceException(
                                    "Couldn't store calendar because the BLOB couldn't be serialized: "
                                            + e.getMessage(), e);
                        }
                    }
                });
    }

    protected boolean calendarExists(String calName) {
        return getDelegate().calendarExists(calName);
    }

    /**
     * <p>
     * Remove (delete) the <code>{@link org.quartz.Calendar}</code> with the
     * given name.
     * </p>
     *
     * <p>
     * If removal of the <code>Calendar</code> would result in
     * <code>Trigger</code>s pointing to non-existent calendars, then a
     * <code>JobPersistenceException</code> will be thrown.</p>
     *       *
     * @param calName The name of the <code>Calendar</code> to be removed.
     * @return <code>true</code> if a <code>Calendar</code> with the given name
     * was found and removed from the store.
     */
    public boolean removeCalendar(final String calName)
            throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Boolean>() {
                    public Boolean execute() throws JobPersistenceException {
                        if (getDelegate().calendarIsReferenced(calName)) {
                            throw new JobPersistenceException(
                                    "Calender cannot be removed if it referenced by a trigger!");
                        }

                        if (!isClustered) {
                            calendarCache.remove(calName);
                        }

                        return (getDelegate().deleteCalendar(calName) > 0);
                    }
                });
    }

    /**
     * <p>
     * Retrieve the given <code>{@link org.quartz.Trigger}</code>.
     * </p>
     *
     * @param calName
     *          The name of the <code>Calendar</code> to be retrieved.
     * @return The desired <code>Calendar</code>, or null if there is no
     *         match.
     */
    public Calendar retrieveCalendar(final String calName)
            throws JobPersistenceException {
        // all calendars are persistent, but we can lazy-cache them during run
        // time as long as we aren't running clustered.
        Calendar cal = (isClustered) ? null : calendarCache.get(calName);
        if (cal != null) {
            return cal;
        }

        try {
            cal = getDelegate().selectCalendar(calName);
            if (!isClustered) {
                calendarCache.put(calName, cal); // lazy-cache...
            }
            return cal;
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException(
                    "Couldn't retrieve calendar because a required class was not found: "
                            + e.getMessage(), e);
        } catch (IOException e) {
            throw new JobPersistenceException(
                    "Couldn't retrieve calendar because the BLOB couldn't be deserialized: "
                            + e.getMessage(), e);
        }
    }

    /**
     * <p>
     * Get the number of <code>{@link org.quartz.Job}</code> s that are
     * stored in the <code>JobStore</code>.
     * </p>
     */
    @Override
    public int getNumberOfJobs()
            throws JobPersistenceException {
        return getDelegate().selectNumJobs();
    }

    /**
     * <p>
     * Get the number of <code>{@link org.quartz.Trigger}</code> s that are
     * stored in the <code>JobsStore</code>.
     * </p>
     */
    @Override
    public int getNumberOfTriggers()
            throws JobPersistenceException {
        return getDelegate().selectNumTriggers();
    }

    /**
     * <p>
     * Get the number of <code>{@link org.quartz.Calendar}</code> s that are
     * stored in the <code>JobsStore</code>.
     * </p>
     */
    @Override
    public int getNumberOfCalendars()
            throws JobPersistenceException {
        return getDelegate().selectNumCalendars();
    }

    /**
     * <p>
     * Get the names of all of the <code>{@link org.quartz.Job}</code> s that
     * matcher the given groupMatcher.
     * </p>
     *
     * <p>
     * If there are no jobs in the given group name, the result should be an empty Set
     * </p>
     */
    @Override
    public Set<JobKey> getJobKeys(final GroupMatcher<JobKey> matcher)
            throws JobPersistenceException {
        return getJobNames(matcher);
    }

    protected Set<JobKey> getJobNames(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        return getDelegate().selectJobsInGroup(matcher);
    }

    /**
     * Determine whether a {@link Job} with the given identifier already
     * exists within the scheduler.
     *
     * @param jobKey the identifier to check for
     * @return true if a Job exists with the given identifier
     * @throws JobPersistenceException
     */
    @Override
    public boolean checkExists(final JobKey jobKey) throws JobPersistenceException {
        return getDelegate().jobExists(jobKey);
    }

    /**
     * Determine whether a {@link Trigger} with the given identifier already
     * exists within the scheduler.
     *
     * @param triggerKey the identifier to check for
     * @return true if a Trigger exists with the given identifier
     * @throws JobPersistenceException
     */
    @Override
    public boolean checkExists(final TriggerKey triggerKey) throws JobPersistenceException {
        return getDelegate().triggerExists(triggerKey);
    }

    /**
     * Clear (delete!) all scheduling data - all {@link Job}s, {@link Trigger}s
     * {@link Calendar}s.
     *
     * @throws JobPersistenceException
     */
    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        executeInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        getDelegate().clearData();
                    }
                });
    }

    /**
     * <p>
     * Get the names of all of the <code>{@link org.quartz.Trigger}</code> s
     * that match the given group Matcher.
     * </p>
     *
     * <p>
     * If there are no triggers in the given group name, the result should be a
     * an empty Set (not <code>null</code>).
     * </p>
     */
    @Override
    public Set<TriggerKey> getTriggerKeys(final GroupMatcher<TriggerKey> matcher)
            throws JobPersistenceException {
        return getDelegate().selectTriggersInGroup(matcher);
    }

    /**
     * <p>
     * Get the names of all of the <code>{@link org.quartz.Job}</code>
     * groups.
     * </p>
     *
     * <p>
     * If there are no known group names, the result should be a zero-length
     * array (not <code>null</code>).
     * </p>
     */
    @Override
    public List<String> getJobGroupNames() throws JobPersistenceException {
        return getDelegate().selectJobGroups();
    }

    /**
     * <p>
     * Get the names of all of the <code>{@link org.quartz.Trigger}</code>
     * groups.
     * </p>
     *
     * <p>
     * If there are no known group names, the result should be a zero-length
     * array (not <code>null</code>).
     * </p>
     */
    @Override
    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        return getDelegate().selectTriggerGroups();
    }

    /**
     * <p>
     * Get the names of all of the <code>{@link org.quartz.Calendar}</code> s
     * in the <code>JobStore</code>.
     * </p>
     *
     * <p>
     * If there are no Calendars in the given group name, the result should be
     * a zero-length array (not <code>null</code>).
     * </p>
     */
    public List<String> getCalendarNames()throws JobPersistenceException {
        return getDelegate().selectCalendars();
    }

    /**
     * <p>
     * Get all of the Triggers that are associated to the given Job.
     * </p>
     *
     * <p>
     * If there are no matches, a zero-length array should be returned.
     * </p>
     */
    @Override
    public List<OperableTrigger> getTriggersForJob(final JobKey jobKey) throws JobPersistenceException {
        return getDelegate().selectTriggersForJob(jobKey);
    }

    /**
     * <p>
     * Get all of the Triggers that are associated to the given Calendar.
     * </p>
     *
     * <p>
     * If there are no matches, a zero-length array should be returned.
     * </p>
     */
    @Override
    public List<OperableTrigger> getTriggersForCalendar(final String calName) throws JobPersistenceException {
        return getDelegate().selectTriggersForCalendar(calName);
    }

    /**
     * <p>
     * Pause the <code>{@link org.quartz.Trigger}</code> with the given name.
     * </p>
     *
     * @see #resumeTrigger(TriggerKey)
     */
    @Override
    public void pauseTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
        executeInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        pauseTriggerIntern(triggerKey);
                    }
                });
    }

    /**
     * <p>
     * Pause the <code>{@link org.quartz.Trigger}</code> with the given name.
     * </p>
     *
     * @see #resumeTriggerIntern(TriggerKey)
     */
    protected void pauseTriggerIntern(TriggerKey triggerKey)
            throws JobPersistenceException {
        String oldState = getDelegate().selectTriggerState(
                triggerKey);

        if (oldState.equals(STATE_WAITING)
                || oldState.equals(STATE_ACQUIRED)) {

            getDelegate().updateTriggerState(triggerKey,
                    STATE_PAUSED);
        } else if (oldState.equals(STATE_BLOCKED)) {
            getDelegate().updateTriggerState(triggerKey,
                    STATE_PAUSED_BLOCKED);
        }
    }

    /**
     * <p>
     * Pause the <code>{@link org.quartz.Job}</code> with the given name - by
     * pausing all of its current <code>Trigger</code>s.
     * </p>
     *
     * @see #resumeJob(JobKey)
     */
    @Override
    public void pauseJob(final JobKey jobKey) throws JobPersistenceException {
        executeInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        List<OperableTrigger> triggers = getTriggersForJob(jobKey);
                        for (OperableTrigger trigger: triggers) {
                            pauseTriggerIntern(trigger.getKey());
                        }
                    }
                });
    }

    /**
     * <p>
     * Pause all of the <code>{@link org.quartz.Job}s</code> matching the given
     * groupMatcher - by pausing all of their <code>Trigger</code>s.
     * </p>
     *
     * @see #resumeJobs(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public Set<String> pauseJobs(final GroupMatcher<JobKey> matcher)
            throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Set<String>>() {
                    public Set<String> execute() throws JobPersistenceException {
                        Set<String> groupNames = new HashSet<String>();
                        Set<JobKey> jobNames = getJobNames(matcher);

                        for (JobKey jobKey : jobNames) {
                            List<OperableTrigger> triggers = getTriggersForJob(jobKey);
                            for (OperableTrigger trigger : triggers) {
                                pauseTriggerIntern(trigger.getKey());
                            }
                            groupNames.add(jobKey.getGroup());
                        }

                        return groupNames;
                    }
                }
        );
    }

    /**
     * Determines if a Trigger for the given job should be blocked.
     * State can only transition to STATE_PAUSED_BLOCKED/BLOCKED from
     * PAUSED/STATE_WAITING respectively.
     *
     * @return STATE_PAUSED_BLOCKED, BLOCKED, or the currentState.
     */
    protected String checkBlockedState(JobKey jobKey, String currentState) throws JobPersistenceException {

        // State can only transition to BLOCKED from PAUSED or WAITING.
        if ((!currentState.equals(STATE_WAITING)) &&
                (!currentState.equals(STATE_PAUSED))) {
            return currentState;
        }
        JobDetail jobDetail = retrieveJob(jobKey);
        if (jobDetail.isConcurrentExectionDisallowed()) { // OLD_TODO: worry about failed/recovering/volatile job  states?
            return (STATE_PAUSED.equals(currentState)) ? STATE_PAUSED_BLOCKED : STATE_BLOCKED;
        }
        return currentState;
    }

    /**
     * <p>
     * Resume (un-pause) the <code>{@link org.quartz.Trigger}</code> with the
     * given name.
     * </p>
     *
     * <p>
     * If the <code>Trigger</code> missed one or more fire-times, then the
     * <code>Trigger</code>'s misfire instruction will be applied.
     * </p>
     *
     * @see #pauseTrigger(TriggerKey)
     */
    @Override
    public void resumeTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
        executeInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        resumeTriggerIntern(triggerKey);
                    }
                });
    }

    /**
     * <p>
     * Resume (un-pause) the <code>{@link org.quartz.Trigger}</code> with the
     * given name.
     * </p>
     *
     * <p>
     * If the <code>Trigger</code> missed one or more fire-times, then the
     * <code>Trigger</code>'s misfire instruction will be applied.
     * </p>
     *
     * @see #pauseTriggerIntern(TriggerKey)
     */
    protected void resumeTriggerIntern(TriggerKey key)
            throws JobPersistenceException {
        TriggerStatus status = getDelegate().selectTriggerStatus(
                key);

        if (status == null || status.getNextFireTime() == null) {
            return;
        }

        boolean blocked = false;
        if(STATE_PAUSED_BLOCKED.equals(status.getStatus())) {
            blocked = true;
        }

        String newState = checkBlockedState(status.getJobKey(), STATE_WAITING);

        boolean misfired = false;

        if (schedulerRunning && status.getNextFireTime().before(new Date())) {
            misfired = updateMisfiredTrigger(key,
                    newState, true);
        }

        if(!misfired) {
            if(blocked) {
                getDelegate().updateTriggerStateFromOtherState(
                        key, newState, STATE_PAUSED_BLOCKED);
            } else {
                getDelegate().updateTriggerStateFromOtherState(
                        key, newState, STATE_PAUSED);
            }
        }
    }

    /**
     * <p>
     * Resume (un-pause) the <code>{@link org.quartz.Job}</code> with the
     * given name.
     * </p>
     *
     * <p>
     * If any of the <code>Job</code>'s<code>Trigger</code> s missed one
     * or more fire-times, then the <code>Trigger</code>'s misfire
     * instruction will be applied.
     * </p>
     *
     * @see #pauseJob(JobKey)
     */
    @Override
    public void resumeJob(final JobKey jobKey) throws JobPersistenceException {
        executeInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        List<OperableTrigger> triggers = getTriggersForJob(jobKey);
                        for (OperableTrigger trigger: triggers) {
                            resumeTriggerIntern(trigger.getKey());
                        }
                    }
                });
    }

    /**
     * <p>
     * Resume (un-pause) all of the <code>{@link org.quartz.Job}s</code> in
     * the given group.
     * </p>
     *
     * <p>
     * If any of the <code>Job</code> s had <code>Trigger</code> s that
     * missed one or more fire-times, then the <code>Trigger</code>'s
     * misfire instruction will be applied.
     * </p>
     *
     * @see #pauseJobs(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public Set<String> resumeJobs(final GroupMatcher<JobKey> matcher)
            throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Set<String>>() {
                    public Set<String> execute() throws JobPersistenceException {
                        Set<JobKey> jobKeys = getJobNames(matcher);
                        Set<String> groupNames = new HashSet<String>();

                        for (JobKey jobKey: jobKeys) {
                            List<OperableTrigger> triggers = getTriggersForJob(jobKey);
                            for (OperableTrigger trigger: triggers) {
                                resumeTriggerIntern(trigger.getKey());
                            }
                            groupNames.add(jobKey.getGroup());
                        }
                        return groupNames;
                    }
                });
    }

    /**
     * <p>
     * Pause all of the <code>{@link org.quartz.Trigger}s</code> matching the
     * given groupMatcher.
     * </p>
     *
     * @see #resumeTriggerGroup(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public Set<String> pauseTriggers(final GroupMatcher<TriggerKey> matcher)
            throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Set<String>>() {
                    public Set<String> execute() throws JobPersistenceException {
                        return pauseTriggerGroup(matcher);
                    }
                });
    }

    /**
     * <p>
     * Pause all of the <code>{@link org.quartz.Trigger}s</code> matching the
     * given groupMatcher.
     * </p>
     *
     * @see #resumeTriggerGroup(org.quartz.impl.matchers.GroupMatcher)
     */
    protected Set<String> pauseTriggerGroup(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {

        getDelegate().updateTriggerGroupStateFromOtherStates(
                matcher, STATE_PAUSED, STATE_ACQUIRED,
                STATE_WAITING, STATE_WAITING);

        getDelegate().updateTriggerGroupStateFromOtherState(
                matcher, STATE_PAUSED_BLOCKED, STATE_BLOCKED);

        List<String> groups = getDelegate().selectTriggerGroups(matcher);

        // make sure to account for an exact group match for a group that doesn't yet exist
        StringMatcher.StringOperatorName operator = matcher.getCompareWithOperator();
        if (operator.equals(StringMatcher.StringOperatorName.EQUALS) && !groups.contains(matcher.getCompareToValue())) {
            groups.add(matcher.getCompareToValue());
        }

        for (String group : groups) {
            if (!getDelegate().isTriggerGroupPaused(group)) {
                getDelegate().insertPausedTriggerGroup(group);
            }
        }

        return new HashSet<String>(groups);
    }

    @Override
    public Set<String> getPausedTriggerGroups()
            throws JobPersistenceException {
        return getDelegate().selectPausedTriggerGroups();
    }

    /**
     * <p>
     * Resume (un-pause) all of the <code>{@link org.quartz.Trigger}s</code>
     * matching the given groupMatcher.
     * </p>
     *
     * <p>
     * If any <code>Trigger</code> missed one or more fire-times, then the
     * <code>Trigger</code>'s misfire instruction will be applied.
     * </p>
     *
     * @see #pauseTriggers(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public Set<String> resumeTriggers(final GroupMatcher<TriggerKey> matcher)
            throws JobPersistenceException {
        return executeInLock(
                LOCK_TRIGGER_ACCESS,
                new Callback<Set<String>>() {
                    public Set<String> execute() throws JobPersistenceException {
                        return resumeTriggerGroup(matcher);
                    }
                });

    }

    /**
     * <p>
     * Resume (un-pause) all of the <code>{@link org.quartz.Trigger}s</code>
     * matching the given groupMatcher.
     * </p>
     *
     * <p>
     * If any <code>Trigger</code> missed one or more fire-times, then the
     * <code>Trigger</code>'s misfire instruction will be applied.
     * </p>
     *
     * @see #pauseTriggers(org.quartz.impl.matchers.GroupMatcher)
     */
    public Set<String> resumeTriggerGroup(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {

        getDelegate().deletePausedTriggerGroup(matcher);
        HashSet<String> groups = new HashSet<String>();

        Set<TriggerKey> keys = getDelegate().selectTriggersInGroup(
                matcher);

        for (TriggerKey key: keys) {
            resumeTriggerIntern(key);
            groups.add(key.getGroup());
        }

        return groups;

        // FUTURE_TODO: find an efficient way to resume triggers (better than the
        // above)... logic below is broken because of
        // findTriggersToBeBlocked()
        /*
         * int res =
         * getDelegate().updateTriggerGroupStateFromOtherState(conn,
         * groupName, STATE_WAITING, PAUSED);
         *
         * if(res > 0) {
         *
         * long misfireTime = System.currentTimeMillis();
         * if(getMisfireThreshold() > 0) misfireTime -=
         * getMisfireThreshold();
         *
         * Key[] misfires =
         * getDelegate().selectMisfiredTriggersInGroupInState(conn,
         * groupName, STATE_WAITING, misfireTime);
         *
         * List blockedTriggers = findTriggersToBeBlocked(conn,
         * groupName);
         *
         * Iterator itr = blockedTriggers.iterator(); while(itr.hasNext()) {
         * Key key = (Key)itr.next();
         * getDelegate().updateTriggerState(conn, key.getName(),
         * key.getGroup(), BLOCKED); }
         *
         * for(int i=0; i < misfires.length; i++) {               String
         * newState = STATE_WAITING;
         * if(blockedTriggers.contains(misfires[i])) newState =
         * BLOCKED; updateMisfiredTrigger(conn,
         * misfires[i].getName(), misfires[i].getGroup(), newState, true); } }
         */
    }

    /**
     * <p>
     * Pause all triggers - equivalent of calling <code>pauseTriggerGroup(group)</code>
     * on every group.
     * </p>
     *
     * <p>
     * When <code>resumeAll()</code> is called (to un-pause), trigger misfire
     * instructions WILL be applied.
     * </p>
     *
     * @see #resumeAll()
     * @see #pauseTriggerGroup(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public void pauseAll() throws JobPersistenceException {
        executeInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {

                        List<String> names = getTriggerGroupNames();

                        for (String name: names) {
                            pauseTriggerGroup(GroupMatcher.triggerGroupEquals(name));
                        }

                        if (!getDelegate().isTriggerGroupPaused(ALL_GROUPS_PAUSED)) {
                            getDelegate().insertPausedTriggerGroup(ALL_GROUPS_PAUSED);
                        }
                    }
                });
    }

    /**
     * <p>
     * Resume (un-pause) all triggers - equivalent of calling <code>resumeTriggerGroup(group)</code>
     * on every group.
     * </p>
     *
     * <p>
     * If any <code>Trigger</code> missed one or more fire-times, then the
     * <code>Trigger</code>'s misfire instruction will be applied.
     * </p>
     *
     * @see #pauseAll()
     */
    @Override
    public void resumeAll()
            throws JobPersistenceException {
        executeInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {

                        List<String> names = getTriggerGroupNames();

                        for (String name: names) {
                            resumeTriggerGroup(GroupMatcher.triggerGroupEquals(name));
                        }

                        getDelegate().deletePausedTriggerGroup(ALL_GROUPS_PAUSED);
                    }
                });
    }

    private static long ftrCtr = System.currentTimeMillis();

    protected synchronized String getFiredTriggerRecordId() {
        return getInstanceId() + ftrCtr++;
    }

    /**
     * <p>
     * Get a handle to the next N triggers to be fired, and mark them as 'reserved'
     * by the calling scheduler.
     * </p>
     *
     * @see #releaseAcquiredTrigger(OperableTrigger)
     */
    @Override
    public List<OperableTrigger> acquireNextTriggers(final long noLaterThan, final int maxCount, final long timeWindow)
            throws JobPersistenceException {

        String lockName;
        if(isAcquireTriggersWithinLock() || maxCount > 1) {
            lockName = LOCK_TRIGGER_ACCESS;
        } else {
            lockName = null;
        }
        return executeInLockWithValidator(lockName,
                new Callback<List<OperableTrigger>>() {
                    public List<OperableTrigger> execute() throws JobPersistenceException {
                        return acquireNextTrigger(noLaterThan, maxCount, timeWindow);
                    }
                },
                new Validator<List<OperableTrigger>>() {
                    public Boolean validate(List<OperableTrigger> result) throws JobPersistenceException {
                        List<FiredTriggerRecord> acquired = getDelegate().selectInstancesFiredTriggerRecords();
                        Set<String> fireInstanceIds = new HashSet<String>();
                        for (FiredTriggerRecord ft : acquired) {
                            fireInstanceIds.add(ft.getFireInstanceId());
                        }
                        for (OperableTrigger tr : result) {
                            if (fireInstanceIds.contains(tr.getFireInstanceId())) {
                                return true;
                            }
                        }
                        return false;
                    }
                });
    }

    // FUTURE_TODO: this really ought to return something like a FiredTriggerBundle,
    // so that the fireInstanceId doesn't have to be on the trigger...
    protected List<OperableTrigger> acquireNextTrigger(long noLaterThan, int maxCount, long timeWindow)
            throws JobPersistenceException {
        if (timeWindow < 0) {
            throw new IllegalArgumentException();
        }

        List<OperableTrigger> acquiredTriggers = new ArrayList<OperableTrigger>();
        Set<JobKey> acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();
        final int MAX_DO_LOOP_RETRY = 3;
        int currentLoopCount = 0;
        do {
            currentLoopCount ++;
            try {
                List<TriggerKey> keys = getDelegate().selectTriggerToAcquire(noLaterThan + timeWindow, getMisfireTime(), maxCount);

                // No trigger is ready to fire yet.
                if (keys == null || keys.size() == 0)
                    return acquiredTriggers;

                long batchEnd = noLaterThan;

                for(TriggerKey triggerKey: keys) {
                    // If our trigger is no longer available, try a new one.
                    OperableTrigger nextTrigger = retrieveTrigger(triggerKey);
                    if(nextTrigger == null) {
                        continue; // next trigger
                    }

                    // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                    // put it back into the timeTriggers set and continue to search for next trigger.
                    JobKey jobKey = nextTrigger.getJobKey();
                    JobDetail job;
                    try {
                        job = retrieveJob(jobKey);
                    } catch (JobPersistenceException jpe) {
                        getLog().error("Error retrieving job, setting trigger state to ERROR.", jpe);
                        getDelegate().updateTriggerState(triggerKey, STATE_ERROR);
                        continue;
                    }

                    if (job.isConcurrentExectionDisallowed()) {
                        if (acquiredJobKeysForNoConcurrentExec.contains(jobKey)) {
                            continue; // next trigger
                        } else {
                            acquiredJobKeysForNoConcurrentExec.add(jobKey);
                        }
                    }

                    Date nextFireTime = nextTrigger.getNextFireTime();

                    // A trigger should not return NULL on nextFireTime when fetched from DB.
                    // But for whatever reason if we do have this (BAD trigger implementation or
                    // data?), we then should log a warning and continue to next trigger.
                    // User would need to manually fix these triggers from DB as they will not
                    // able to be clean up by Quartz since we are not returning it to be processed.
                    if (nextFireTime == null) {
                        log.warn("Trigger {} returned null on nextFireTime and yet still exists in DB!",
                                nextTrigger.getKey());
                        continue;
                    }

                    if (nextFireTime.getTime() > batchEnd) {
                        break;
                    }
                    // We now have a acquired trigger, let's add to return list.
                    // If our trigger was no longer in the expected state, try a new one.
                    int rowsUpdated = getDelegate().updateTriggerStateFromOtherState(triggerKey, STATE_ACQUIRED, STATE_WAITING);
                    if (rowsUpdated <= 0) {
                        continue; // next trigger
                    }
                    nextTrigger.setFireInstanceId(getFiredTriggerRecordId());
                    getDelegate().insertFiredTrigger(nextTrigger, STATE_ACQUIRED, null);

                    if(acquiredTriggers.isEmpty()) {
                        batchEnd = Math.max(nextFireTime.getTime(), System.currentTimeMillis()) + timeWindow;
                    }
                    acquiredTriggers.add(nextTrigger);
                }

                // if we didn't end up with any trigger to fire from that first
                // batch, try again for another batch. We allow with a max retry count.
                if(acquiredTriggers.size() == 0 && currentLoopCount < MAX_DO_LOOP_RETRY) {
                    continue;
                }

                // We are done with the while loop.
                break;
            } catch (Exception e) {
                throw new JobPersistenceException(
                        "Couldn't acquire next trigger: " + e.getMessage(), e);
            }
        } while (true);

        // Return the acquired trigger list
        return acquiredTriggers;
    }

    /**
     * <p>
     * Inform the <code>JobStore</code> that the scheduler no longer plans to
     * fire the given <code>Trigger</code>, that it had previously acquired
     * (reserved).
     * </p>
     */
    @Override
    public void releaseAcquiredTrigger(final OperableTrigger trigger) {
        retryExecutInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        getDelegate().updateTriggerStateFromOtherState(
                                trigger.getKey(), STATE_WAITING, STATE_ACQUIRED);
                        getDelegate().updateTriggerStateFromOtherState(
                                trigger.getKey(), STATE_WAITING, STATE_BLOCKED);
                        getDelegate().deleteFiredTrigger(trigger.getFireInstanceId());
                    }
                });
    }

    /**
     * <p>
     * Inform the <code>JobStore</code> that the scheduler is now firing the
     * given <code>Trigger</code> (executing its associated <code>Job</code>),
     * that it had previously acquired (reserved).
     * </p>
     *
     * @return null if the trigger or its job or calendar no longer exist, or
     *         if the trigger was not successfully put into the 'executing'
     *         state.
     */
    @Override
    public List<TriggerFiredResult> triggersFired(final List<OperableTrigger> triggers) throws JobPersistenceException {
        return executeInLockWithValidator(LOCK_TRIGGER_ACCESS,
                new Callback<List<TriggerFiredResult>>() {
                    public List<TriggerFiredResult> execute() throws JobPersistenceException {
                        List<TriggerFiredResult> results = new ArrayList<TriggerFiredResult>();

                        TriggerFiredResult result;
                        for (OperableTrigger trigger : triggers) {
                            try {
                                TriggerFiredBundle bundle = triggerFired(trigger);
                                result = new TriggerFiredResult(bundle);
                            } catch (JobPersistenceException jpe) {
                                result = new TriggerFiredResult(jpe);
                            } catch(RuntimeException re) {
                                result = new TriggerFiredResult(re);
                            }
                            results.add(result);
                        }

                        return results;
                    }
                },
                new Validator<List<TriggerFiredResult>>() {
                    @Override
                    public Boolean validate(List<TriggerFiredResult> result) throws JobPersistenceException {
                        List<FiredTriggerRecord> acquired = getDelegate().selectInstancesFiredTriggerRecords();
                        Set<String> executingTriggers = new HashSet<String>();
                        for (FiredTriggerRecord ft : acquired) {
                            if (STATE_EXECUTING.equals(ft.getFireInstanceState())) {
                                executingTriggers.add(ft.getFireInstanceId());
                            }
                        }
                        for (TriggerFiredResult tr : result) {
                            if (tr.getTriggerFiredBundle() != null && executingTriggers.contains(tr.getTriggerFiredBundle().getTrigger().getFireInstanceId())) {
                                return true;
                            }
                        }
                        return false;
                    }
                });
    }

    protected TriggerFiredBundle triggerFired(OperableTrigger trigger)
            throws JobPersistenceException {
        JobDetail job;
        Calendar cal = null;

        // Make sure trigger wasn't deleted, paused, or completed...
        String state = getDelegate().selectTriggerState(
                trigger.getKey());
        if (!state.equals(STATE_ACQUIRED)) {
            return null;
        }

        try {
            job = retrieveJob(trigger.getJobKey());
            if (job == null) { return null; }
        } catch (JobPersistenceException jpe) {
            getLog().error("Error retrieving job, setting trigger state to ERROR.", jpe);
            getDelegate().updateTriggerState(trigger.getKey(),
                    STATE_ERROR);
            throw jpe;
        }

        if (trigger.getCalendarName() != null) {
            cal = retrieveCalendar(trigger.getCalendarName());
            if (cal == null) { return null; }
        }

        getDelegate().updateFiredTrigger(trigger, STATE_EXECUTING, job);

        Date prevFireTime = trigger.getPreviousFireTime();

        // call triggered - to update the trigger's next-fire-time state...
        trigger.triggered(cal);

        state = STATE_WAITING;
        boolean force = true;

        if (job.isConcurrentExectionDisallowed()) {
            state = STATE_BLOCKED;
            force = false;
            getDelegate().updateTriggerStatesForJobFromOtherState(job.getKey(),
                    STATE_BLOCKED, STATE_WAITING);
            getDelegate().updateTriggerStatesForJobFromOtherState(job.getKey(),
                    STATE_BLOCKED, STATE_ACQUIRED);
            getDelegate().updateTriggerStatesForJobFromOtherState(job.getKey(),
                    STATE_PAUSED_BLOCKED, STATE_PAUSED);
        }

        if (trigger.getNextFireTime() == null) {
            state = STATE_COMPLETE;
            force = true;
        }

        storeTrigger(trigger, job, true, state, force, false);

        job.getJobDataMap().clearDirtyFlag();

        return new TriggerFiredBundle(job, trigger, cal, trigger.getKey().getGroup()
                .equals(Scheduler.DEFAULT_RECOVERY_GROUP), new Date(), trigger
                .getPreviousFireTime(), prevFireTime, trigger.getNextFireTime());
    }

    /**
     * <p>
     * Inform the <code>JobStore</code> that the scheduler has completed the
     * firing of the given <code>Trigger</code> (and the execution its
     * associated <code>Job</code>), and that the <code>{@link org.quartz.JobDataMap}</code>
     * in the given <code>JobDetail</code> should be updated if the <code>Job</code>
     * is stateful.
     * </p>
     */
    @Override
    public void triggeredJobComplete(final OperableTrigger trigger,
                                     final JobDetail jobDetail, final Trigger.CompletedExecutionInstruction triggerInstCode) {
        retryExecutInLock(
                LOCK_TRIGGER_ACCESS,
                new VoidCallback() {
                    public void executeVoid() throws JobPersistenceException {
                        if (triggerInstCode == Trigger.CompletedExecutionInstruction.DELETE_TRIGGER) {
                            if(trigger.getNextFireTime() == null) {
                                // double check for possible reschedule within job
                                // execution, which would cancel the need to delete...
                                TriggerStatus stat = getDelegate().selectTriggerStatus(
                                        trigger.getKey());
                                if(stat != null && stat.getNextFireTime() == null) {
                                    removeTrigger(trigger.getKey());
                                }
                            } else{
                                removeTrigger(trigger.getKey());
                                signalSchedulingChangeOnTxCompletion(0L);
                            }
                        } else if (triggerInstCode == Trigger.CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
                            getDelegate().updateTriggerState(trigger.getKey(),
                                    STATE_COMPLETE);
                            signalSchedulingChangeOnTxCompletion(0L);
                        } else if (triggerInstCode == Trigger.CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
                            getLog().info("Trigger " + trigger.getKey() + " set to ERROR state.");
                            getDelegate().updateTriggerState(trigger.getKey(),
                                    STATE_ERROR);
                            signalSchedulingChangeOnTxCompletion(0L);
                        } else if (triggerInstCode == Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
                            getDelegate().updateTriggerStatesForJob(
                                    trigger.getJobKey(), STATE_COMPLETE);
                            signalSchedulingChangeOnTxCompletion(0L);
                        } else if (triggerInstCode == Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
                            getLog().info("All triggers of Job " +
                                    trigger.getKey() + " set to ERROR state.");
                            getDelegate().updateTriggerStatesForJob(
                                    trigger.getJobKey(), STATE_ERROR);
                            signalSchedulingChangeOnTxCompletion(0L);
                        }

                        if (jobDetail.isConcurrentExectionDisallowed()) {
                            getDelegate().updateTriggerStatesForJobFromOtherState(
                                    jobDetail.getKey(), STATE_WAITING,
                                    STATE_BLOCKED);

                            getDelegate().updateTriggerStatesForJobFromOtherState(
                                    jobDetail.getKey(), STATE_PAUSED,
                                    STATE_PAUSED_BLOCKED);

                            signalSchedulingChangeOnTxCompletion(0L);
                        }
                        if (jobDetail.isPersistJobDataAfterExecution()) {
                            try {
                                if (jobDetail.getJobDataMap().isDirty()) {
                                    getDelegate().updateJobData(jobDetail);
                                }
                            } catch (IOException e) {
                                throw new JobPersistenceException(
                                        "Couldn't serialize job data: " + e.getMessage(), e);
                            }
                        }

                        getDelegate().deleteFiredTrigger(trigger.getFireInstanceId());
                    }
                });
    }

    protected StdRedisDelegate getDelegate() {
        return delegate;
    }

    //---------------------------------------------------------------------------
    // Management methods
    //---------------------------------------------------------------------------

    protected ThreadLocal<Long> sigChangeForTxCompletion = new ThreadLocal<Long>();
    protected void signalSchedulingChangeOnTxCompletion(long candidateNewNextFireTime) {
        Long sigTime = sigChangeForTxCompletion.get();
        if(sigTime == null && candidateNewNextFireTime >= 0L)
            sigChangeForTxCompletion.set(candidateNewNextFireTime);
        else {
            if(sigTime == null || candidateNewNextFireTime < sigTime)
                sigChangeForTxCompletion.set(candidateNewNextFireTime);
        }
    }

    protected Long clearAndGetSignalSchedulingChangeOnTxCompletion() {
        Long t = sigChangeForTxCompletion.get();
        sigChangeForTxCompletion.set(null);
        return t;
    }

    protected void signalSchedulingChangeImmediately(long candidateNewNextFireTime) {
        schedSignaler.signalSchedulingChange(candidateNewNextFireTime);
    }

    //---------------------------------------------------------------------------
    // Cluster management methods
    //---------------------------------------------------------------------------

    /**
     * Perform a redis operation while lock is acquired
     * @param lockName The name of the lock to acquire, for example
     * "TRIGGER_ACCESS".  If null, then no lock is acquired, but the
     * txCallback is still executed in a transaction.
     * @param callback a callback containing the actions to perform during lock
     * @param <T> return class
     * @return the result of the actions performed while locked, if any
     * @throws JobPersistenceException
     */
    protected <T> T executeInLock(String lockName, Callback<T> callback) throws JobPersistenceException {
        return executeInLockWithValidator(lockName, callback, null);
    }

    protected <T> T retryExecutInLock(String lockName, Callback<T> txCallback) {
        for (int retry = 1; !shutdown; retry++) {
            try {
                return executeInLockWithValidator(lockName, txCallback, null);
            } catch (JobPersistenceException jpe) {
                if(retry % 4 == 0) {
                    schedSignaler.notifySchedulerListenersError("An error occurred while " + txCallback, jpe);
                }
            } catch (RuntimeException e) {
                getLog().error("retryExecutInLock: RuntimeException " + e.getMessage(), e);
            }
            try {
                Thread.sleep(getRetryInterval()); // retry every N seconds (the db connection must be failed)
            } catch (InterruptedException e) {
                throw new IllegalStateException("Received interrupted exception", e);
            }
        }
        throw new IllegalStateException("JobStore is shutdown - aborting retry");
    }

    /**
     * Execute the given callback having optionally acquired the given lock.
     * This uses the non-managed transaction connection.
     *
     * @param lockName The name of the lock to acquire, for example
     * "TRIGGER_ACCESS".  If null, then no lock is acquired, but the
     * lockCallback is still executed in a non-managed transaction.
     */
    protected <T> T executeInLockWithValidator(
            String lockName,
            Callback<T> callback, final Validator<T> validator) throws JobPersistenceException {
        boolean transOwner = false;
        String lockValue = null;
        try {
            if (lockName != null) {
                lockValue = UUID.randomUUID().toString();
                transOwner = getDelegate().lock(lockName, lockValue, timeout);
            }
            T result = callback.execute();
            if (!transOwner && validator != null) {
                retryExecutInLock(lockName, new Callback<Boolean>() {
                    @Override
                    public Boolean execute() throws JobPersistenceException {
                        return validator.validate(result);
                    }
                });
            }

            Long sigTime = clearAndGetSignalSchedulingChangeOnTxCompletion();
            if(sigTime != null && sigTime >= 0) {
                signalSchedulingChangeImmediately(sigTime);
            }

            return result;
        } catch (JobPersistenceException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new JobPersistenceException("Unexpected runtime exception: "
                    + e.getMessage(), e);
        } finally {
            if (transOwner) {
                getDelegate().release(lockName, lockValue);
            }
        }
        /*boolean transOwner = false;
        Connection conn = null;
        try {
            if (lockName != null) {
                // If we aren't using db locks, then delay getting DB connection
                // until after acquiring the lock since it isn't needed.
                if (getLockHandler().requiresConnection()) {
                    conn = getNonManagedTXConnection();
                }

                transOwner = getLockHandler().obtainLock(conn, lockName);
            }

            if (conn == null) {
                conn = getNonManagedTXConnection();
            }

            final T result = txCallback.execute();
            try {
                commitConnection(conn);
            } catch (JobPersistenceException e) {
                rollbackConnection(conn);
                if (txValidator == null || !retryExecuteInNonManagedTXLock(lockName, new Callback<Boolean>() {
                    @Override
                    public Boolean execute() throws JobPersistenceException {
                        return txValidator.validate(result);
                    }
                })) {
                    throw e;
                }
            }

            Long sigTime = clearAndGetSignalSchedulingChangeOnTxCompletion();
            if(sigTime != null && sigTime >= 0) {
                signalSchedulingChangeImmediately(sigTime);
            }

            return result;
        } catch (JobPersistenceException e) {
            rollbackConnection(conn);
            throw e;
        } catch (RuntimeException e) {
            rollbackConnection(conn);
            throw new JobPersistenceException("Unexpected runtime exception: "
                    + e.getMessage(), e);
        } finally {
            try {
                releaseLock(lockName, transOwner);
            } finally {
                cleanupConnection(conn);
            }
        }*/
    }

    /**
     * Implement this interface to provide the code to execute within
     * the a template.  If no return value is required, execute
     * should just return null.
     *
     */
    protected interface Callback<T> {
        T execute() throws JobPersistenceException;
    }

    protected interface Validator<T> {
        Boolean validate(T result) throws JobPersistenceException;
    }

    /**
     * Implement this interface to provide the code to execute within
     * the a template that has no return value.
     *
     * @see #executeInLock(String, Callback)
     */
    protected abstract class VoidCallback implements Callback<Void> {
        public final Void execute() throws JobPersistenceException {
            executeVoid();
            return null;
        }

        abstract void executeVoid() throws JobPersistenceException;
    }

}
