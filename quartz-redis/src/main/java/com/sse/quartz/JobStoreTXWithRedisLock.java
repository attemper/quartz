package com.sse.quartz;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;
import org.quartz.impl.jdbcjobstore.JobStoreTX;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;
import java.sql.Connection;
import java.util.UUID;

/**
 * @author ldang
 */
public class JobStoreTXWithRedisLock extends JobStoreTX {

    private RedisClient redisClient;

    private RedisCommands<String, String> redisCommands;

    private static final String UNLOCK_LUA;

    /**
     * lua for release lock
     */
    static {
        UNLOCK_LUA = "if redis.call(\"get\",KEYS[1]) == ARGV[1] " +
                     "then " +
                     "    return redis.call(\"del\",KEYS[1]) " +
                     "else " +
                     "    return 0 " +
                     "end ";
    }

    /**
     * host. default is localhost
     */
    protected String host = "localhost";

    /**
     * port. default is 6379
     */
    protected int port = 6379;

    /**
     * password default is null
     */
    protected String password;

    /**
     * database default is 0
     */
    protected int database;

    /**
     * using ssl. default is false
     */
    protected boolean ssl;

    /**
     * default expire seconds
     */
    protected int expireInSeconds = 2;

    @Override
    public void initialize(ClassLoadHelper classLoadHelper, SchedulerSignaler schedSignaler) throws SchedulerConfigException {
        super.initialize(classLoadHelper, schedSignaler);
        //init redis
        RedisURI redisURI = RedisURI.builder().redis(host, port).withSsl(ssl).withDatabase(database).build();
        if (password != null && !"".equals(password.trim())) {
            redisURI.setPassword(password);
        }
        //RedisURI redisURI = RedisURI.create(host, port);
        this.redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connect = redisClient.connect();
        this.redisCommands = connect.sync();
    }

    @Override
    protected <T> T executeInLock(String lockName, TransactionCallback<T> txCallback) throws JobPersistenceException {
        boolean transOwner = false;
        Connection conn = null;
        String lockValue = null;
        try {
            if (lockName != null) {
                lockValue = UUID.randomUUID().toString();
                transOwner = lock(lockName, lockValue, expireInSeconds);
            }

            if (conn == null) {
                conn = getNonManagedTXConnection();
            }

            final T result = txCallback.execute(conn);
            try {
                commitConnection(conn);
            } catch (JobPersistenceException e) {
                rollbackConnection(conn);
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
                if (transOwner && lockValue != null) {
                    release(lockName, lockValue);
                }
            } finally {
                cleanupConnection(conn);
            }
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    /**
     * lock
     * @param key
     * @param value
     * @param expire
     * @return
     */
    private boolean lock(String key, String value, long expire) {
        return this.redisCommands.set(key, value, new SetArgs().ex(expire).nx()) != null;
    }

    /**
     * release lock
     * @param key
     * @param value
     * @return
     */
    private boolean release(String key, String value) {
        return this.redisCommands.eval(UNLOCK_LUA, ScriptOutputType.BOOLEAN, new String[]{key}, value);
    }

    /* ---------- Note: set methods must be void----------- */

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public boolean isSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }
}
