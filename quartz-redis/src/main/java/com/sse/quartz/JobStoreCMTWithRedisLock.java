package com.sse.quartz;

import org.quartz.JobPersistenceException;
import org.quartz.SchedulerConfigException;
import org.quartz.impl.jdbcjobstore.JobStoreCMT;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.SchedulerSignaler;

import java.sql.Connection;
import java.util.UUID;

public class JobStoreCMTWithRedisLock extends JobStoreCMT {

    private RedisLockHandler redisLockHandler;

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
     * default expire milliseconds
     */
    protected long expireInMills = 500;

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        super.initialize(loadHelper, signaler);
        redisLockHandler = new RedisLockHandler(host, port, database, password, ssl);
    }

    @Override
    protected <T> T executeInLock(
            String lockName,
            TransactionCallback<T> txCallback) throws JobPersistenceException {
        boolean transOwner = false;
        Connection conn = null;
        String lockValue = null;
        try {
            if (lockName != null) {
                // If we aren't using db locks, then delay getting DB connection
                // until after acquiring the lock since it isn't needed.
                lockValue = UUID.randomUUID().toString();
                transOwner = redisLockHandler.lock(lockName, lockValue, expireInMills);
            }

            if (conn == null) {
                conn = getConnection();
            }

            return txCallback.execute(conn);
        } finally {
            try {
                if (transOwner && lockValue != null) {
                    redisLockHandler.release(lockName, lockValue);
                }
            } finally {
                cleanupConnection(conn);
            }
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        redisLockHandler.shutdown();
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

    public long getExpireInMills() {
        return expireInMills;
    }

    public void setExpireInMills(long expireInMills) {
        this.expireInMills = expireInMills;
    }
}
