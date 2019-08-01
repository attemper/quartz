package com.github.quartz.nosqljobstore;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

public class RedisLockHandler {

    private RedisClient redisClient;

    private RedisCommands<String, String> redisCommands;
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

    public RedisLockHandler(String host, int port, int database, String password, boolean ssl) {
        RedisURI redisURI = RedisURI.builder().redis(host, port).withSsl(ssl).withDatabase(database).build();
        if (password != null && !"".equals(password.trim())) {
            redisURI.setPassword(password);
        }
        this.redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> connect = redisClient.connect();
        this.redisCommands = connect.sync();
    }
    /**
     * lock
     * @param key
     * @param value
     * @param expireInMills
     * @return
     */
    public boolean lock(String key, String value, long expireInMills) {
        return this.redisCommands.set(key, value, new SetArgs().px(expireInMills).nx()) != null;
    }

    /**
     * release lock
     * @param key
     * @param value
     * @return
     */
    public boolean release(String key, String value) {
        return this.redisCommands.eval(UNLOCK_LUA, ScriptOutputType.BOOLEAN, new String[]{key}, value);
    }

    public void shutdown() {
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }
}
