/**
 *
 */
package com.bluejeans.redis.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;

/**
 * Redis client implementing collections operations
 *
 * @author Dinesh Ilindra
 */
@Resource
public class RedisCollectionClient<T extends Serializable> {

    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    public static final class CollEntry<T> {
        private final String coll, id;
        private final T value;
        private String parentColl;
        private String parentId;
        private final boolean indexId;
        private final long score;
        private long expiryMillis = 0;

        /**
         * @param coll
         * @param id
         * @param value
         * @param score
         */
        public CollEntry(final String coll, final String id, final long score, final T value) {
            this(coll, id, score, value, coll, id, true);
        }

        /**
         * @param coll
         * @param id
         * @param value
         * @param score
         * @param indexId
         */
        public CollEntry(final String coll, final String id, final long score, final T value, final boolean indexId) {
            super();
            this.coll = coll;
            this.id = id;
            this.value = value;
            this.score = score;
            this.indexId = indexId;
        }

        /**
         * @param coll
         * @param id
         * @param value
         * @param parentColl
         * @param parentId
         * @param score
         */
        public CollEntry(final String coll, final String id, final long score, final T value, final String parentColl,
                final String parentId) {
            this(coll, id, score, value, parentColl, parentId, true);
        }

        /**
         * @param coll
         * @param id
         * @param value
         * @param parentColl
         * @param parentId
         * @param score
         * @param indexId
         */
        public CollEntry(final String coll, final String id, final long score, final T value, final String parentColl,
                final String parentId, final boolean indexId) {
            super();
            this.coll = coll;
            this.id = id;
            this.value = value;
            this.parentColl = parentColl;
            this.parentId = parentId;
            this.score = score;
            this.indexId = indexId;
        }

        public String getColl() {
            return coll;
        }

        public String getId() {
            return id;
        }

        public T getValue() {
            return value;
        }

        public long getScore() {
            return score;
        }

        public String getParentColl() {
            return parentColl;
        }

        public String getParentId() {
            return parentId;
        }

        public boolean isIndexId() {
            return indexId;
        }

        /**
         * @return the expiryMillis
         */
        public long getExpiryMillis() {
            return expiryMillis;
        }

        public CollEntry<T> withExpiry(final long millis) {
            this.expiryMillis = millis;
            return this;
        }

    }

    private JedisPool writePool = null;
    private JedisPool readPool = null;
    private String primaryHost = null;
    private String replicaHost = null;
    private GenericObjectPoolConfig poolConfig;
    private String delim = ":";
    private String password, clientName;
    private int database = Protocol.DEFAULT_DATABASE;
    private int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
    private int soTimeout = Protocol.DEFAULT_TIMEOUT;
    private Class<?> dataType;
    private String valueOfMethod = "valueOf";

    @PostConstruct
    public void init() {
        if (poolConfig == null) {
            poolConfig = new GenericObjectPoolConfig();
        }
        if (dataType == null) {
            dataType = String.class;
        }
        if (primaryHost != null) {
            writePool = createPool(primaryHost);
        }
        if (replicaHost != null) {
            readPool = createPool(replicaHost);
        } else {
            readPool = writePool;
        }
    }

    @PreDestroy
    public void destroy() {
        if (writePool != null) {
            writePool.destroy();
        }
        if (readPool != null) {
            readPool.destroy();
        }
    }

    private JedisPool createPool(final String redisHost) {
        String host = redisHost;
        int port = Protocol.DEFAULT_PORT;
        if (redisHost.indexOf(':') > 0) {
            final String[] info = redisHost.split(":");
            host = info[0];
            port = Integer.parseInt(info[1]);
        }
        return new JedisPool(poolConfig, host, port, connectionTimeout, soTimeout, password, database, clientName);
    }

    public String _get(final String key) {
        try (Jedis jedis = readPool.getResource()) {
            return jedis.get(key);
        }
    }

    public byte[] _get(final byte[] key) {
        try (Jedis jedis = readPool.getResource()) {
            return jedis.get(key);
        }
    }

    public List<String> _get(final String... keys) {
        if (keys.length > 0) {
            try (Jedis jedis = readPool.getResource()) {
                return smget(jedis.mget(keys));
            }
        } else {
            return new ArrayList<>();
        }
    }

    public List<byte[]> _get(final byte[]... keys) {
        if (keys.length > 0) {
            try (Jedis jedis = readPool.getResource()) {
                return bmget(jedis.mget(keys));
            }
        } else {
            return new ArrayList<>();
        }
    }

    public void _set(final String key, final String value) {
        try (Jedis jedis = writePool.getResource()) {
            jedis.set(key, value);
        }
    }

    public void _set(final byte[] key, final byte[] value) {
        try (Jedis jedis = writePool.getResource()) {
            jedis.set(key, value);
        }
    }

    public void _set(final String key, final long expiryMillis, final String value) {
        try (Jedis jedis = writePool.getResource()) {
            jedis.set(key, value);
            jedis.pexpire(key, expiryMillis);
        }
    }

    public void _set(final byte[] key, final long expiryMillis, final byte[] value) {
        try (Jedis jedis = writePool.getResource()) {
            jedis.set(key, value);
            jedis.pexpire(key, expiryMillis);
        }
    }

    public void _set(final Map<T, T> data) {
        if (!data.isEmpty()) {
            final T sample = data.keySet().iterator().next();
            try (Jedis jedis = writePool.getResource()) {
                final Pipeline pipe = jedis.pipelined();
                pipe.multi();
                if (sample instanceof byte[]) {
                    data.forEach((key, value) -> pipe.set((byte[]) key, (byte[]) value));
                } else {
                    data.forEach((key, value) -> pipe.set(key.toString(), value.toString()));
                }
                pipe.exec();
                pipe.sync();
            }
        }
    }

    public void _expire(final String key, final long expiryMillis) {
        try (Jedis jedis = writePool.getResource()) {
            jedis.pexpire(key, expiryMillis);
        }
    }

    public void _expire(final byte[] key, final long expiryMillis) {
        try (Jedis jedis = writePool.getResource()) {
            jedis.pexpire(key, expiryMillis);
        }
    }

    public void _delete(final String... keys) {
        if (keys.length > 0) {
            try (Jedis jedis = writePool.getResource()) {
                jedis.del(keys);
            }
        }
    }

    public void _delete(final byte[]... keys) {
        if (keys.length > 0) {
            try (Jedis jedis = writePool.getResource()) {
                jedis.del(keys);
            }
        }
    }

    public void delete(final List<String> keys) {
        if (!keys.isEmpty()) {
            try (Jedis jedis = writePool.getResource()) {
                jedis.del(keys.toArray(EMPTY_STRING_ARRAY));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private T convert(final byte[] value) {
        if (value == null) {
            return null;
        }
        return (T) SerializationUtils.deserialize(value);
    }

    public T get(final String coll, final String id) {
        return get(coll + delim + id);
    }

    @SuppressWarnings("unchecked")
    public T get(final String key) {
        try (Jedis jedis = readPool.getResource()) {
            if (dataType == byte[].class) {
                return (T) jedis.get(key.getBytes());
            } else if (dataType == String.class) {
                return (T) jedis.get(key);
            } else {
                return convert(jedis.get(key.getBytes()));
            }
        }
    }

    public List<String> smget(final List<String> list) {
        list.removeAll(Collections.singleton(null));
        return list;
    }

    public List<byte[]> bmget(final List<byte[]> list) {
        list.removeAll(Collections.singleton(null));
        return list;
    }

    public List<T> tmget(final List<T> list) {
        list.removeAll(Collections.singleton(null));
        return list;
    }

    @SuppressWarnings("unchecked")
    public List<T> get(final List<String> keys) {
        if (!keys.isEmpty()) {
            try (Jedis jedis = readPool.getResource()) {
                if (dataType == byte[].class) {
                    return (List<T>) bmget(jedis.mget(keys.stream().map(k -> k.getBytes()).toArray(byte[][]::new)));
                } else if (dataType == String.class) {
                    return (List<T>) smget(jedis.mget(keys.toArray(EMPTY_STRING_ARRAY)));
                } else {
                    return tmget(jedis.mget(keys.stream().map(k -> k.getBytes()).toArray(byte[][]::new)).stream()
                            .map(v -> convert(v)).collect(Collectors.toList()));
                }
            }
        } else {
            return new ArrayList<>();
        }
    }

    @SuppressWarnings("unchecked")
    public List<T> getMulti(final String coll, final String... ids) {
        if (ids.length > 0) {
            try (Jedis jedis = readPool.getResource()) {
                if (dataType == byte[].class) {
                    final byte[][] byteKeys = new byte[ids.length][];
                    for (int index = 0; index < byteKeys.length; index++) {
                        byteKeys[index] = (coll + delim + ids[index]).getBytes();
                    }
                    return (List<T>) bmget(jedis.mget(byteKeys));
                } else if (dataType == String.class) {
                    final String[] keys = new String[ids.length];
                    for (int index = 0; index < keys.length; index++) {
                        keys[index] = coll + delim + ids[index];
                    }
                    return (List<T>) smget(jedis.mget(keys));
                } else {
                    final byte[][] keys = new byte[ids.length][];
                    for (int index = 0; index < keys.length; index++) {
                        keys[index] = (coll + delim + ids[index]).getBytes();
                    }
                    return tmget(jedis.mget(keys).stream().map(v -> convert(v)).collect(Collectors.toList()));
                }
            }
        } else {
            return new ArrayList<>();
        }
    }

    public List<T> getMulti(final String coll, final List<String> ids) {
        return getMulti(coll, ids.toArray(EMPTY_STRING_ARRAY));
    }

    public void expire(final String coll, final String id, final long expiryMillis) {
        _expire(toKey(coll, id), expiryMillis);
    }

    public String set(final String coll, final String id, final long score, final T value) {
        return set(coll, id, score, value, true);
    }

    public String set(final String coll, final String id, final long score, final T value, final boolean indexId) {
        try (Jedis jedis = writePool.getResource()) {
            final String key = coll + delim + id;
            if (dataType == byte[].class) {
                jedis.set(key.getBytes(), (byte[]) value);
            } else if (dataType == String.class) {
                jedis.set(key, String.valueOf(value));
            } else {
                jedis.set(key.getBytes(), SerializationUtils.serialize(value));
            }
            if (indexId) {
                jedis.zadd(coll + "_ids", score, id);
            }
            return key;
        }
    }

    public void setParent(final String coll, final String id, final long score, final String parentColl,
            final String parentId) {
        try (Jedis jedis = writePool.getResource()) {
            jedis.sadd(parentColl + "_childs", coll);
            jedis.sadd(coll + "_parents", parentColl);
            jedis.zadd(parentColl + "_children" + delim + parentId + delim + coll, score, id);
            jedis.set(coll + "_parent" + delim + id + delim + parentColl, parentId);
        }
    }

    public List<String> set(final List<CollEntry<T>> data) {
        try (Jedis jedis = writePool.getResource()) {
            final List<String> keys = new ArrayList<>();
            final Pipeline pipe = jedis.pipelined();
            pipe.multi();
            for (final CollEntry<T> ce : data) {
                final String key = ce.coll + delim + ce.id;
                keys.add(key);
                if (ce.value instanceof byte[]) {
                    pipe.set(key.getBytes(), (byte[]) ce.value);
                } else if (ce.value instanceof String) {
                    pipe.set(key, String.valueOf(ce.value));
                } else {
                    pipe.set(key.getBytes(), SerializationUtils.serialize(ce.value));
                }
                if (ce.indexId) {
                    pipe.zadd(ce.coll + "_ids", ce.score, ce.id);
                }
                if (ce.getParentColl() != null) {
                    pipe.sadd(ce.parentColl + "_childs", ce.coll);
                    pipe.sadd(ce.coll + "_parents", ce.parentColl);
                    pipe.zadd(ce.parentColl + "_children" + delim + ce.parentId + delim + ce.coll, ce.score, ce.id);
                    pipe.set(ce.coll + "_parent" + delim + ce.id + delim + ce.parentColl, ce.parentId);
                }
                if (ce.expiryMillis > 0) {
                    pipe.pexpire(key, ce.expiryMillis);
                }
            }
            pipe.exec();
            pipe.sync();
            return keys;
        }
    }

    public String set(final String coll, final String id, final long score, final T value, final String parentColl,
            final String parentId) {
        return set(coll, id, score, value, parentColl, parentId, true);
    }

    public String set(final String coll, final String id, final long score, final T value, final String parentColl,
            final String parentId, final boolean indexId) {
        setParent(coll, id, score, parentColl, parentId);
        return set(coll, id, score, value, indexId);
    }

    public long getCount(final String coll) {
        try (Jedis jedis = readPool.getResource()) {
            return jedis.zcard(coll + "_ids");
        }
    }

    public List<T> getAll(final String coll, final int offset, final int limit) {
        return getAll(coll, offset, limit, false);
    }

    public List<T> getAll(final String coll, final int offset, final int limit, final boolean desc) {
        return get(getAllIds(coll, offset, limit, desc, true));
    }

    public List<String> getAllIds(final String coll, final int offset, final int limit) {
        return getAllIds(coll, offset, limit, false, false);
    }

    public List<String> getAllIds(final String coll, final int offset, final int limit, final boolean desc,
            final boolean withCollNames) {
        int end = offset + limit - 1;
        if (limit < 0) {
            end = -1;
        }
        try (Jedis jedis = readPool.getResource()) {
            Set<String> ids = null;
            if (desc) {
                ids = jedis.zrevrange(coll + "_ids", offset, end);
            } else {
                ids = jedis.zrange(coll + "_ids", offset, end);
            }
            if (ids.isEmpty()) {
                return new ArrayList<String>();
            }
            return ids.stream().map(id -> withCollNames ? coll + delim + id : id).collect(Collectors.toList());
        }
    }

    public String getParentId(final String coll, final String id, final String parentColl) {
        try (Jedis jedis = readPool.getResource()) {
            return jedis.get(coll + "_parent" + delim + id + delim + parentColl);
        }
    }

    @SuppressWarnings("unchecked")
    public T getParent(final String coll, final String id, final String parentColl) {
        try (Jedis jedis = readPool.getResource()) {
            if (dataType == byte[].class) {
                return (T) jedis.get((parentColl + delim + getParentId(coll, id, parentColl)).getBytes());
            } else if (dataType == String.class) {
                return (T) jedis.get(parentColl + delim + getParentId(coll, id, parentColl));
            } else {
                return convert(jedis.get(parentColl + delim + getParentId(coll, id, parentColl)).getBytes());
            }
        }
    }

    public Set<String> getParentColls(final String coll) {
        try (Jedis jedis = readPool.getResource()) {
            return jedis.zrange(coll + "_parents", 0, -1);
        }
    }

    public Set<String> getChildColls(final String coll) {
        try (Jedis jedis = readPool.getResource()) {
            return jedis.zrange(coll + "_childs", 0, -1);
        }
    }

    public long getChildCount(final String coll, final String childColl, final String id) {
        try (Jedis jedis = readPool.getResource()) {
            return jedis.zcard(coll + "_children" + delim + id + delim + childColl);
        }
    }

    public List<T> getChilds(final String parentColl, final String childColl, final String parentId, final int offset,
            final int limit) {
        return getChilds(parentColl, childColl, parentId, offset, limit, false);
    }

    public List<T> getChilds(final String parentColl, final String childColl, final String parentId, final int offset,
            final int limit, final boolean desc) {
        return get(getChildIds(parentColl, childColl, parentId, offset, limit, desc));
    }

    public List<String> getChildIds(final String parentColl, final String childColl, final String parentId,
            final int offset, final int limit) {
        return getChildIds(parentColl, childColl, parentId, offset, limit, false);
    }

    public List<String> getChildIds(final String parentColl, final String childColl, final String parentId,
            final int offset, final int limit, final boolean desc) {
        int end = offset + limit - 1;
        if (limit < 0) {
            end = -1;
        }
        try (Jedis jedis = readPool.getResource()) {
            Set<String> ids = null;
            if (desc) {
                ids = jedis.zrevrange(parentColl + "_children" + delim + parentId + delim + childColl, offset, end);
            } else {
                ids = jedis.zrange(parentColl + "_children" + delim + parentId + delim + childColl, offset, end);
            }
            if (ids.isEmpty()) {
                return new ArrayList<String>();
            }
            return ids.stream().map(id -> childColl + delim + id).collect(Collectors.toList());
        }
    }

    public void delete(final String coll, final String id) {
        delete(coll, id, false);
    }

    public void delete(final String coll, final String id, final long expireMillis) {
        delete(coll, id, false, expireMillis);
    }

    public void delete(final String coll, final String id, final boolean deleteChildren) {
        delete(coll, id, deleteChildren, -1);
    }

    public void delete(final String coll, final String id, final boolean deleteChildren, final long expireMillis) {
        try (Jedis jedis = writePool.getResource()) {
            if (expireMillis <= 0) {
                jedis.del(coll + delim + id);
            } else {
                jedis.pexpire(coll + delim + id, expireMillis);
            }
            jedis.zrem(coll + "_ids", id);
            final Set<String> parentColls = jedis.smembers(coll + "_parents");
            parentColls.forEach(parentColl -> jedis
                    .zrem(parentColl + "_children" + delim + getParentId(coll, id, parentColl) + delim + coll, id));
            parentColls.forEach(parentColl -> jedis.del(coll + "_parent" + delim + id + delim + parentColl));
            final Set<String> childColls = jedis.smembers(coll + "_childs");
            if (deleteChildren) {
                delete(childColls.stream()
                        .map(childColl -> jedis.zrange(coll + "_children" + delim + id + delim + childColl, 0, -1)
                                .stream().map(cid -> childColl + delim + cid))
                        .flatMap(x -> x).collect(Collectors.toList()));
                childColls.forEach(childColl -> jedis.zrem(childColl + "_ids",
                        jedis.zrange(coll + "_children" + delim + id + delim + childColl, 0, -1)
                                .toArray(EMPTY_STRING_ARRAY)));
                delete(childColls.stream().map(childColl -> coll + "_children" + delim + id + delim + childColl)
                        .collect(Collectors.toList()));
            }
        }
    }

    public void clear(final String coll) {
        delete(getAllIds(coll, 0, -1));
    }

    public String toKey(final String coll, final String id) {
        return coll + delim + id;
    }

    public String[] fromKey(final String key) {
        return key.split(delim, 2);
    }

    /**
     * @return the primaryHost
     */
    public String getPrimaryHost() {
        return primaryHost;
    }

    /**
     * @param primaryHost
     *            the primaryHost to set
     */
    public void setPrimaryHost(final String primaryHost) {
        this.primaryHost = primaryHost;
    }

    /**
     * @return the replicaHost
     */
    public String getReplicaHost() {
        return replicaHost;
    }

    /**
     * @param replicaHost
     *            the replicaHost to set
     */
    public void setReplicaHost(final String replicaHost) {
        this.replicaHost = replicaHost;
    }

    public GenericObjectPoolConfig getPoolConfig() {
        return poolConfig;
    }

    public void setPoolConfig(final GenericObjectPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    public String getDelim() {
        return delim;
    }

    public void setDelim(final String delim) {
        this.delim = delim;
    }

    public JedisPool getPool() {
        return writePool;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(final String password) {
        this.password = password;
    }

    public String getClientName() {
        return clientName;
    }

    public void setClientName(final String clientName) {
        this.clientName = clientName;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(final int database) {
        this.database = database;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(final int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(final int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public Class<?> getDataType() {
        return dataType;
    }

    public void setDataType(final Class<?> dataType) {
        this.dataType = dataType;
    }

    public String getValueOfMethod() {
        return valueOfMethod;
    }

    public void setValueOfMethod(final String valueOfMethod) {
        this.valueOfMethod = valueOfMethod;
    }

}
