package interview.guide.infrastructure.redis;

import interview.guide.common.exception.BusinessException;
import interview.guide.common.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RKeys;
import org.redisson.api.RList;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.options.KeysScanOptions;
import org.redisson.api.stream.PendingEntry;
import org.redisson.api.stream.PendingResult;
import org.redisson.api.stream.PendingResult;
import org.redisson.api.stream.StreamAddArgs;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.api.stream.StreamPendingRangeArgs;
import org.redisson.api.stream.StreamRangeArgs;
import org.redisson.api.stream.StreamReadArgs;
import org.redisson.api.stream.StreamReadGroupArgs;
import org.redisson.api.stream.StreamTrimArgs;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Redis 服务封装
 * 提供通用的 Redis 操作，包括缓存、分布式锁、Stream 消息队列等
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisService {

    private final RedissonClient redissonClient;

    // ==================== 基础键值操作 ====================

    /**
     * 设置值（无过期时间）
     */
    public <T> void set(String key, T value) {
        RBucket<T> bucket = redissonClient.getBucket(key);
        bucket.set(value);
    }

    /**
     * 设置值（带过期时间）
     */
    public <T> void set(String key, T value, Duration ttl) {
        RBucket<T> bucket = redissonClient.getBucket(key);
        bucket.set(value, ttl);
    }

    /**
     * 获取值
     */
    public <T> T get(String key) {
        RBucket<T> bucket = redissonClient.getBucket(key);
        return bucket.get();
    }

    /**
     * 获取值，如果不存在则使用 loader 加载并缓存
     */
    public <T> T getOrLoad(String key, Duration ttl, Function<String, T> loader) {
        RBucket<T> bucket = redissonClient.getBucket(key);
        T value = bucket.get();
        if (value == null) {
            value = loader.apply(key);
            if (value != null) {
                bucket.set(value, ttl);
            }
        }
        return value;
    }

    /**
     * 删除键
     */
    public boolean delete(String key) {
        return redissonClient.getBucket(key).delete();
    }

    /**
     * 检查键是否存在
     */
    public boolean exists(String key) {
        return redissonClient.getBucket(key).isExists();
    }

    /**
     * 设置过期时间
     */
    public boolean expire(String key, Duration ttl) {
        return redissonClient.getBucket(key).expire(ttl);
    }

    /**
     * 获取剩余过期时间（毫秒）
     */
    public long getTimeToLive(String key) {
        return redissonClient.getBucket(key).remainTimeToLive();
    }

    // ==================== Hash 操作 ====================

    /**
     * 设置 Hash 字段
     */
    public <K, V> void hSet(String key, K field, V value) {
        RMap<K, V> map = redissonClient.getMap(key);
        map.put(field, value);
    }

    /**
     * 获取 Hash 字段
     */
    public <K, V> V hGet(String key, K field) {
        RMap<K, V> map = redissonClient.getMap(key);
        return map.get(field);
    }

    /**
     * 获取整个 Hash
     */
    public <K, V> Map<K, V> hGetAll(String key) {
        RMap<K, V> map = redissonClient.getMap(key);
        return map.readAllMap();
    }

    /**
     * 删除 Hash 字段
     */
    public <K, V> boolean hDelete(String key, K field) {
        RMap<K, V> map = redissonClient.getMap(key);
        return map.remove(field) != null;
    }

    /**
     * 检查 Hash 字段是否存在
     */
    public <K> boolean hExists(String key, K field) {
        RMap<K, Object> map = redissonClient.getMap(key);
        return map.containsKey(field);
    }

    // ==================== 分布式锁 ====================

    /**
     * 获取锁（阻塞等待）
     */
    public RLock getLock(String lockKey) {
        return redissonClient.getLock(lockKey);
    }

    /**
     * 尝试获取锁（非阻塞）
     */
    public boolean tryLock(String lockKey, long waitTime, long leaseTime, TimeUnit unit) {
        RLock lock = redissonClient.getLock(lockKey);
        try {
            return lock.tryLock(waitTime, leaseTime, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 释放锁
     */
    public void unlock(String lockKey) {
        RLock lock = redissonClient.getLock(lockKey);
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    /**
     * 执行带锁的操作
     */
    public <T> T executeWithLock(String lockKey, long waitTime, long leaseTime,
                                  TimeUnit unit, LockedOperation<T> operation) {
        RLock lock = redissonClient.getLock(lockKey);
        try {
            if (lock.tryLock(waitTime, leaseTime, unit)) {
                try {
                    return operation.execute();
                } finally {
                    lock.unlock();
                }
            }
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "获取锁失败: " + lockKey);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BusinessException(ErrorCode.INTERNAL_ERROR, "获取锁被中断: " + lockKey, e);
        }
    }

    @FunctionalInterface
    public interface LockedOperation<T> {
        T execute();
    }

    // ==================== Stream 消息队列 ====================

    /**
     * Stream 消息处理器接口
     */
    @FunctionalInterface
    public interface StreamMessageProcessor {
        void process(StreamMessageId messageId, Map<String, String> data);
    }

    /**
     * 消费 Stream 消息（阻塞模式）
     * 使用 Redis BLOCK 参数，让服务端等待消息，比客户端轮询更高效
     *
     * @param streamKey      Stream 键
     * @param groupName      消费者组名
     * @param consumerName   消费者名
     * @param count          每次读取数量
     * @param blockTimeoutMs 阻塞等待超时时间（毫秒），0 表示无限等待
     * @param processor      消息处理器
     * @return true 如果处理了消息，false 如果超时无消息
     */
    public boolean streamConsumeMessages(
            String streamKey,
            String groupName,
            String consumerName,
            int count,
            long blockTimeoutMs,
            StreamMessageProcessor processor) {

        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);

        // 使用阻塞读取，让 Redis 服务端等待消息
        Map<StreamMessageId, Map<String, String>> messages;
        try {
            messages = stream.readGroup(
                groupName,
                consumerName,
                StreamReadGroupArgs.neverDelivered()
                    .count(count)
                    .timeout(Duration.ofMillis(blockTimeoutMs))
            );
        } catch (ClassCastException e) {
            // Redisson 4.0.0 bug: 无消息时返回 EmptyList 而非空 Map，内部强转失败。
            // 等价于"本次无消息"，静默返回即可。
            return false;
        }

        if (messages == null || messages.isEmpty()) {
            return false;
        }

        for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
            processor.process(entry.getKey(), entry.getValue());
        }

        return true;
    }

    /**
     * 创建消费者组（如果不存在）
     */
    public void createStreamGroup(String streamKey, String groupName) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        try {
            stream.createGroup(StreamCreateGroupArgs.name(groupName).makeStream());
            log.info("创建 Stream 消费者组: stream={}, group={}", streamKey, groupName);
        } catch (Exception e) {
            // 组已存在，忽略
            if (e instanceof org.redisson.client.RedisException
                    && e.getMessage() != null
                    && e.getMessage().contains("BUSYGROUP")) {
                return;
            }
            log.warn("创建消费者组失败: stream={}, group={}, error={}", streamKey, groupName, e.getMessage());
        }
    }

    /**
     * 发送消息到 Stream
     */
    public String streamAdd(String streamKey, Map<String, String> message) {
        return streamAdd(streamKey, message, 0);
    }

    /**
     * 发送消息到 Stream（带长度限制）
     *
     * @param streamKey Stream 键
     * @param message   消息内容
     * @param maxLen    最大长度，超过时自动裁剪旧消息，0 表示不限制
     * @return 消息ID
     */
    public String streamAdd(String streamKey, Map<String, String> message, int maxLen) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        StreamAddArgs<String, String> args = StreamAddArgs.entries(message);
        if (maxLen > 0) {
            args.trimNonStrict().maxLen(maxLen);
        }
        StreamMessageId messageId = stream.add(args);
        log.debug("发送 Stream 消息: stream={}, messageId={}, maxLen={}", streamKey, messageId, maxLen);
        return messageId.toString();
    }

    /**
     * 从 Stream 读取消息（消费者组模式）
     */
    public Map<StreamMessageId, Map<String, String>> streamReadGroup(
            String streamKey, String groupName, String consumerName, int count) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.readGroup(groupName, consumerName,
            StreamReadGroupArgs.neverDelivered().count(count));
    }

    /**
     * 确认消息已处理
     */
    public void streamAck(String streamKey, String groupName, StreamMessageId... ids) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        stream.ack(groupName, ids);
    }

    /**
     * 获取 Stream 长度
     */
    public long streamLen(String streamKey) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.size();
    }

    /**
     * 查询消费者组中的 Pending 消息列表（用于 PEL 回收）
     *
     * @param streamKey Stream 键
     * @param groupName 消费者组名
     * @param count     最多返回条数
     * @return Pending 消息列表
     */
    public List<PendingEntry> streamListPending(
            String streamKey, String groupName, int count) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.listPending(
            StreamPendingRangeArgs.groupName(groupName)
                .startId(StreamMessageId.MIN)
                .endId(StreamMessageId.MAX)
                .count(count)
        );
    }

    /**
     * 认领闲置的 Pending 消息（XCLAIM）
     *
     * @param streamKey     Stream 键
     * @param groupName     消费者组名
     * @param consumerName  目标消费者名
     * @param minIdleTimeMs 最小闲置时间（毫秒）
     * @param ids           要认领的消息ID列表
     * @return 认领成功后的消息数据（消息ID → 字段数据）
     */
    public Map<StreamMessageId, Map<String, String>> streamClaimMessages(
            String streamKey, String groupName, String consumerName,
            long minIdleTimeMs, StreamMessageId... ids) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.claim(
            groupName,
            consumerName,
            minIdleTimeMs,
            TimeUnit.MILLISECONDS,
            ids
        );
    }

    /**
     * 按范围读取 Stream 消息（用于 DLQ 浏览，无需消费者组）
     *
     * @param streamKey Stream 键
     * @param start     起始消息ID
     * @param end       结束消息ID
     * @param count     最多返回条数
     * @return 消息数据（消息ID → 字段数据）
     */
    public Map<StreamMessageId, Map<String, String>> streamReadRange(
            String streamKey, StreamMessageId start, StreamMessageId end, int count) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.range(
            StreamRangeArgs.startId(start)
                .endId(end)
                .count(count)
        );
    }

    /**
     * 从 Stream 开头读取消息（无消费者组模式，用于 DLQ 浏览）
     *
     * @param streamKey Stream 键
     * @param count     最多返回条数
     * @return 消息数据（消息ID → 字段数据）
     */
    public Map<StreamMessageId, Map<String, String>> streamReadFromStart(
            String streamKey, int count) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.read(
            StreamReadArgs.greaterThan(StreamMessageId.MIN)
                .count(count)
        );
    }

    /**
     * 删除 Stream 中的消息（用于 DLQ 消息删除）
     *
     * @param streamKey Stream 键
     * @param ids       要删除的消息ID
     * @return 实际删除的消息数量
     */
    public long streamRemoveMessages(String streamKey, StreamMessageId... ids) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.remove(ids);
    }

    /**
     * 获取 Pending 消息数量
     *
     * @param streamKey Stream 键
     * @param groupName 消费者组名
     * @return Pending 消息总数
     */
    public long streamPendingCount(String streamKey, String groupName) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        PendingResult result = stream.getPendingInfo(groupName);
        return result.getTotal();
    }

    /**
     * 获取 Pending 汇总信息（含总数、最小/最大ID、消费者分布）。
     * 用于监控告警和安全裁剪。
     *
     * @param streamKey Stream 键
     * @param groupName 消费者组名
     * @return PendingResult，Stream 不存在时返回 null
     */
    public PendingResult streamGetPendingInfo(String streamKey, String groupName) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        try {
            return stream.getPendingInfo(groupName);
        } catch (Exception e) {
            log.debug("获取 Pending 信息失败（消费者组可能不存在）: streamKey={}, groupName={}",
                streamKey, groupName);
            return null;
        }
    }

    /**
     * 获取消费者组中最旧的 Pending 消息 ID（用于确定安全裁剪边界）。
     *
     * @param streamKey Stream 键
     * @param groupName 消费者组名
     * @return 最旧的 Pending 消息 ID，无 Pending 消息时返回 null
     */
    public StreamMessageId streamGetMinPendingId(String streamKey, String groupName) {
        List<PendingEntry> entries = streamListPending(streamKey, groupName, 1);
        if (entries == null || entries.isEmpty()) {
            return null;
        }
        return entries.get(0).getId();
    }

    /**
     * 按消息 ID 裁剪 Stream（XTRIM MINID）。
     * 删除所有 ID 小于指定值的消息，保留 minId 及之后的消息。
     * 使用近似裁剪（~），性能更好。
     *
     * @param streamKey Stream 键
     * @param minId     保留的最小消息 ID
     * @return 实际裁剪的消息数量
     */
    public long streamTrimByMinId(String streamKey, StreamMessageId minId) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.trim(StreamTrimArgs.minId(minId).noLimit());
    }

    /**
     * 按长度裁剪 Stream（XTRIM MAXLEN）。
     * 仅在没有 Pending 消息时使用，作为安全裁剪的降级方案。
     *
     * @param streamKey Stream 键
     * @param maxLen    保留的最大消息数
     * @return 实际裁剪的消息数量
     */
    public long streamTrimByMaxLen(String streamKey, int maxLen) {
        RStream<String, String> stream = redissonClient.getStream(streamKey, StringCodec.INSTANCE);
        return stream.trim(StreamTrimArgs.maxLen(maxLen).noLimit());
    }

    // ==================== 原子计数器 ====================

    /**
     * 获取原子计数器
     */
    public RAtomicLong getAtomicLong(String key) {
        return redissonClient.getAtomicLong(key);
    }

    /**
     * 自增并返回
     */
    public long increment(String key) {
        return redissonClient.getAtomicLong(key).incrementAndGet();
    }

    /**
     * 自减并返回
     */
    public long decrement(String key) {
        return redissonClient.getAtomicLong(key).decrementAndGet();
    }

    // ==================== 列表操作 ====================

    /**
     * 从列表右侧添加元素
     */
    public <T> void listRightPush(String key, T value) {
        RList<T> list = redissonClient.getList(key);
        list.add(value);
    }

    /**
     * 获取列表所有元素
     */
    public <T> List<T> listGetAll(String key) {
        RList<T> list = redissonClient.getList(key);
        return list.readAll();
    }

    // ==================== 工具方法 ====================

    /**
     * 获取 RedissonClient（用于高级操作）
     */
    public RedissonClient getClient() {
        return redissonClient;
    }

    /**
     * 按模式删除键
     */
    public long deleteByPattern(String pattern) {
        RKeys keys = redissonClient.getKeys();
        return keys.deleteByPattern(pattern);
    }

    /**
     * 按模式查找键
     */
    public Iterable<String> findKeysByPattern(String pattern) {
        RKeys keys = redissonClient.getKeys();
        return keys.getKeys(KeysScanOptions.defaults().pattern(pattern));
    }
}
