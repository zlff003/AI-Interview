package interview.guide.common.async;

import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.infrastructure.redis.RedisService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.PendingEntry;
import org.redisson.api.stream.StreamMessageId;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static interview.guide.common.constant.AsyncTaskStreamConstants.CONSUMER_POOL_SIZE;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_ERROR;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_FAILED_AT;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_ORIGINAL_STREAM;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_RETRY_COUNT;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_STREAM_SUFFIX;
import static interview.guide.common.constant.AsyncTaskStreamConstants.FIELD_RETRY_COUNT;
import static interview.guide.common.constant.AsyncTaskStreamConstants.MAX_RETRY_COUNT;
import static interview.guide.common.constant.AsyncTaskStreamConstants.PEL_BACKLOG_WARN_RATIO;
import static interview.guide.common.constant.AsyncTaskStreamConstants.PEL_CLAIM_BATCH_SIZE;
import static interview.guide.common.constant.AsyncTaskStreamConstants.PEL_CLAIM_IDLE_TIMEOUT_MS;
import static interview.guide.common.constant.AsyncTaskStreamConstants.PEL_MONITOR_INTERVAL_MS;
import static interview.guide.common.constant.AsyncTaskStreamConstants.PEL_RECOVERY_INTERVAL_MS;
import static interview.guide.common.constant.AsyncTaskStreamConstants.PEL_STUCK_CRITICAL_MS;
import static interview.guide.common.constant.AsyncTaskStreamConstants.STREAM_MAX_LEN;
import static interview.guide.common.constant.AsyncTaskStreamConstants.STREAM_TRIM_INTERVAL_MS;
import static interview.guide.common.constant.AsyncTaskStreamConstants.STREAM_TRIM_THRESHOLD_FACTOR;

/**
 * Redis Stream 消费者模板基类。
 * 支持并发消费、指数退避重试、死信队列(DLQ)、Pending 消息自动回收。
 *
 * <p>架构：1个轮询线程 + N个Worker线程 + Semaphore(N)背压</p>
 */
@Slf4j
public abstract class AbstractStreamConsumer<T> {

  private final RedisService redisService;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private String consumerName;
  private ExecutorService workerPool;
  private ScheduledExecutorService retryScheduler;
  private ScheduledExecutorService pelRecoveryScheduler;
  private Semaphore concurrencySemaphore;

  protected AbstractStreamConsumer(RedisService redisService) {
    this.redisService = redisService;
  }

  // ==================== 生命周期 ====================

  @PostConstruct
  public void init() {
    this.consumerName = consumerPrefix() + UUID.randomUUID().toString().substring(0, 8);
    this.concurrencySemaphore = new Semaphore(CONSUMER_POOL_SIZE);

    this.workerPool = new ThreadPoolExecutor(
        CONSUMER_POOL_SIZE,
        CONSUMER_POOL_SIZE,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(CONSUMER_POOL_SIZE * 2),
        r -> {
          Thread t = new Thread(r, threadName() + "-worker");
          t.setDaemon(true);
          return t;
        },
        new ThreadPoolExecutor.CallerRunsPolicy()
    );

    this.retryScheduler = Executors.newScheduledThreadPool(2, r -> {
      Thread t = new Thread(r, threadName() + "-retry");
      t.setDaemon(true);
      return t;
    });

    this.pelRecoveryScheduler = Executors.newScheduledThreadPool(1, r -> {
      Thread t = new Thread(r, threadName() + "-pel");
      t.setDaemon(true);
      return t;
    });

    running.set(true);

    // 启动 PEL 定时回收任务
    pelRecoveryScheduler.scheduleAtFixedRate(
        this::recoverStalePending,
        PEL_RECOVERY_INTERVAL_MS,
        PEL_RECOVERY_INTERVAL_MS,
        TimeUnit.MILLISECONDS
    );

    // 启动 Stream 安全裁剪任务（基于 PEL 状态，避免误删未处理消息）
    pelRecoveryScheduler.scheduleAtFixedRate(
        this::safeTrimStream,
        STREAM_TRIM_INTERVAL_MS,
        STREAM_TRIM_INTERVAL_MS,
        TimeUnit.MILLISECONDS
    );

    // 启动 PEL 积压监控告警任务
    pelRecoveryScheduler.scheduleAtFixedRate(
        this::monitorPelBacklog,
        PEL_MONITOR_INTERVAL_MS,
        PEL_MONITOR_INTERVAL_MS,
        TimeUnit.MILLISECONDS
    );

    // 启动轮询线程
    Thread pollerThread = new Thread(this::consumeLoop, threadName() + "-poller");
    pollerThread.setDaemon(true);
    pollerThread.start();

    log.info("{} consumer started: consumerName={}, poolSize={}",
        taskDisplayName(), consumerName, CONSUMER_POOL_SIZE);
  }

  @PreDestroy
  public void shutdown() {
    running.set(false);
    shutdownExecutor(workerPool, "workerPool");
    shutdownExecutor(retryScheduler, "retryScheduler");
    shutdownExecutor(pelRecoveryScheduler, "pelScheduler");
    log.info("{} consumer stopped: consumerName={}", taskDisplayName(), consumerName);
  }

  private void shutdownExecutor(ExecutorService executor, String name) {
    if (executor == null) {
      return;
    }
    executor.shutdown();
    try {
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  // ==================== 主循环 ====================

  private void consumeLoop() {
    try {
      redisService.createStreamGroup(streamKey(), groupName());
      log.info("Redis Stream group is ready: {}", groupName());
    } catch (Exception e) {
      log.warn("Failed to prepare Redis Stream group: groupName={}", groupName(), e);
    }

    while (running.get()) {
      try {
        // 背压：等待空闲 worker
        concurrencySemaphore.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }

      try {
        boolean hasMessages = redisService.streamConsumeMessages(
            streamKey(),
            groupName(),
            consumerName,
            AsyncTaskStreamConstants.BATCH_SIZE,
            AsyncTaskStreamConstants.POLL_INTERVAL_MS,
            (messageId, data) -> {
              workerPool.submit(() -> {
                try {
                  processMessage(messageId, data);
                } finally {
                  concurrencySemaphore.release();
                }
              });
            }
        );

        if (!hasMessages) {
          // 无消息时释放信号量（本轮未提交任何任务）
          concurrencySemaphore.release();
        }
      } catch (Exception e) {
        concurrencySemaphore.release();
        if (Thread.currentThread().isInterrupted()) {
          log.info("Consumer poller thread interrupted");
          break;
        }
        log.error("Failed to consume message: streamKey={}", streamKey(), e);
      }
    }
  }

  // ==================== 消息处理 ====================

  private void processMessage(StreamMessageId messageId, Map<String, String> data) {
    T payload = parsePayload(messageId, data);
    if (payload == null) {
      ackMessage(messageId);
      return;
    }

    int retryCount = parseRetryCount(data);
    log.info("Processing {} task: payload={}, messageId={}, retryCount={}",
        taskDisplayName(), payloadIdentifier(payload), messageId, retryCount);

    try {
      markProcessing(payload);
      processBusiness(payload);
      markCompleted(payload);
      ackMessage(messageId);
      log.info("{} task completed: {}", taskDisplayName(), payloadIdentifier(payload));
    } catch (Exception e) {
      log.error("{} task failed: {}", taskDisplayName(), payloadIdentifier(payload), e);
      if (retryCount < MAX_RETRY_COUNT) {
        scheduleRetry(messageId, data, retryCount, payload, e.getMessage());
      } else {
        handleDlq(messageId, data, retryCount, payload, e.getMessage());
      }
    }
  }

  // ==================== 指数退避重试 ====================

  /**
   * 使用指数退避调度重试：延迟 = 2^retryCount 秒（1s, 2s, 4s）
   */
  private void scheduleRetry(StreamMessageId messageId, Map<String, String> data,
                              int retryCount, T payload, String error) {
    long delaySeconds = (long) Math.pow(2, retryCount);
    log.info("{} task scheduled for retry: payload={}, retryCount={}, delay={}s",
        taskDisplayName(), payloadIdentifier(payload), retryCount + 1, delaySeconds);

    retryScheduler.schedule(() -> {
      try {
        Map<String, String> retryData = new HashMap<>(data);
        retryData.put(FIELD_RETRY_COUNT, String.valueOf(retryCount + 1));

        // 不传 maxLen，由安全裁剪任务统一处理，避免误删 PEL 中未处理的消息
        redisService.streamAdd(streamKey(), retryData, 0);
        ackMessage(messageId);
        log.info("{} task re-enqueued after retry: payload={}, retryCount={}",
            taskDisplayName(), payloadIdentifier(payload), retryCount + 1);
      } catch (Exception retryEx) {
        log.error("Failed to re-enqueue retry: payload={}, retryCount={}",
            payloadIdentifier(payload), retryCount + 1, retryEx);
        handleDlq(messageId, data, retryCount, payload,
            error + " [retry enqueue also failed: " + retryEx.getMessage() + "]");
      }
    }, delaySeconds, TimeUnit.SECONDS);
  }

  // ==================== 死信队列(DLQ) ====================

  /**
   * 将超过最大重试次数的消息写入死信队列
   */
  private void handleDlq(StreamMessageId messageId, Map<String, String> data,
                          int retryCount, T payload, String error) {
    String truncatedError = truncateError(error);
    log.warn("{} task moved to DLQ: payload={}, retryCount={}, error={}",
        taskDisplayName(), payloadIdentifier(payload), retryCount, truncatedError);

    try {
      Map<String, String> dlqData = new HashMap<>(data);
      dlqData.put(DLQ_FIELD_ORIGINAL_STREAM, streamKey());
      dlqData.put(DLQ_FIELD_ERROR, truncatedError);
      dlqData.put(DLQ_FIELD_RETRY_COUNT, String.valueOf(retryCount));
      dlqData.put(DLQ_FIELD_FAILED_AT, Instant.now().toString());

      redisService.streamAdd(streamKey() + DLQ_STREAM_SUFFIX, dlqData);
      markFailed(payload, truncatedError);
      ackMessage(messageId);
      log.info("{} task sent to DLQ successfully: payload={}",
          taskDisplayName(), payloadIdentifier(payload));
    } catch (Exception dlqEx) {
      log.error("Failed to send to DLQ: payload={}", payloadIdentifier(payload), dlqEx);
      markFailed(payload, truncatedError
          + " [DLQ write also failed: " + dlqEx.getMessage() + "]");
      // 仍然 ACK，避免 PEL 无限堆积
      ackMessage(messageId);
    }
  }

  // ==================== PEL 定时回收 ====================

  /**
   * 定时扫描 PEL，将超时未确认的消息重新分配给当前消费者处理。
   * 解决消费者宕机后消息残留在 PEL 中的问题。
   */
  private void recoverStalePending() {
    try {
      List<PendingEntry> pendingEntries = redisService.streamListPending(
          streamKey(), groupName(), PEL_CLAIM_BATCH_SIZE);

      if (pendingEntries == null || pendingEntries.isEmpty()) {
        return;
      }

      List<StreamMessageId> staleIds = pendingEntries.stream()
          .filter(e -> e.getIdleTime() > PEL_CLAIM_IDLE_TIMEOUT_MS)
          .map(PendingEntry::getId)
          .toList();

      if (staleIds.isEmpty()) {
        return;
      }

      log.info("Recovering {} stale pending messages from PEL: streamKey={}, groupName={}",
          staleIds.size(), streamKey(), groupName());

      Map<StreamMessageId, Map<String, String>> claimed =
          redisService.streamClaimMessages(
              streamKey(), groupName(), consumerName,
              PEL_CLAIM_IDLE_TIMEOUT_MS,
              staleIds.toArray(StreamMessageId[]::new));

      if (claimed == null || claimed.isEmpty()) {
        log.debug("No messages claimed from PEL (may already be claimed by others)");
        return;
      }

      log.info("Claimed {} stale messages from PEL", claimed.size());

      for (Map.Entry<StreamMessageId, Map<String, String>> entry : claimed.entrySet()) {
        try {
          concurrencySemaphore.acquire();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        workerPool.submit(() -> {
          try {
            processMessage(entry.getKey(), entry.getValue());
          } finally {
            concurrencySemaphore.release();
          }
        });
      }
    } catch (Exception e) {
      log.error("PEL recovery failed: streamKey={}, groupName={}",
          streamKey(), groupName(), e);
    }
  }

  // ==================== Stream 安全裁剪 ====================

  /**
   * 基于 PEL 状态的安全裁剪。
   * 有 Pending 消息时按最旧 Pending ID 裁剪（保护未处理消息）；
   * 无 Pending 消息时按 MAXLEN 裁剪（所有消息已消费，安全降级）。
   */
  private void safeTrimStream() {
    try {
      long streamLen = redisService.streamLen(streamKey());
      long threshold = (long) (STREAM_MAX_LEN * STREAM_TRIM_THRESHOLD_FACTOR);

      if (streamLen <= threshold) {
        return;
      }

      StreamMessageId minPendingId =
          redisService.streamGetMinPendingId(streamKey(), groupName());

      if (minPendingId == null) {
        // 无 Pending 消息：安全降级为 MAXLEN 裁剪
        long trimmed = redisService.streamTrimByMaxLen(streamKey(), STREAM_MAX_LEN);
        long newLen = redisService.streamLen(streamKey());
        log.info("Safe trim (no PEL): streamKey={}, oldLen={}, newLen={}, trimmed~{}",
            streamKey(), streamLen, newLen, trimmed);
      } else {
        // 有 Pending 消息：按最旧 Pending ID 裁剪，保护所有未处理消息
        long trimmed = redisService.streamTrimByMinId(streamKey(), minPendingId);
        long newLen = redisService.streamLen(streamKey());
        log.info("Safe trim (PEL-aware): streamKey={}, minPendingId={}, oldLen={}, newLen={}, trimmed~{}",
            streamKey(), minPendingId, streamLen, newLen, trimmed);

        // 裁剪后仍远超阈值 → PEL 堆积严重
        if (newLen > STREAM_MAX_LEN * 3) {
          log.warn("Stream still oversized after safe trim (PEL backlog suspected): "
              + "streamKey={}, streamLen={}, minPendingId={}",
              streamKey(), newLen, minPendingId);
        }
      }
    } catch (Exception e) {
      log.error("Safe trim failed: streamKey={}", streamKey(), e);
    }
  }

  // ==================== PEL 积压监控 ====================

  /**
   * 定时上报 PEL 积压指标，触发告警。
   * 监控维度：PEL/Stream 比率（积压告警）、最老消息闲置时间（卡死告警）。
   */
  private void monitorPelBacklog() {
    try {
      long streamLen = redisService.streamLen(streamKey());
      if (streamLen == 0) {
        return;
      }

      long pendingCount = redisService.streamPendingCount(streamKey(), groupName());
      double ratio = (double) pendingCount / streamLen;

      if (ratio > PEL_BACKLOG_WARN_RATIO) {
        log.warn("PEL backlog warning: streamKey={}, streamLen={}, pendingCount={}, ratio={}",
            streamKey(), streamLen, pendingCount, String.format("%.2f", ratio));
      }

      List<PendingEntry> oldestPending =
          redisService.streamListPending(streamKey(), groupName(), 1);
      if (oldestPending != null && !oldestPending.isEmpty()) {
        long idleTime = oldestPending.get(0).getIdleTime();
        if (idleTime > PEL_STUCK_CRITICAL_MS) {
          log.error("CRITICAL: PEL messages stuck > {}ms: streamKey={}, idleTime={}ms, "
              + "messageId={}, pendingCount={}",
              PEL_STUCK_CRITICAL_MS, streamKey(), idleTime,
              oldestPending.get(0).getId(), pendingCount);
        }
      }
    } catch (Exception e) {
      log.error("PEL monitor failed: streamKey={}", streamKey(), e);
    }
  }

  // ==================== 工具方法 ====================

  protected int parseRetryCount(Map<String, String> data) {
    try {
      return Integer.parseInt(data.getOrDefault(FIELD_RETRY_COUNT, "0"));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  protected String truncateError(String error) {
    if (error == null) {
      return null;
    }
    return error.length() > 500 ? error.substring(0, 500) : error;
  }

  private void ackMessage(StreamMessageId messageId) {
    try {
      redisService.streamAck(streamKey(), groupName(), messageId);
    } catch (Exception e) {
      log.error("Failed to ack stream message: messageId={}", messageId, e);
    }
  }

  protected RedisService redisService() {
    return redisService;
  }

  // ==================== 子类抽象方法 ====================

  protected abstract String taskDisplayName();

  protected abstract String streamKey();

  protected abstract String groupName();

  protected abstract String consumerPrefix();

  protected abstract String threadName();

  protected abstract T parsePayload(StreamMessageId messageId, Map<String, String> data);

  protected abstract String payloadIdentifier(T payload);

  protected abstract void markProcessing(T payload);

  protected abstract void processBusiness(T payload);

  protected abstract void markCompleted(T payload);

  /** 标记任务最终失败（仅状态持久化，重试/DLQ由基类处理） */
  protected abstract void markFailed(T payload, String error);

  /**
   * 重试消息入队。
   *
   * @deprecated 自 2.0 起，重试由基类 scheduleRetry() 统一处理（含指数退避）。
   *             子类可保留实现以保持编译兼容，但框架不再调用此方法。
   */
  @Deprecated
  protected void retryMessage(T payload, int retryCount) {
    // 默认空实现：重试由基类统一处理
  }
}
