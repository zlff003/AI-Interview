package interview.guide.common.async;

import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_ERROR;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_FAILED_AT;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_ORIGINAL_STREAM;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_RETRY_COUNT;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_STREAM_SUFFIX;
import static interview.guide.common.constant.AsyncTaskStreamConstants.FIELD_RETRY_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.infrastructure.redis.RedisService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.redisson.api.stream.PendingEntry;
import org.redisson.api.stream.StreamMessageId;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * AbstractStreamConsumer 单元测试。
 * 覆盖正常处理、重试、DLQ、PEL回收、空消息等场景。
 */
@DisplayName("AbstractStreamConsumer 单元测试")
class AbstractStreamConsumerTest {

  private RedisService redisService;
  private ScheduledExecutorService mockRetryScheduler;
  private TestConsumer consumer;
  private StreamMessageId testMessageId;

  // ---------- 测试用的具体 Consumer 实现 ----------

  static class TestPayload {
    String id;
    String content;

    TestPayload(String id, String content) {
      this.id = id;
      this.content = content;
    }
  }

  static class TestConsumer extends AbstractStreamConsumer<TestPayload> {

    boolean processBusinessCalled = false;
    boolean processBusinessThrows = false;
    boolean markProcessingCalled = false;
    boolean markCompletedCalled = false;
    boolean markFailedCalled = false;
    String markFailedError;
    TestPayload lastPayload;
    int processBusinessCallCount = 0;

    TestConsumer(RedisService redisService) {
      super(redisService);
    }

    @Override
    protected String taskDisplayName() {
      return "TestTask";
    }

    @Override
    protected String streamKey() {
      return "test:stream";
    }

    @Override
    protected String groupName() {
      return "test-group";
    }

    @Override
    protected String consumerPrefix() {
      return "test-consumer-";
    }

    @Override
    protected String threadName() {
      return "test-thread";
    }

    @Override
    protected TestPayload parsePayload(StreamMessageId messageId, Map<String, String> data) {
      String id = data.get("id");
      String content = data.get("content");
      if (id == null) {
        return null;
      }
      return new TestPayload(id, content);
    }

    @Override
    protected String payloadIdentifier(TestPayload payload) {
      return "id=" + payload.id;
    }

    @Override
    protected void markProcessing(TestPayload payload) {
      markProcessingCalled = true;
      lastPayload = payload;
    }

    @Override
    protected void processBusiness(TestPayload payload) {
      processBusinessCalled = true;
      processBusinessCallCount++;
      lastPayload = payload;
      if (processBusinessThrows) {
        throw new RuntimeException("Simulated business failure");
      }
    }

    @Override
    protected void markCompleted(TestPayload payload) {
      markCompletedCalled = true;
    }

    @Override
    protected void markFailed(TestPayload payload, String error) {
      markFailedCalled = true;
      markFailedError = error;
    }
  }

  // ---------- Setup / Teardown ----------

  @BeforeEach
  void setUp() {
    redisService = mock(RedisService.class);
    mockRetryScheduler = mock(ScheduledExecutorService.class);

    consumer = new TestConsumer(redisService);

    // 注入 mock scheduler 以验证重试调度（绕过真实的异步延迟）
    ReflectionTestUtils.setField(consumer, "retryScheduler", mockRetryScheduler);
    // 设置 consumerName（跳过 init() 时，需要手动设置）
    ReflectionTestUtils.setField(consumer, "consumerName", "test-consumer-12345678");
    // 设置 concurrencySemaphore（PEL recovery 需要）
    ReflectionTestUtils.setField(consumer, "concurrencySemaphore",
        new java.util.concurrent.Semaphore(4));
    // 设置 workerPool（PEL recovery 需要）
    ReflectionTestUtils.setField(consumer, "workerPool",
        java.util.concurrent.Executors.newFixedThreadPool(2));

    testMessageId = new StreamMessageId(1_000_000L, 0L);
  }

  @AfterEach
  void tearDown() {
    consumer.shutdown();
  }

  // ---------- 工具方法 ----------

  private Map<String, String> buildTestData(String id, String content, int retryCount) {
    Map<String, String> data = new HashMap<>();
    data.put("id", id);
    data.put("content", content);
    data.put(FIELD_RETRY_COUNT, String.valueOf(retryCount));
    return data;
  }

  private void invokeProcessMessage(StreamMessageId msgId, Map<String, String> data) {
    ReflectionTestUtils.invokeMethod(consumer, "processMessage",
        msgId, data);
  }

  // ==================== 正常处理 ====================

  @Nested
  @DisplayName("正常处理流程")
  class SuccessfulProcessing {

    @Test
    @DisplayName("消息成功处理后 ACK 并标记完成")
    void shouldAckAndMarkCompletedOnSuccess() {
      Map<String, String> data = buildTestData("1", "hello", 0);

      invokeProcessMessage(testMessageId, data);

      assertThat(consumer.markProcessingCalled).isTrue();
      assertThat(consumer.processBusinessCalled).isTrue();
      assertThat(consumer.markCompletedCalled).isTrue();
      assertThat(consumer.markFailedCalled).isFalse();
      verify(redisService).streamAck("test:stream", "test-group", testMessageId);
    }

    @Test
    @DisplayName("多条消息可连续成功处理")
    void shouldProcessMultipleMessagesSequentially() {
      for (int i = 0; i < 5; i++) {
        TestConsumer freshConsumer = new TestConsumer(redisService);
        ReflectionTestUtils.setField(freshConsumer, "retryScheduler", mockRetryScheduler);

        Map<String, String> data = buildTestData(String.valueOf(i), "msg-" + i, 0);
        StreamMessageId msgId = new StreamMessageId(1_000_000L, i);

        ReflectionTestUtils.invokeMethod(freshConsumer, "processMessage", msgId, data);

        assertThat(freshConsumer.markCompletedCalled).isTrue();
        assertThat(freshConsumer.markFailedCalled).isFalse();
        freshConsumer.shutdown();
      }
    }
  }

  // ==================== 异常重试 ====================

  @Nested
  @DisplayName("异常重试流程")
  class RetryOnFailure {

    @Test
    @DisplayName("retryCount<3 时调度延迟重试（不立即 ACK）")
    void shouldScheduleRetryOnFailure() {
      consumer.processBusinessThrows = true;
      Map<String, String> data = buildTestData("1", "hello", 0);

      invokeProcessMessage(testMessageId, data);

      // 调度了延迟重试
      ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
      verify(mockRetryScheduler).schedule(
          runnableCaptor.capture(),
          eq(1L),  // delay = 2^0 = 1s
          eq(TimeUnit.SECONDS)
      );

      // 标记了 Processing 但未标记 Completed/Failed
      assertThat(consumer.markProcessingCalled).isTrue();
      assertThat(consumer.markCompletedCalled).isFalse();
      assertThat(consumer.markFailedCalled).isFalse();

      // 尚未 ACK（延迟重试的 runnable 才负责 ACK）
      verify(redisService, never()).streamAck(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("retryCount=1 时延迟 2s（指数退避）")
    void shouldUseExponentialBackoff() {
      consumer.processBusinessThrows = true;
      Map<String, String> data = buildTestData("1", "hello", 1);

      invokeProcessMessage(testMessageId, data);

      // 第二次重试: 2^1 = 2s 延迟
      verify(mockRetryScheduler).schedule(
          any(Runnable.class),
          eq(2L),
          eq(TimeUnit.SECONDS)
      );
    }

    @Test
    @DisplayName("retryCount=2 时延迟 4s（指数退避）")
    void shouldUseExponentialBackoffForRetry2() {
      consumer.processBusinessThrows = true;
      Map<String, String> data = buildTestData("1", "hello", 2);

      invokeProcessMessage(testMessageId, data);

      // 第三次重试: 2^2 = 4s 延迟
      verify(mockRetryScheduler).schedule(
          any(Runnable.class),
          eq(4L),
          eq(TimeUnit.SECONDS)
      );
    }
  }

  // ==================== 死信队列(DLQ) ====================

  @Nested
  @DisplayName("死信队列(DLQ)流程")
  class DeadLetterQueue {

    @Test
    @DisplayName("retryCount>=3 时写入 DLQ 并标记失败")
    void shouldSendToDlqWhenMaxRetryExceeded() {
      consumer.processBusinessThrows = true;
      Map<String, String> data = buildTestData("1", "hello", 3);

      invokeProcessMessage(testMessageId, data);

      // markFailed 被调用
      assertThat(consumer.markFailedCalled).isTrue();
      assertThat(consumer.markFailedError).contains("Simulated business failure");

      // DLQ 被写入
      ArgumentCaptor<Map<String, String>> dlqCaptor = ArgumentCaptor.forClass(Map.class);
      verify(redisService).streamAdd(
          eq("test:stream" + DLQ_STREAM_SUFFIX),
          dlqCaptor.capture()
      );

      Map<String, String> dlqData = dlqCaptor.getValue();
      assertThat(dlqData).containsKey(DLQ_FIELD_ORIGINAL_STREAM);
      assertThat(dlqData.get(DLQ_FIELD_ORIGINAL_STREAM)).isEqualTo("test:stream");
      assertThat(dlqData).containsKey(DLQ_FIELD_ERROR);
      assertThat(dlqData.get(DLQ_FIELD_ERROR)).contains("Simulated business failure");
      assertThat(dlqData).containsKey(DLQ_FIELD_RETRY_COUNT);
      assertThat(dlqData.get(DLQ_FIELD_RETRY_COUNT)).isEqualTo("3");
      assertThat(dlqData).containsKey(DLQ_FIELD_FAILED_AT);

      // 原始消息被 ACK
      verify(redisService).streamAck("test:stream", "test-group", testMessageId);
    }

    @Test
    @DisplayName("DLQ 写入失败时仍标记失败并 ACK 避免 PEL 堆积")
    void shouldMarkFailedAndAckEvenWhenDlqWriteFails() {
      consumer.processBusinessThrows = true;
      Map<String, String> data = buildTestData("1", "hello", 3);

      // DLQ 写入失败
      when(redisService.streamAdd(eq("test:stream" + DLQ_STREAM_SUFFIX), any()))
          .thenThrow(new RuntimeException("Redis down"));

      invokeProcessMessage(testMessageId, data);

      // 仍然标记了失败
      assertThat(consumer.markFailedCalled).isTrue();
      assertThat(consumer.markFailedError).contains("DLQ write also failed");

      // 仍然 ACK 了（避免 PEL 堆积）
      verify(redisService).streamAck("test:stream", "test-group", testMessageId);
    }
  }

  // ==================== 空消息处理 ====================

  @Nested
  @DisplayName("空消息/格式错误")
  class NullPayload {

    @Test
    @DisplayName("parsePayload 返回 null 时直接 ACK 跳过")
    void shouldAckAndSkipWhenPayloadIsNull() {
      Map<String, String> data = new HashMap<>();
      data.put("content", "no id field");

      invokeProcessMessage(testMessageId, data);

      // 未进入业务处理
      assertThat(consumer.markProcessingCalled).isFalse();
      assertThat(consumer.processBusinessCalled).isFalse();

      // 直接 ACK
      verify(redisService).streamAck("test:stream", "test-group", testMessageId);
    }
  }

  // ==================== 并发处理 ====================

  @Nested
  @DisplayName("并发处理")
  class ConcurrentProcessing {

    @Test
    @DisplayName("多条消息并发处理时各自独立完成")
    void shouldProcessMessagesConcurrently() throws Exception {
      // 使用慢速消费者模拟并发
      TestConsumer slowConsumer = new TestConsumer(redisService) {
        @Override
        protected void processBusiness(TestPayload payload) {
          super.processBusiness(payload);
          try {
            Thread.sleep(100);  // 模拟慢速 IO
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      };
      ReflectionTestUtils.setField(slowConsumer, "retryScheduler", mockRetryScheduler);

      int messageCount = 10;
      Thread[] threads = new Thread[messageCount];

      for (int i = 0; i < messageCount; i++) {
        final int idx = i;
        threads[i] = new Thread(() -> {
          Map<String, String> data = buildTestData(String.valueOf(idx), "msg-" + idx, 0);
          StreamMessageId msgId = new StreamMessageId(1_000_000L, idx);
          ReflectionTestUtils.invokeMethod(slowConsumer, "processMessage", msgId, data);
        });
      }

      // 启动所有线程
      for (Thread t : threads) {
        t.start();
      }
      for (Thread t : threads) {
        t.join(5000);
      }

      // 所有 10 条消息都成功处理
      assertThat(slowConsumer.processBusinessCallCount).isEqualTo(10);
      slowConsumer.shutdown();
    }
  }

  // ==================== PEL 回收 ====================

  @Nested
  @DisplayName("PEL 回收")
  class PelRecovery {

    @Test
    @DisplayName("扫描到超时 Pending 消息时调用 claim")
    void shouldClaimStalePendingMessages() {
      // 模拟 PendingEntry：闲置时间 600 秒（超过 300 秒阈值）
      StreamMessageId pendingId1 = new StreamMessageId(1_000_001L, 0L);
      StreamMessageId pendingId2 = new StreamMessageId(1_000_002L, 0L);

      PendingEntry idleEntry1 = new PendingEntry(
          pendingId1, "old-consumer-1", 600_000L, System.currentTimeMillis() - 600_000L);
      PendingEntry idleEntry2 = new PendingEntry(
          pendingId2, "old-consumer-2", 400_000L, System.currentTimeMillis() - 400_000L);

      when(redisService.streamListPending(anyString(), anyString(), anyInt()))
          .thenReturn(List.of(idleEntry1, idleEntry2));

      // 调用 recoverStalePending
      ReflectionTestUtils.invokeMethod(consumer, "recoverStalePending");

      // 验证 streamClaimMessages 确实被调用过（使用 mockingDetails 避免 varargs 匹配问题）
      boolean claimCalled = org.mockito.Mockito.mockingDetails(redisService)
          .getInvocations().stream()
          .anyMatch(inv -> inv.getMethod().getName().equals("streamClaimMessages"));
      assertThat(claimCalled)
          .as("Expected streamClaimMessages to be called for stale PEL recovery")
          .isTrue();
    }

    @Test
    @DisplayName("无超时消息时不调用 claim")
    void shouldNotClaimWhenNoStaleMessages() {
      // 模拟最近刚 pending 的消息（闲置 10 秒，未超过阈值）
      PendingEntry recentEntry = new PendingEntry(
          new StreamMessageId(1_000_001L, 0L),
          "consumer-1",
          10_000L,
          System.currentTimeMillis() - 10_000L);

      when(redisService.streamListPending(anyString(), anyString(), anyInt()))
          .thenReturn(List.of(recentEntry));

      ReflectionTestUtils.invokeMethod(consumer, "recoverStalePending");

      // 不应该调用 claim（没有超时消息）
      verify(redisService, never()).streamClaimMessages(
          anyString(), anyString(), anyString(), anyLong());
    }

    @Test
    @DisplayName("PEL 为空时不抛异常")
    void shouldNotThrowWhenPelIsEmpty() {
      when(redisService.streamListPending(anyString(), anyString(), anyInt()))
          .thenReturn(List.of());

      assertDoesNotThrow(() ->
          ReflectionTestUtils.invokeMethod(consumer, "recoverStalePending"));
    }
  }

  // ==================== 工具方法 ====================

  @Nested
  @DisplayName("工具方法")
  class UtilityMethods {

    @Test
    @DisplayName("parseRetryCount 默认返回 0")
    void shouldReturnZeroForMissingRetryCount() {
      Map<String, String> data = Map.of("id", "1");
      assertThat(consumer.parseRetryCount(data)).isEqualTo(0);
    }

    @Test
    @DisplayName("parseRetryCount 正确解析数字")
    void shouldParseRetryCountCorrectly() {
      Map<String, String> data = Map.of(FIELD_RETRY_COUNT, "5");
      assertThat(consumer.parseRetryCount(data)).isEqualTo(5);
    }

    @Test
    @DisplayName("truncateError 截断过长错误信息")
    void shouldTruncateLongError() {
      String longError = "x".repeat(1000);
      String result = consumer.truncateError(longError);
      assertThat(result).hasSize(500);
    }

    @Test
    @DisplayName("truncateError 对 null 返回 null")
    void shouldReturnNullForNullError() {
      assertThat(consumer.truncateError(null)).isNull();
    }

    @Test
    @DisplayName("truncateError 不过度截断短信息")
    void shouldNotTruncateShortError() {
      String shortError = "Short error message";
      String result = consumer.truncateError(shortError);
      assertThat(result).isEqualTo(shortError);
    }
  }
}
