package interview.guide.common.async;

import interview.guide.common.constant.AsyncTaskStreamConstants;
import interview.guide.common.model.DlqMessage;
import interview.guide.infrastructure.redis.RedisService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.stream.StreamMessageId;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_ERROR;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_FAILED_AT;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_ORIGINAL_STREAM;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_FIELD_RETRY_COUNT;
import static interview.guide.common.constant.AsyncTaskStreamConstants.DLQ_STREAM_SUFFIX;

/**
 * 死信队列(DLQ)管理服务。
 * 提供 DLQ 消息列表、重放、删除等运维操作。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DlqManagementService {

  private final RedisService redisService;

  /**
   * 列出指定 Stream 的 DLQ 消息。
   *
   * @param originalStreamKey 原始业务 Stream Key（如 resume:analyze:stream）
   * @param count             最多返回条数
   * @return DLQ 消息列表
   */
  public List<DlqMessage> listDlqMessages(String originalStreamKey, int count) {
    String dlqKey = originalStreamKey + DLQ_STREAM_SUFFIX;
    Map<StreamMessageId, Map<String, String>> messages =
        redisService.streamReadFromStart(dlqKey, count);

    if (messages == null || messages.isEmpty()) {
      return Collections.emptyList();
    }

    List<DlqMessage> result = new ArrayList<>();
    for (Map.Entry<StreamMessageId, Map<String, String>> entry : messages.entrySet()) {
      Map<String, String> data = entry.getValue();
      Map<String, String> payload = new java.util.HashMap<>(data);

      // 移除元数据字段，仅保留业务字段
      String originalStream = payload.remove(DLQ_FIELD_ORIGINAL_STREAM);
      String error = payload.remove(DLQ_FIELD_ERROR);
      String retryCountStr = payload.remove(DLQ_FIELD_RETRY_COUNT);
      String failedAt = payload.remove(DLQ_FIELD_FAILED_AT);

      int retryCount = 0;
      try {
        retryCount = Integer.parseInt(retryCountStr != null ? retryCountStr : "0");
      } catch (NumberFormatException ignored) {
        // ignore
      }

      result.add(new DlqMessage(
          entry.getKey().toString(),
          originalStream != null ? originalStream : originalStreamKey,
          Collections.unmodifiableMap(payload),
          error != null ? error : "Unknown error",
          retryCount,
          failedAt != null ? failedAt : Instant.now().toString()
      ));
    }

    return result;
  }

  /**
   * 重放 DLQ 消息到原始 Stream。
   *
   * @param originalStreamKey 原始业务 Stream Key
   * @param messageId         要重放的 DLQ 消息ID（字符串形式）
   */
  public boolean replayDlqMessage(String originalStreamKey, String messageId) {
    String dlqKey = originalStreamKey + DLQ_STREAM_SUFFIX;

    // 从 DLQ 读取该消息
    Map<StreamMessageId, Map<String, String>> allMessages =
        redisService.streamReadFromStart(dlqKey, 100);

    if (allMessages == null) {
      return false;
    }

    StreamMessageId targetId = null;
    Map<String, String> targetData = null;

    for (Map.Entry<StreamMessageId, Map<String, String>> entry : allMessages.entrySet()) {
      if (entry.getKey().toString().equals(messageId)) {
        targetId = entry.getKey();
        targetData = entry.getValue();
        break;
      }
    }

    if (targetId == null || targetData == null) {
      log.warn("DLQ message not found: dlqKey={}, messageId={}", dlqKey, messageId);
      return false;
    }

    // 提取原始 Stream 和业务数据
    String originalStream = targetData.getOrDefault(
        DLQ_FIELD_ORIGINAL_STREAM, originalStreamKey);

    Map<String, String> businessData = new java.util.HashMap<>(targetData);
    businessData.remove(DLQ_FIELD_ORIGINAL_STREAM);
    businessData.remove(DLQ_FIELD_ERROR);
    businessData.remove(DLQ_FIELD_RETRY_COUNT);
    businessData.remove(DLQ_FIELD_FAILED_AT);
    // 重置重试计数
    businessData.put(AsyncTaskStreamConstants.FIELD_RETRY_COUNT, "0");

    // 重新发送到原始 Stream（不传 maxLen，由安全裁剪任务统一处理）
    String newMessageId = redisService.streamAdd(originalStream, businessData, 0);
    log.info("DLQ message replayed: originalStream={}, dlqMessageId={}, newMessageId={}",
        originalStream, messageId, newMessageId);

    // 从 DLQ 删除
    redisService.streamRemoveMessages(dlqKey, targetId);
    log.info("DLQ message deleted after replay: dlqKey={}, messageId={}", dlqKey, messageId);

    return true;
  }

  /**
   * 删除指定 DLQ 消息。
   *
   * @param originalStreamKey 原始业务 Stream Key
   * @param messageId         要删除的消息ID（字符串形式）
   */
  public boolean deleteDlqMessage(String originalStreamKey, String messageId) {
    String dlqKey = originalStreamKey + DLQ_STREAM_SUFFIX;
    StreamMessageId sid = parseMessageId(messageId);
    if (sid == null) {
      log.warn("Invalid messageId format: {}", messageId);
      return false;
    }
    long removed = redisService.streamRemoveMessages(dlqKey, sid);
    if (removed > 0) {
      log.info("DLQ message deleted: dlqKey={}, messageId={}", dlqKey, messageId);
      return true;
    }
    log.warn("DLQ message not found for deletion: dlqKey={}, messageId={}", dlqKey, messageId);
    return false;
  }

  private StreamMessageId parseMessageId(String messageId) {
    try {
      String[] parts = messageId.split("-");
      if (parts.length == 2) {
        return new StreamMessageId(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
      }
    } catch (NumberFormatException ignored) {
      // ignore
    }
    return null;
  }
}
