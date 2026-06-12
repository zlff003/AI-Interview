package interview.guide.common.model;

import java.util.Map;

/**
 * 死信队列(DLQ)消息 DTO，用于管理接口返回。
 */
public record DlqMessage(
    String messageId,
    String originalStream,
    Map<String, String> payload,
    String error,
    int retryCount,
    String failedAt
) {}
