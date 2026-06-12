package interview.guide.modules.admin.controller;

import interview.guide.common.async.DlqManagementService;
import interview.guide.common.model.DlqMessage;
import interview.guide.common.result.Result;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 死信队列(DLQ)管理接口。
 * 供运维人员查看、重放、删除死信消息。
 */
@Slf4j
@RestController
@RequestMapping("/api/admin/streams")
@RequiredArgsConstructor
public class DlqManagementController {

  private final DlqManagementService dlqManagementService;

  /**
   * 列出指定 Stream 的 DLQ 死信消息
   */
  @GetMapping("/{streamKey}/dlq")
  public Result<List<DlqMessage>> listDlqMessages(
      @PathVariable String streamKey,
      @RequestParam(defaultValue = "50") int count) {
    List<DlqMessage> messages = dlqManagementService.listDlqMessages(streamKey, count);
    return Result.success(messages);
  }

  /**
   * 重放 DLQ 消息到原始 Stream
   */
  @PostMapping("/{streamKey}/dlq/{messageId}/replay")
  public Result<String> replayDlqMessage(
      @PathVariable String streamKey,
      @PathVariable String messageId) {
    boolean ok = dlqManagementService.replayDlqMessage(streamKey, messageId);
    if (ok) {
      return Result.success("消息已重放到 " + streamKey);
    }
    return Result.error("消息重放失败：未找到消息 " + messageId);
  }

  /**
   * 删除指定 DLQ 消息
   */
  @DeleteMapping("/{streamKey}/dlq/{messageId}")
  public Result<String> deleteDlqMessage(
      @PathVariable String streamKey,
      @PathVariable String messageId) {
    boolean ok = dlqManagementService.deleteDlqMessage(streamKey, messageId);
    if (ok) {
      return Result.success("消息已从 DLQ 删除");
    }
    return Result.error("消息删除失败：未找到消息 " + messageId);
  }
}
