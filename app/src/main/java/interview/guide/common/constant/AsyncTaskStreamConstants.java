package interview.guide.common.constant;

/**
 * 异步任务 Redis Stream 通用常量
 * 包含知识库向量化和简历分析两个异步任务的配置
 */
public final class AsyncTaskStreamConstants {

    private AsyncTaskStreamConstants() {
        // 私有构造函数，防止实例化
    }

    // ========== 通用消息字段 ==========

    /**
     * 重试次数字段
     */
    public static final String FIELD_RETRY_COUNT = "retryCount";

    /**
     * 文档内容字段
     */
    public static final String FIELD_CONTENT = "content";

    // ========== 通用消费者配置 ==========

    /**
     * 最大重试次数
     */
    public static final int MAX_RETRY_COUNT = 3;

    /**
     * 每次拉取的消息批次大小
     */
    public static final int BATCH_SIZE = 10;

    /**
     * 消费者轮询间隔（毫秒）
     */
    public static final long POLL_INTERVAL_MS = 1000;

    /**
     * Stream 目标最大长度。
     * 不再在每次写入时裁剪（避免误删 PEL 中未处理的消息），
     * 改由定时安全裁剪任务根据 PEL 情况决定裁剪边界。
     */
    public static final int STREAM_MAX_LEN = 1000;

    // ========== Stream 安全裁剪配置 ==========

    /**
     * 安全裁剪间隔（毫秒）
     */
    public static final long STREAM_TRIM_INTERVAL_MS = 60_000;

    /**
     * 裁剪触发阈值因子：Stream 长度超过 MAX_LEN * factor 时触发裁剪
     */
    public static final double STREAM_TRIM_THRESHOLD_FACTOR = 1.5;

    // ========== PEL 监控配置 ==========

    /**
     * PEL 积压监控间隔（毫秒）
     */
    public static final long PEL_MONITOR_INTERVAL_MS = 60_000;

    /**
     * PEL 积压告警比率：Pending 数量超过 Stream 长度的此比例时 WARN
     */
    public static final double PEL_BACKLOG_WARN_RATIO = 0.3;

    /**
     * PEL 消息严重卡死阈值（毫秒）：最老消息闲置超过此时间触发 CRITICAL 告警
     */
    public static final long PEL_STUCK_CRITICAL_MS = 600_000;

    /**
     * 消费者工作线程池大小
     */
    public static final int CONSUMER_POOL_SIZE = 4;

    // ========== 死信队列(DLQ)配置 ==========

    /**
     * 死信队列 Stream Key 后缀
     */
    public static final String DLQ_STREAM_SUFFIX = ":dlq";

    /**
     * DLQ 消息字段：原始 Stream Key
     */
    public static final String DLQ_FIELD_ORIGINAL_STREAM = "_original_stream";

    /**
     * DLQ 消息字段：失败原因
     */
    public static final String DLQ_FIELD_ERROR = "_error";

    /**
     * DLQ 消息字段：终态重试次数
     */
    public static final String DLQ_FIELD_RETRY_COUNT = "_retry_count";

    /**
     * DLQ 消息字段：失败时间
     */
    public static final String DLQ_FIELD_FAILED_AT = "_failed_at";

    // ========== PEL 回收配置 ==========

    /**
     * PEL 回收扫描间隔（毫秒）
     */
    public static final long PEL_RECOVERY_INTERVAL_MS = 30_000;

    /**
     * PEL 消息空闲超过此时间将被回收（毫秒，默认5分钟）
     */
    public static final long PEL_CLAIM_IDLE_TIMEOUT_MS = 300_000;

    /**
     * PEL 回收时每次最多认领的消息数
     */
    public static final int PEL_CLAIM_BATCH_SIZE = 50;

    // ========== 知识库向量化 Stream 配置 ==========

    /**
     * 知识库向量化 Stream Key
     */
    public static final String KB_VECTORIZE_STREAM_KEY = "knowledgebase:vectorize:stream";

    /**
     * 知识库向量化 Consumer Group 名称
     */
    public static final String KB_VECTORIZE_GROUP_NAME = "vectorize-group";

    /**
     * 知识库向量化 Consumer 名称前缀
     */
    public static final String KB_VECTORIZE_CONSUMER_PREFIX = "vectorize-consumer-";

    /**
     * 知识库ID字段
     */
    public static final String FIELD_KB_ID = "kbId";

    // ========== 简历分析 Stream 配置 ==========

    /**
     * 简历分析 Stream Key
     */
    public static final String RESUME_ANALYZE_STREAM_KEY = "resume:analyze:stream";

    /**
     * 简历分析 Consumer Group 名称
     */
    public static final String RESUME_ANALYZE_GROUP_NAME = "analyze-group";

    /**
     * 简历分析 Consumer 名称前缀
     */
    public static final String RESUME_ANALYZE_CONSUMER_PREFIX = "analyze-consumer-";

    /**
     * 简历ID字段
     */
    public static final String FIELD_RESUME_ID = "resumeId";

    // ========== 面试评估 Stream 配置 ==========

    /**
     * 面试评估 Stream Key
     */
    public static final String INTERVIEW_EVALUATE_STREAM_KEY = "interview:evaluate:stream";

    /**
     * 面试评估 Consumer Group 名称
     */
    public static final String INTERVIEW_EVALUATE_GROUP_NAME = "evaluate-group";

    /**
     * 面试评估 Consumer 名称前缀
     */
    public static final String INTERVIEW_EVALUATE_CONSUMER_PREFIX = "evaluate-consumer-";

    /**
     * 面试会话ID字段
     */
    public static final String FIELD_SESSION_ID = "sessionId";

    // ========== 语音面试评估 Stream 配置 ==========

    /**
     * 语音面试评估 Stream Key
     */
    public static final String VOICE_EVALUATE_STREAM_KEY = "voice:evaluate:stream";

    /**
     * 语音面试评估 Consumer Group 名称
     */
    public static final String VOICE_EVALUATE_GROUP_NAME = "voice-evaluate-group";

    /**
     * 语音面试评估 Consumer 名称前缀
     */
    public static final String VOICE_EVALUATE_CONSUMER_PREFIX = "voice-evaluate-consumer-";

    /**
     * 语音面试会话ID字段
     */
    public static final String FIELD_VOICE_SESSION_ID = "voiceSessionId";
}
