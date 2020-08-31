package cn.emay.superscheduler.base;

/**
 * 任务类型
 */
public enum TaskType {
    /**
     * Cron 表达式任务
     */
    CRON,
    /**
     * 固定时间间隔任务
     */
    FIXED_DELAY,
    /**
     * 固定频率任务
     */
    FIXED_RATE,
    /**
     * 动态时间间隔任务
     */
    DYNAMIC_DELAY
}