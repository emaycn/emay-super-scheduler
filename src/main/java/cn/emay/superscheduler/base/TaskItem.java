package cn.emay.superscheduler.base;

import org.springframework.scheduling.config.Task;

/**
 * 任务元数据
 */
public class TaskItem {

    /**
     * 任务
     */
    private final Task task;
    /**
     * 任务
     */
    private final String name;
    /**
     * 分片
     */
    private final String sharded;

    /**
     * @param task    任务
     * @param name    名字
     * @param sharded 分片
     */
    public TaskItem(Task task, String name, String sharded) {
        this.task = task;
        this.name = name;
        this.sharded = sharded;
    }

    public Task getTask() {
        return task;
    }

    public String getName() {
        return name;
    }

    public String getSharded() {
        return sharded;
    }
}