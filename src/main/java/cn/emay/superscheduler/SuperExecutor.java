package cn.emay.superscheduler;


import cn.emay.superscheduler.base.TaskItem;
import cn.emay.superscheduler.base.TaskType;
import cn.emay.superscheduler.core.SuperScheduled;
import cn.emay.superscheduler.exec.DynamicDeployTaskGender;
import cn.emay.superscheduler.exec.FixedDelayTaskGender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.config.CronTask;
import org.springframework.scheduling.config.FixedDelayTask;
import org.springframework.scheduling.config.FixedRateTask;
import org.springframework.scheduling.config.TriggerTask;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ScheduledFuture;

/**
 * 业务线程池容器
 */
public class SuperExecutor {

    /**
     * log
     */
    private final Logger log = LoggerFactory.getLogger(SuperScheduler.class);
    /**
     * 动态任务容器
     */
    private final Map<String, Map<String, List<ScheduledFuture<?>>>> dynamicTasks = new HashMap<>();
    /**
     * 本节点是否竞争到锁
     */
    private volatile boolean hasLock = false;
    /**
     * 默认的分片
     */
    public static final String DEFAULT_SHARDED = "_32_DEFAULT_1024_";
    /**
     * 动态调整的分片
     */
    public static final String DYNAMIC_SHARDED = "_32_DYNAMIC_1024_";
    /**
     * 线程池
     */
    private final TaskScheduler taskScheduler;

    /**
     * @param taskScheduler 线程池
     */
    public SuperExecutor(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    /**
     * 是否竞争到了节点锁
     */
    public boolean isNoHasLock() {
        return !hasLock;
    }

    /**
     * 放置节点锁
     */
    public void setHasLock(boolean hasLock) {
        this.hasLock = hasLock;
    }

    /**
     * 停止并销毁所有任务
     */
    public synchronized void destroy() {
        for (Map<String, List<ScheduledFuture<?>>> map : this.dynamicTasks.values()) {
            map.values().forEach(list -> list.forEach(task -> task.cancel(false)));
        }
        dynamicTasks.clear();
    }

    /**
     * 根据任务名从动态任务管理器中拿任务集合<br/>
     * 动态调配的分片DYNAMIC_SHARDED不做统计
     *
     * @param name 任务名
     * @return 任务集合
     */
    private Map<String, List<ScheduledFuture<?>>> getTasksByName(String name) {
        return dynamicTasks.computeIfAbsent(name, k -> new HashMap<>());
    }

    /**
     * 根据任务名和属性名从动态任务管理器中拿任务集合<br/>
     * 动态调配的分片DYNAMIC_SHARDED不做统计
     *
     * @param name    任务名
     * @param sharded 分片
     * @return 任务集合
     */
    private List<ScheduledFuture<?>> getTasksByNameAndField(String name, String sharded) {
        Map<String, List<ScheduledFuture<?>>> fieldMap = getTasksByName(name);
        return fieldMap.computeIfAbsent(sharded, k -> new ArrayList<>());
    }

    /**
     * 获取当前任务的所有分片并发数量<br/>
     * 动态调配的分片DYNAMIC_SHARDED不做统计
     *
     * @param name 任务名称
     * @return 所有分片并发数量
     */
    public Map<String, Integer> getTasksByNameNumberWithoutDynamicSharded(String name) {
        Map<String, Integer> concurrent = new HashMap<>();
        Map<String, List<ScheduledFuture<?>>> map = dynamicTasks.computeIfAbsent(name, k -> new HashMap<>());
        map.forEach((sharded, list) -> {
            if (sharded.equals(SuperExecutor.DYNAMIC_SHARDED)) {
                return;
            }
            concurrent.put(sharded, list.size());
        });
        return concurrent;
    }

    /**
     * 生成一个cron任务
     *
     * @param only    是否但节点执行
     * @param name    任务名字
     * @param sharded 分片
     * @param bean    执行对象
     * @param method  执行方法
     * @param cron    cron表达式
     * @return cron任务
     */
    public TaskItem genCronTask(boolean only, String name, String sharded, Object bean, Method method, String cron) {
        FixedDelayTaskGender fixedDelayTaskGender = new FixedDelayTaskGender(this, only, name, sharded, bean, method);
        CronTask task = new CronTask(fixedDelayTaskGender.getRunnable(), cron);
        return new TaskItem(task, name, sharded);
    }

    /**
     * 生成一个动态延时任务
     *
     * @param only         是否但节点执行
     * @param name         任务名字
     * @param sharded      分片
     * @param bean         执行对象
     * @param method       执行方法
     * @param initialDelay 初始化延时时间
     * @return 动态延时任务
     */
    public TaskItem genDynamicDelayTask(boolean only, String name, String sharded, Object bean, Method method, long initialDelay) {
        DynamicDeployTaskGender exec = new DynamicDeployTaskGender(this, only, name, sharded, bean, method, initialDelay);
        TriggerTask task = new TriggerTask(exec.getTask(), exec.getTrigger());
        return new TaskItem(task, name, sharded);
    }

    /**
     * 生成一个固定延时任务
     *
     * @param only         是否但节点执行
     * @param name         任务名字
     * @param sharded      分片
     * @param bean         执行对象
     * @param method       执行方法
     * @param fixedDelay   延时时间
     * @param initialDelay 初始化延时时间
     * @return 固定延时任务
     */
    public TaskItem genFixedDelayTask(boolean only, String name, String sharded, Object bean, Method method, long fixedDelay, long initialDelay) {
        FixedDelayTaskGender fixedDelayTaskGender = new FixedDelayTaskGender(this, only, name, sharded, bean, method);
        FixedDelayTask task = new FixedDelayTask(fixedDelayTaskGender.getRunnable(), fixedDelay, initialDelay);
        return new TaskItem(task, name, sharded);
    }

    /**
     * 生成一个固定频率任务
     *
     * @param only         是否但节点执行
     * @param name         任务名字
     * @param sharded      分片
     * @param bean         执行对象
     * @param method       执行方法
     * @param fixedRate    频率
     * @param initialDelay 初始化延时时间
     * @return 固定频率任务
     */
    public TaskItem genFixedRateTask(boolean only, String name, String sharded, Object bean, Method method, long fixedRate, long initialDelay) {
        FixedDelayTaskGender fixedDelayTaskGender = new FixedDelayTaskGender(this, only, name, sharded, bean, method);
        FixedRateTask task = new FixedRateTask(fixedDelayTaskGender.getRunnable(), fixedRate, initialDelay);
        return new TaskItem(task, name, sharded);
    }

    /**
     * 启动一个任务
     *
     * @param item 任务
     */
    public synchronized void scheduleTask(TaskItem item) {
        ScheduledFuture<?> future;
        if (item.getTask() instanceof CronTask) {
            CronTask task = (CronTask) item.getTask();
            future = this.taskScheduler.schedule(task.getRunnable(), task.getTrigger());
        } else if (item.getTask() instanceof TriggerTask) {
            TriggerTask task = (TriggerTask) item.getTask();
            future = this.taskScheduler.schedule(task.getRunnable(), task.getTrigger());
        } else if (item.getTask() instanceof FixedDelayTask) {
            FixedDelayTask task = (FixedDelayTask) item.getTask();
            Date startTime = new Date(System.currentTimeMillis() + task.getInitialDelay());
            future = this.taskScheduler.scheduleWithFixedDelay(task.getRunnable(), startTime, task.getInterval());
        } else if (item.getTask() instanceof FixedRateTask) {
            FixedRateTask task = (FixedRateTask) item.getTask();
            Date startTime = new Date(System.currentTimeMillis() + task.getInitialDelay());
            future = this.taskScheduler.scheduleAtFixedRate(task.getRunnable(), startTime, task.getInterval());
        } else {
            return;
        }
        this.getTasksByNameAndField(item.getName(), item.getSharded()).add(future);
        if (log.isDebugEnabled()) {
            log.debug("启动任务" + item.getName() + "-" + item.getSharded());
        }
    }

    /**
     * 生成并启动一个任务
     *
     * @param taskType  任务类型
     * @param name      任务名
     * @param sharded   分片
     * @param bean      执行对象
     * @param method    执行方法
     * @param scheduled 定义
     */
    public void genAndScheduleTask(TaskType taskType, String name, String sharded, Object bean, Method method, SuperScheduled scheduled) {
        long initialDelay = Math.max(scheduled.initialDelay(), 0L);
        int concurrentMax = Math.max(scheduled.dynamicConcurrentMax(), 1);
        TaskItem item = null;
        switch (taskType) {
            case CRON:
                item = genCronTask(scheduled.only(), name, sharded, bean, method, scheduled.cron());
                break;
            case FIXED_RATE:
                item = genFixedRateTask(scheduled.only(), name, sharded, bean, method, scheduled.fixedRate(), initialDelay);
                break;
            case FIXED_DELAY:
                item = genFixedDelayTask(scheduled.only(), name, sharded, bean, method, scheduled.fixedDelay(), initialDelay);
                break;
            case DYNAMIC_DELAY:
                item = genDynamicDelayTask(scheduled.only(), name, sharded, bean, method, initialDelay);
                break;
            default:
                break;
        }
        if (item != null) {
            scheduleTask(item);
        }
    }

    /**
     * 停止并移除一个任务
     *
     * @param name    任务名称
     * @param sharded 分片
     */
    public synchronized void removeOneTask(String name, String sharded) {
        List<ScheduledFuture<?>> tasks = getTasksByNameAndField(name, sharded);
        if (tasks.size() == 0) {
            return;
        }
        ScheduledFuture<?> item = tasks.remove(0);
        item.cancel(false);
        if (log.isDebugEnabled()) {
            log.debug("清除任务" + name + "-" + sharded);
        }
    }

    /**
     * 停止并移除任务的所有分片
     *
     * @param name 任务名称
     */
    public void removeTaskByName(String name) {
        Map<String, List<ScheduledFuture<?>>> map = getTasksByName(name);
        List<String> allTasks = new ArrayList<>();
        map.forEach((sharded, tasks) -> tasks.forEach(task -> allTasks.add(sharded)));
        for (String sharded : allTasks) {
            removeOneTask(name, sharded);
        }
    }


    /**
     * 反射执行方法，基础工具方法
     *
     * @param bean   对象
     * @param method 方法
     * @param args   参数
     * @param <T>    返回值类型
     * @return 返回值
     */
    @SuppressWarnings("unchecked")
    public static <T> T invokeMethod(Object bean, Method method, Object... args) {
        try {
            return (T) method.invoke(bean, args);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
