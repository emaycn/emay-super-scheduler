package cn.emay.superscheduler.exec;

import cn.emay.superscheduler.SuperExecutor;
import cn.emay.superscheduler.base.SuperComputer;
import cn.emay.superscheduler.base.TaskType;
import cn.emay.superscheduler.core.ConcurrentComputer;
import cn.emay.superscheduler.core.ShardedConcurrentComputer;
import cn.emay.superscheduler.core.SuperScheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 动态计算并发数执行逻辑
 */
public class ComputeConcurrentExecutor {
    /**
     * 日志
     */
    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * 任务信息
     */
    private final SuperScheduled scheduled;
    /**
     * 任务名
     */
    private final String name;
    /**
     * 执行对象
     */
    private final Method method;
    /**
     * 执行方法
     */
    private final Object bean;
    /**
     * 任务类型
     */
    private final TaskType taskType;
    /**
     * 计算逻辑
     */
    private final SuperComputer superComputer;
    /**
     * 线程池
     */
    private final SuperExecutor executor;
    /**
     * 执行并发数量计算逻辑的方法
     */
    private static final Method LOGIC_METHOD;

    static {
        try {
            // 初始化加载出计算方法
            LOGIC_METHOD = ComputeConcurrentExecutor.class.getDeclaredMethod("compute");
            LOGIC_METHOD.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * @param executor 线程池
     * @param name          任务名
     * @param scheduled     任务定义
     * @param bean          执行对象
     * @param method        执行方法
     * @param taskType      任务类型
     * @param superComputer 并发计算器
     */
    public ComputeConcurrentExecutor(SuperExecutor executor, String name, SuperScheduled scheduled, Object bean, Method method, TaskType taskType, SuperComputer superComputer) {
        this.scheduled = scheduled;
        this.name = name;
        this.bean = bean;
        this.method = method;
        this.superComputer = superComputer;
        this.taskType = taskType;
        this.executor = executor;
    }

    /**
     * 计算
     */
    public void compute() {
        // 需要锁但是没有竞争到锁释放所有线程
        if (scheduled.only() && executor.isNoHasLock()) {
            executor.removeTaskByName(name);
            if(log.isDebugEnabled()){
                log.debug("本节点未获取到锁，任务" + name + "全部停止");
            }
            return;
        }

        // 根据当前分片并发数计算出所需分片并发数
        Map<String, Integer> concurrent = executor.getTasksByNameNumberWithoutDynamicSharded(name);
        Map<String, Integer> need = computerNeed(concurrent);

        // 计算出新增、删除、存在三个分片集合
        Set<String> hasSet = concurrent.keySet().stream().filter(need::containsKey).collect(Collectors.toSet());
        Set<String> removeSet = concurrent.keySet().stream().filter(sharded -> !hasSet.contains(sharded)).collect(Collectors.toSet());
        Set<String> addSet = need.keySet().stream().filter(sharded -> !hasSet.contains(sharded)).collect(Collectors.toSet());

        // 处理删除的分片
        removeSet.forEach(sharded -> {
            int size = concurrent.get(sharded);
            for (int i = 0; i < size; i++) {
                executor.removeOneTask(name, sharded);
            }
        });

        // 处理新增的分片
        addSet.forEach(sharded -> {
            int size = need.get(sharded);
            for (int i = 0; i < size; i++) {
                executor.genAndScheduleTask(taskType, name, sharded, bean, method, scheduled);
            }
        });

        // 处理存在的的分片
        hasSet.forEach(sharded -> {
            int theOld = concurrent.get(sharded);
            int theNew = need.get(sharded);
            if (theOld > theNew) {
                int sub = theOld - theNew;
                for (int i = 0; i < sub; i++) {
                    executor.removeOneTask(name, sharded);
                }
            } else if (theOld < theNew) {
                int add = theNew - theOld;
                for (int i = 0; i < add; i++) {
                    executor.genAndScheduleTask(taskType, name, sharded, bean, method, scheduled);
                }
            }
        });

        if(log.isDebugEnabled()){
            log.debug("调整并发 : 当前(" + toString(concurrent) + ") -> 需要(" + toString(need) + ")");
        }

    }

    /**
     * 根据当前并发数计算出所需并发数
     *
     * @param concurrent 当前并发数
     * @return 所需并发数
     */
    private Map<String, Integer> computerNeed(Map<String, Integer> concurrent) {
        Map<String, Integer> needNew;
        Map<String, Integer> need = new HashMap<>();
        if (superComputer instanceof ConcurrentComputer) {
            int now = concurrent.getOrDefault(SuperExecutor.DEFAULT_SHARDED, 0);
            int needNumber = ((ConcurrentComputer) superComputer).compute(now);
            need.put(SuperExecutor.DEFAULT_SHARDED, Math.max(needNumber, 0));
        } else if (superComputer instanceof ShardedConcurrentComputer) {
            Map<String, Integer> needOld = ((ShardedConcurrentComputer) superComputer).compute(concurrent);
            if (needOld != null) {
                need.putAll(needOld);
            }
        }

        int concurrentMax = scheduled.dynamicConcurrentMax();
        if (concurrentMax > 0) {
            long needNumber = need.values().stream().mapToInt((s) -> s).summaryStatistics().getSum();
            if (needNumber > concurrentMax) {
                needNew = new HashMap<>();
                AtomicInteger atomicInteger = new AtomicInteger(concurrentMax);
                need.forEach((sharded, number) -> {
                    for (int i = 0; i < number; i++) {
                        int now = atomicInteger.addAndGet(-1);
                        if (now < 0) {
                            break;
                        }
                        needNew.computeIfAbsent(sharded, k -> 0);
                        needNew.put(sharded, needNew.get(sharded) + 1);
                    }
                });
            } else {
                needNew = need;
            }
        } else {
            needNew = need;
        }
        return needNew;
    }


    /**
     * map转字符串，基础工具方法
     *
     * @param map map
     * @return 字符串
     */
    public String toString(Map<String, Integer> map) {
        StringBuilder builder = new StringBuilder();
        map.forEach((k, v) -> builder.append(k).append("=").append(v).append(";"));
        return builder.toString();
    }

    /**
     * 获取执行并发数量计算逻辑的方法
     */
    public static Method getLogicMethod() {
        return LOGIC_METHOD;
    }
}
