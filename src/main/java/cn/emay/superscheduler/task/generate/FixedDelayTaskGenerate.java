package cn.emay.superscheduler.task.generate;


import cn.emay.superscheduler.SuperExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * 静态延时任务执行器
 */
public class FixedDelayTaskGenerate {

    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * 执行对象
     */
    private final Method method;
    /**
     * 执行方法
     */
    private final Object bean;
    /**
     * 执行逻辑
     */
    private final Runnable runnable;
    /**
     * 任务名称
     */
    private final String taskName;
    /**
     * 分片
     */
    private final String sharded;
    /**
     * 线程池
     */
    private final SuperExecutor executor;
    /**
     * 是否单节点执行
     */
    private final boolean only;

    /**
     * @param executor 线程池
     * @param only     是否单节点执行
     * @param taskName 任务名称
     * @param sharded  分片
     * @param bean     执行对象
     * @param method   执行方法
     */
    public FixedDelayTaskGenerate(SuperExecutor executor, boolean only, String taskName, String sharded, Object bean, Method method) {
        this.bean = bean;
        this.method = method;
        this.only = only;
        this.sharded = sharded;
        this.taskName = taskName;
        this.executor = executor;

        this.runnable = genRunnable();
    }

    /**
     * 生成任务实体
     */
    private Runnable genRunnable() {
        return () -> {
            // 需要锁但是没有竞争到锁，不执行
            if (only && executor.isNoHasLock()) {
                if (log.isDebugEnabled()) {
                    log.debug("本节点未获取到锁，任务" + taskName + "不执行");
                }
                return;
            }
            if (SuperExecutor.DEFAULT_SHARDED.equals(this.sharded) || SuperExecutor.DYNAMIC_SHARDED.equals(this.sharded)) {
                SuperExecutor.invokeMethod(bean, method);
            } else {
                SuperExecutor.invokeMethod(bean, method, this.sharded);
            }
        };
    }

    public String getTaskName() {
        return taskName;
    }

    public String getSharded() {
        return sharded;
    }

    public Object getBean() {
        return bean;
    }

    public Method getMethod() {
        return method;
    }

    public Runnable getRunnable() {
        return runnable;
    }
}
