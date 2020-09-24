package cn.emay.superscheduler.task.generate;

import cn.emay.superscheduler.SuperExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.Trigger;

import java.lang.reflect.Method;
import java.util.Date;

/**
 * 动态延时任务执行器
 */
public class DynamicDeployTaskGenerate {

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
     * 执行触发器
     */
    private final Trigger trigger;
    /**
     * 任务名称
     */
    private final String taskName;
    /**
     * 属性
     */
    private final String sharded;
    /**
     * 初始化延时，仅在创建执行单元后第一次执行生效
     */
    private Long initialDelay;
    /**
     * 执行间隔，每次执行后都会刷新此值
     */
    private long delayMillis = 0L;
    /**
     * 线程池
     */
    private final SuperExecutor executor;
    /**
     * 是否单节点执行
     */
    private final boolean only;

    /**
     * @param executor           线程池
     * @param only               是否单节点执行
     * @param taskName           任务名称
     * @param sharded            分片
     * @param bean               执行对象
     * @param method             执行方法
     * @param initialDelayMillis 初始化延迟时间
     */
    public DynamicDeployTaskGenerate(SuperExecutor executor, boolean only, String taskName, String sharded, Object bean, Method method, long initialDelayMillis) {
        this.bean = bean;
        this.method = method;
        this.only = only;
        this.sharded = sharded;
        this.taskName = taskName;
        this.executor = executor;

        this.initialDelay = initialDelayMillis;
        this.runnable = genRunnable();
        this.trigger = genTrigger();
    }

    /**
     * 生成任务实体
     */
    private Runnable genRunnable() {
        return () -> {
            try {
                // 需要锁但是没有竞争到锁就不执行，休息10秒
                if (this.only && this.executor.isNoHasLock()) {
                    if (log.isDebugEnabled()) {
                        log.debug("本节点未获取到锁，任务" + taskName + "不执行");
                    }
                    this.delayMillis = 10L * 1000L;
                    return;
                }
                if (SuperExecutor.DEFAULT_SHARDED.equals(this.sharded) || SuperExecutor.DYNAMIC_SHARDED.equals(this.sharded)) {
                    this.delayMillis = SuperExecutor.invokeMethod(bean, method);
                } else {
                    this.delayMillis = SuperExecutor.invokeMethod(bean, method, this.sharded);
                }
                if (log.isDebugEnabled()) {
                    log.debug("执行任务 " + taskName + " 完毕");
                }
            } catch (Throwable e) {
                this.delayMillis = 1000L;
                log.error("执行任务  " + taskName + " 报错", e);
            }
        };
    }

    /**
     * 生成计时器
     */
    private Trigger genTrigger() {
        return triggerContext -> {
            if (initialDelay == null) {
                return new Date(this.delayMillis + System.currentTimeMillis());
            } else {
                Date date = new Date(this.delayMillis + initialDelay + System.currentTimeMillis());
                initialDelay = null;
                return date;
            }
        };
    }

    public Object getBean() {
        return bean;
    }

    public Method getMethod() {
        return method;
    }

    public Runnable getTask() {
        return runnable;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getSharded() {
        return sharded;
    }

}
