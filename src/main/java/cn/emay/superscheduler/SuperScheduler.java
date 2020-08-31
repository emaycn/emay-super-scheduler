package cn.emay.superscheduler;

import cn.emay.redis.RedisClient;
import cn.emay.superscheduler.base.TaskItem;
import cn.emay.superscheduler.base.TaskType;
import cn.emay.superscheduler.core.ConcurrentComputer;
import cn.emay.superscheduler.core.ShardedConcurrentComputer;
import cn.emay.superscheduler.core.SuperScheduled;
import cn.emay.superscheduler.exec.ComputeConcurrentExecutor;
import cn.emay.superscheduler.exec.GetLockTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * super scheduler 加载单元
 */
public class SuperScheduler implements BeanPostProcessor, ApplicationContextAware, InitializingBean, DisposableBean, ApplicationRunner {

    /**
     * log
     */
    private final Logger log = LoggerFactory.getLogger(SuperScheduler.class);
    /**
     * 缓存spring context
     */
    public static ApplicationContext APPLICATION_CONTEXT;
    /**
     * 线程池核心线程数
     */
    @Value("${scheduler.poolSize}")
    private int poolSize;
    /**
     * 线程池线程名称前缀
     */
    @Value("${scheduler.threadNamePrefix}")
    private String threadNamePrefix;
    /**
     * 线程池停止时等待业务执行完毕时间
     */
    @Value("${scheduler.awaitTerminationSeconds}")
    private int awaitTerminationSeconds;
    /**
     * redis bean 名称
     */
    @Value("${scheduler.redisBeanName}")
    private String redisBeanName;
    /**
     * 单节点锁名字
     */
    @Value("${scheduler.onlyLockName}")
    private String onlyLockName;
    /**
     * 线程池
     */
    private ThreadPoolTaskScheduler businessScheduler;
    /**
     * 是否开启节点锁
     */
    private boolean isOnlyEnable = false;
    /**
     * 单节点锁线程池
     */
    private ThreadPoolTaskScheduler onlyLockScheduler;
    /**
     * 用来做分布式锁的redis
     */
    private RedisClient redis;
    /**
     * 扫描到的SuperScheduled临时存放容器
     */
    private List<TaskItem> tempWaitTasks;
    /**
     * 缓存spring的scheduler注册器
     */
    private SuperExecutor executor;

    /**
     * 1. 缓存spring上下文
     *
     * @param applicationContext spring上下文
     * @throws BeansException BeansException
     */
    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        APPLICATION_CONTEXT = applicationContext;
    }

    /**
     * 2. 初始化业务线程池、业务线程池容器、redis
     */
    @Override
    public void afterPropertiesSet() {
        tempWaitTasks = new ArrayList<>();
        businessScheduler = new ThreadPoolTaskScheduler();
        businessScheduler.setPoolSize(Math.max(1, poolSize));
        businessScheduler.setThreadNamePrefix(threadNamePrefix);
        businessScheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
        businessScheduler.setAwaitTerminationSeconds(Math.max(0, awaitTerminationSeconds));
        businessScheduler.initialize();
        executor = new SuperExecutor(businessScheduler);
        if (redisBeanName != null) {
            redis = APPLICATION_CONTEXT.getBean(redisBeanName, RedisClient.class);
        }
    }

    /**
     * 3. 在所有bean创建成功后，遍历所有bean，加载@SuperScheduled的方法
     *
     * @param bean     bean
     * @param beanName bean名称
     * @return bean
     * @throws BeansException BeansException
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, @NonNull String beanName) throws BeansException {
        for (Method method : bean.getClass().getDeclaredMethods()) {
            SuperScheduled scheduled = method.getAnnotation(SuperScheduled.class);
            if (scheduled != null) {
                this.processSuperScheduled(scheduled, bean, method);
            }
        }
        return bean;
    }

    /**
     * 加载@SuperScheduled的逻辑
     *
     * @param scheduled 定义
     * @param bean      对象
     * @param method    方法
     */
    private void processSuperScheduled(SuperScheduled scheduled, Object bean, Method method) {
        String name = "@SuperScheduled[" + bean.getClass().getName() + ":" + method.getName() + "]";
        boolean only = scheduled.only();
        if (only && redis == null) {
            throw new IllegalArgumentException(name + "集群单节点执行参数[only=true]，但是redis[" + redisBeanName + "]没有找到");
        }
        isOnlyEnable = isOnlyEnable || only;

        long initialDelay = Math.max(scheduled.initialDelay(), 0L);

        long dynamicConcurrentComputeDelay = scheduled.dynamicConcurrentComputeDelay();
        String dynamicConcurrentComputeBean = scheduled.dynamicConcurrentComputeBean();
        int dynamicConcurrentMax = scheduled.dynamicConcurrentMax();
        int fixedConcurrent = scheduled.fixedConcurrent();
        boolean isDynamicConcurrent = dynamicConcurrentComputeDelay > 0L;
        boolean isShardedDynamicConcurrent = false;
        Object computer = null;
        if (isDynamicConcurrent) {
            try {
                computer = APPLICATION_CONTEXT.getBean(dynamicConcurrentComputeBean);
            } catch (BeansException e) {
                throw new IllegalArgumentException(name + "动态调整并发开启，但是调整Bean[" + dynamicConcurrentComputeBean + "]在spring没有注册");
            }
            if (computer instanceof ConcurrentComputer) {
                isShardedDynamicConcurrent = false;
                if (method.getParameterCount() != 0) {
                    throw new IllegalArgumentException(name + "动态并发任务，方法不能定义参数");
                }
            } else if (computer instanceof ShardedConcurrentComputer) {
                isShardedDynamicConcurrent = true;
                if (method.getParameterCount() != 1) {
                    throw new IllegalArgumentException(name + "动态分片并发任务，方法必须只能有一个String类型的参数");
                }
                if (!method.getParameterTypes()[0].getName().equals(String.class.getName())) {
                    throw new IllegalArgumentException(name + "动态分片并发任务，方法必须只能有一个String类型的参数");
                }
            } else {
                throw new IllegalArgumentException(name + "动态调整并发开启，但是调整Bean[" + dynamicConcurrentComputeBean + "]不是支持的类型");
            }
        } else {
            if (fixedConcurrent <= 0) {
                throw new IllegalArgumentException(name + "采用固定并发数，但是并发数设置小于0[fixedConcurrent=" + fixedConcurrent + "]");
            }
            if (method.getParameterCount() != 0) {
                throw new IllegalArgumentException(name + "固定并发任务，方法不能定义参数");
            }
        }

        if (scheduled.cron().length() > 0) {
            if (!method.getReturnType().equals(void.class)) {
                throw new IllegalArgumentException(name + "CRON任务，方法返回值类型必须是void");
            }
            if (isDynamicConcurrent) {
                addDynamicTask(isShardedDynamicConcurrent, name, scheduled, bean, method, TaskType.CRON, dynamicConcurrentComputeDelay, computer);
            } else {
                for (int i = 0; i < fixedConcurrent; i++) {
                    TaskItem item = executor.genCronTask(scheduled.only(), name, SuperExecutor.DEFAULT_SHARDED, bean, method, scheduled.cron());
                    tempWaitTasks.add(item);
                    if (log.isDebugEnabled()) {
                        log.debug("加载" + TaskType.CRON.toString() + "任务" + item.getName());
                    }
                }
            }
        }

        if (scheduled.fixedDelay() > 0L) {
            if (!method.getReturnType().equals(void.class)) {
                throw new IllegalArgumentException(name + "固定间隔时间任务，方法返回值类型必须是void");
            }
            if (isDynamicConcurrent) {
                addDynamicTask(isShardedDynamicConcurrent, name, scheduled, bean, method, TaskType.FIXED_DELAY, dynamicConcurrentComputeDelay, computer);
            } else {
                for (int i = 0; i < fixedConcurrent; i++) {
                    TaskItem item = executor.genFixedDelayTask(scheduled.only(), name, SuperExecutor.DEFAULT_SHARDED, bean, method, scheduled.fixedDelay(), initialDelay);
                    tempWaitTasks.add(item);
                    if (log.isDebugEnabled()) {
                        log.debug("加载" + TaskType.FIXED_DELAY.toString() + "任务" + item.getName());
                    }
                }
            }
        }

        if (scheduled.fixedRate() > 0) {
            if (!method.getReturnType().equals(void.class)) {
                throw new IllegalArgumentException(name + "固定频率任务，方法返回值类型必须是void");
            }
            if (isDynamicConcurrent) {
                addDynamicTask(isShardedDynamicConcurrent, name, scheduled, bean, method, TaskType.FIXED_RATE, dynamicConcurrentComputeDelay, computer);
            } else {
                for (int i = 0; i < fixedConcurrent; i++) {
                    TaskItem item = executor.genFixedRateTask(scheduled.only(), name, SuperExecutor.DEFAULT_SHARDED, bean, method, scheduled.fixedRate(), initialDelay);
                    tempWaitTasks.add(item);
                    if (log.isDebugEnabled()) {
                        log.debug("加载" + TaskType.FIXED_RATE.toString() + "任务" + item.getName());
                    }
                }
            }
        }

        if (scheduled.dynamicDelay()) {
            if (!method.getReturnType().equals(long.class)) {
                throw new IllegalArgumentException(name + "动态执行时间任务，方法返回值类型必须是long");
            }
            if (isDynamicConcurrent) {
                addDynamicTask(isShardedDynamicConcurrent, name, scheduled, bean, method, TaskType.DYNAMIC_DELAY, dynamicConcurrentComputeDelay, computer);
            } else {
                for (int i = 0; i < fixedConcurrent; i++) {
                    TaskItem item = executor.genDynamicDelayTask(scheduled.only(), name, SuperExecutor.DEFAULT_SHARDED, bean, method, initialDelay);
                    tempWaitTasks.add(item);
                    if (log.isDebugEnabled()) {
                        log.debug("加载" + TaskType.DYNAMIC_DELAY.toString() + "任务" + item.getName());
                    }
                }
            }
        }
    }

    /**
     * 增加动态并发调配任务
     *
     * @param isShardedDynamicConcurrent    是否分片并发
     * @param name                          任务名
     * @param scheduled                     定义
     * @param bean                          对象
     * @param method                        方法
     * @param type                          任务类型
     * @param dynamicConcurrentComputeDelay 动态并发计算延时
     * @param computer                      动态并发计算器
     */
    private void addDynamicTask(boolean isShardedDynamicConcurrent, String name, SuperScheduled scheduled, Object bean, Method method, TaskType type, long dynamicConcurrentComputeDelay, Object computer) {
        ComputeConcurrentExecutor task;
        if (isShardedDynamicConcurrent) {
            task = new ComputeConcurrentExecutor(executor, name, scheduled, bean, method, type, (ShardedConcurrentComputer) computer);
        } else {
            task = new ComputeConcurrentExecutor(executor, name, scheduled, bean, method, type, (ConcurrentComputer) computer);
        }
        TaskItem item = executor.genFixedDelayTask(false, name, SuperExecutor.DYNAMIC_SHARDED, task, ComputeConcurrentExecutor.getLogicMethod(), dynamicConcurrentComputeDelay, 0L);
        tempWaitTasks.add(item);
        if (log.isDebugEnabled()) {
            log.debug("加载动态调配并发" + type.toString() + "任务" + name);
        }
    }

    /**
     * 4. spring容器启动后，加载但节点锁定线程池和任务、执行所有任务
     */
    @Override
    public void run(ApplicationArguments args) {
        if (isOnlyEnable) {
            onlyLockScheduler = new ThreadPoolTaskScheduler();
            onlyLockScheduler.setPoolSize(1);
            onlyLockScheduler.setThreadNamePrefix(threadNamePrefix + "_lock_only_");
            onlyLockScheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
            onlyLockScheduler.initialize();
            GetLockTask task = new GetLockTask(redis, onlyLockName, executor);
            onlyLockScheduler.scheduleWithFixedDelay(task, 5000L);
        }
        tempWaitTasks.forEach(task -> executor.scheduleTask(task));
        tempWaitTasks.clear();
    }

    /**
     * 5. 加载单元销毁后，停止并销毁所有任务、业务线程池、锁定线程池、业务线程池容器
     */
    @Override
    public void destroy() {
        executor.destroy();
        if (isOnlyEnable && onlyLockScheduler != null) {
            onlyLockScheduler.shutdown();
        }
        businessScheduler.shutdown();
    }

}
