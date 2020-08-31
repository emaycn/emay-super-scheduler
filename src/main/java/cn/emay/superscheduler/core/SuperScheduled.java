package cn.emay.superscheduler.core;

import java.lang.annotation.*;

/**
 * 1. 支持动态执行间隔时间<br/>
 * 2. 支持并发以及并发的动态调整<br/>
 * 3. 支持分片并发以及分片并发的动态调整<br/>
 *
 * @author frank
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SuperScheduled {

    /* 通用属性 */

    /**
     * 应用集群部署时，保证此任务仅在一个节点执行<br/>
     * 需要配置redis<br/>
     * 通用属性
     */
    boolean only() default false;

    /**
     * 初始化延时时间，单位毫秒<br/>
     * 任务第一次执行的延迟时间<br/>
     * 最小为0，cron类型任务不生效<br/>
     * 通用属性
     */
    long initialDelay() default 0L;

    /* 任务类型属性 */

    /**
     * cron表达式<br/>
     * 如果不为空，则根据并发控制属性加载为cron任务<br/>
     * 任务类型属性
     */
    String cron() default "";

    /**
     * 固定间隔时间，单位毫秒<br/>
     * 如果大于0，则根据并发控制属性加载为固定间隔时间执行的任务<br/>
     * 任务类型属性
     */
    long fixedDelay() default -1L;

    /**
     * 固定频率时间，单位毫秒<br/>
     * 如果大于0，则根据并发控制属性加载为固定频率时间执行的任务<br/>
     * 任务类型属性
     */
    long fixedRate() default -1L;

    /**
     * 是否开启动态间隔时间<br/>
     * 需要方法返回值为long类型，根据返回值动态调整任务执行间隔时间<br/>
     * 如果开启，则根据并发控制属性加载为动态频率时间执行的任务<br/>
     * 任务类型属性
     */
    boolean dynamicDelay() default false;

    /* 并发控制属性 */

    /**
     * 固定并发数量<br/>
     * 如果大于0，则加载固定数量的任务，并发执行<br/>
     * 并发控制属性，默认为固定1个并发，但是开启动态并发调整此固定并发数量失效
     */
    int fixedConcurrent() default 1;

    /**
     * 动态调整并发数间隔时间，单位毫秒<br/>
     * 如果大于0,则开启动态并发调整<br/>
     * 每间隔此时间，调用dynamicConcurrentComputeBean()计算并调整所需并发数<br/>
     * 并发控制属性
     */
    long dynamicConcurrentComputeDelay() default -1;

    /**
     * 动态计算并发数逻辑实例在spring注册的名称<br/>
     * 支持两种计算逻辑：<br/>
     * ConcurrentComputer ： 普通并发数量计算器<br/>
     * ConcurrentShardedComputer ： 分片并发数量计算器，需要方法接收分片信息，String类型<br/>
     * 并发控制属性
     */
    String dynamicConcurrentComputeBean() default "";

    /**
     * 并发数量最大值,动态调整并发数量不能超过此值<br/>
     * 如果小于等于0，则不限制<br/>
     * 并发控制属性
     */
    int dynamicConcurrentMax() default -1;

}
