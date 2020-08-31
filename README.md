# 基于Spring的Super Scheduler

扩展spring的Scheduled功能，支持：

1. 并发；
2. 根据返回值动态delay；
3. 动态调整并发(以及分片并发)数；

````java

package cn.emay.superscheduler;


import cn.emay.json.JsonHelper;
import cn.emay.superscheduler.core.ConcurrentComputer;
import cn.emay.superscheduler.core.ShardedConcurrentComputer;
import cn.emay.superscheduler.core.SuperScheduled;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 配置文件中<br/>
 * scheduler.redisBeanName redis spring 注册名<br/>
 * scheduler.poolSize 核心线程数<br/>
 * scheduler.threadNamePrefix 线程名前缀<br/>
 * scheduler.awaitTerminationSeconds 停止时等待当前线程业务执行完毕时间<br/>
 * scheduler.onlyLockName 但节点锁的名字
 * <p>
 * 任务类型和并发类型可以任意组合：
 * 任务类型：cron,fixedDelay,fixedRate,dynamicDelay
 * 并发类型：fixedConcurrent,dynamicConcurrent*
 */
@Component
public class SpringTaskTest {

    /**
     * 1. 固定间隔时间执行,并发1,初始延迟1秒执行
     */
    @SuperScheduled(fixedDelay = 1000L, initialDelay = 1000L, only = true, fixedConcurrent = 2)
    public void t1() {
        String now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 开始执行");
        testLongTime(5000L);
        now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 执行完成，1秒后再次执行");
    }

    /**
     * 2. 固定频率执行,并发2
     */
    @SuperScheduled(initialDelay = 1000L,fixedRate = 1000L, fixedConcurrent = 2)
    public void t2() {
        String now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 开始执行");
        testLongTime(3000L);
        now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 执行完成，0秒后再次执行");
    }

    /**
     * 3. cron执行,并发1,集群单节点执行
     */
    @SuperScheduled(cron = "0/3 * * * * ?" , fixedConcurrent = 2)
    public void t3() {
        String now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 开始执行");
        testLongTime(2000L);
        now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 执行完成，1秒后再次执行");
    }

    /**
     * 4. 动态间隔时间执行,并发1
     */
    @SuperScheduled(dynamicDelay = true,fixedConcurrent = 2,only = true)
    public long t4() {
        String now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 开始执行");
        testLongTime(2000L);
        now = toString(new Date(), "HH:mm:ss");
        int next = new Random().nextInt(4);
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 执行完成，" + next + "秒后再次执行");
        return next * 1000L;
    }

    /**
     * 6. 动态间隔时间执行,动态并发<br/>
     */
    @SuperScheduled(dynamicDelay = true, dynamicConcurrentComputeDelay = 10L * 1000L, dynamicConcurrentComputeBean = "t6ComputeBean", dynamicConcurrentMax = 4)
    public long t6() {
        String now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 开始执行");
        now = toString(new Date(), "HH:mm:ss");
        int next = 1;
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 执行完成，" + next + "秒后再次执行");
        return next * 1000L;
    }


    /**
     * 6. 动态调配并发数量
     */
    @Bean("t6ComputeBean")
    public ConcurrentComputer t6ComputeBean() {
        return concurrent -> {
            String now = toString(new Date(), "HH:mm:ss");
            int need = new Random().nextInt(6);
            System.out.println(now + " : " + Thread.currentThread().getName() + " : " + "t6ComputeBean 开始调整并发,当前并发量(" + concurrent + "),期望并发量(" + need + ")");
            return need;
        };
    }

    /**
     * 7. 分片并发<br/>
     *
     * @param sharded 分片
     */
    @SuperScheduled(dynamicDelay = true, dynamicConcurrentComputeDelay = 10000L, dynamicConcurrentComputeBean = "t7ComputeBean", dynamicConcurrentMax = 4)
    public long t7(String sharded) {
        String now = toString(new Date(), "HH:mm:ss");
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 开始执行 by分片 " + sharded);
//        testLongTime(2000L);
        now = toString(new Date(), "HH:mm:ss");
        int next = 2;
        System.out.println(now + " : " + Thread.currentThread().getName() + " : 执行完成，" + next + "秒后再次执行 by分片 " + sharded);
        return next * 1000L;
    }

    static AtomicInteger integer = new AtomicInteger();

    /**
     * 7. 动态调配分片并发数量
     */
    @Bean("t7ComputeBean")
    public ShardedConcurrentComputer t7ComputeBean() {
        return concurrent -> {
            String nowConcurrent = JsonHelper.toJsonString(concurrent);

            Map<String, Integer> need = new HashMap<>();

            int i = integer.addAndGet(1);
            switch (i) {
                case 1:
                    need.put("key1", 1);
                    break;
                case 2:
                    need.put("key2", 1);
                    break;
                case 3:
                    need.put("key3", 1);
                    break;
                case 4:
                    need.put("key4", 1);
                    need.put("key5", 1);
                    break;
                case 5:
                    need.put("key4", 3);
                    need.put("key2", 2);
                    break;
                case 6:
                    need.put("key6", 2);
                    need.put("key1", 3);
                    break;
                default:
                    need.put("key10", 5);
                    break;
            }

            String needConcurrent = JsonHelper.toJsonString(need);

            String now = toString(new Date(), "HH:mm:ss");
            System.out.println(now + " : " + Thread.currentThread().getName() + " : " + "t7ComputeBean 开始调整并发,当前并发量(" + nowConcurrent + "),期望并发量(" + needConcurrent + ")");
            return need;
        };
    }

    /**
     * 模拟长事务<br/>
     */
    private void testLongTime(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException ignored) {

        }
    }

    /**
     * 把日期转成字符串
     */
    public static String toString(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }


}


````