package cn.emay.superscheduler.exec;

import cn.emay.redis.RedisClient;
import cn.emay.superscheduler.SuperExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * 竞争单点锁任务
 */
public class GetLockTask implements Runnable {

    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * redis
     */
    private final RedisClient redis;
    /**
     * 当前节点标示
     */
    private final String sign;
    /**
     * 锁名称
     */
    private final String lockName;
    /**
     * 线程池
     */
    private final SuperExecutor executor;

    /**
     * @param redis     redis
     * @param lockName  锁名称
     * @param executor 线程池
     */
    public GetLockTask(RedisClient redis, String lockName, SuperExecutor executor) {
        this.redis = redis;
        this.lockName = lockName;
        this.sign = UUID.randomUUID().toString();
        this.executor = executor;
    }

    @Override
    public void run() {
        String key = "KV_TASK_LOCK_APPLY_" + lockName;
        String getSign = redis.get(key);
        if (getSign == null) {
            boolean isSuccess = redis.setnx(key, sign, 60);
            // 抢占成功 / 失败
            executor.setHasLock(isSuccess);
            if (log.isDebugEnabled()) {
                log.debug("抢占锁[" + lockName + "=" + sign + "]" + (isSuccess ? "成功" : "失败"));
            }
        } else {
            if (getSign.equals(sign)) {
                // 续租
                redis.expire(key, 60);
                if (log.isDebugEnabled()) {
                    log.debug("续租锁[" + lockName + "=" + sign + "]成功");
                }
            } else {
                // 抢占失败
                executor.setHasLock(false);
                if (log.isDebugEnabled()) {
                    log.debug("锁[" + lockName + "=" + getSign + "]被其他节点抢占");
                }
            }
        }
    }
}
