package cn.emay.superscheduler.task.lock;

import cn.emay.superscheduler.SuperExecutor;
import cn.emay.superscheduler.core.OnlyLockHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 竞争单点锁任务
 */
public class GetLockTask implements Runnable {

    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * redis
     */
    private final OnlyLockHandler lock;
    /**
     * 当前节点标示
     */
    private final String nodeId;
    /**
     * 锁名称
     */
    private final String lockName;
    /**
     * 线程池
     */
    private final SuperExecutor executor;

    /**
     * @param lock     lock
     * @param lockName 锁名称
     * @param executor 线程池
     */
    public GetLockTask(OnlyLockHandler lock, String lockName, String nodeId, SuperExecutor executor) {
        this.lock = lock;
        this.lockName = lockName;
        this.nodeId = nodeId;
        this.executor = executor;
    }

    @Override
    public void run() {
        if (lock.lock(lockName, nodeId, 60)) {
            executor.setHasLock(true);
            if (log.isDebugEnabled()) {
                log.debug(nodeId + "抢占锁[" + lockName + "]成功");
            }
        } else {
            executor.setHasLock(false);
            if (log.isDebugEnabled()) {
                log.debug(nodeId + "抢占锁[" + lockName + "]失败");
            }
        }
    }
}
