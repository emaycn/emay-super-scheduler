package cn.emay.superscheduler.core;

/**
 * 分布式锁控制者
 */
public interface OnlyLockHandler {

    /**
     * 竞争锁<br/>
     * 同一个节点即使得到锁也会不断的继续竞争锁，请注意更新最新的持有锁时间
     *
     * @param onlyLockName 锁名称
     * @param nodeId       节点id
     * @param seconds      保持锁的时间
     * @return 是否竞争成功
     */
    boolean lock(String onlyLockName, String nodeId, int seconds);

    /**
     * 释放锁<br/>
     * 节点关停时触发调用
     *
     * @param onlyLockName 锁名称
     * @param nodeId       节点id
     */
    void unLock(String onlyLockName, String nodeId);

}
