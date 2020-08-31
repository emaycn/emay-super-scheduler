package cn.emay.superscheduler.core;


import cn.emay.superscheduler.base.SuperComputer;

/**
 * 任务并发计算器<br/>
 * 计算任务所需并发数
 */
public interface ConcurrentComputer extends SuperComputer {

    /**
     * 计算任务所需并发数
     *
     * @param concurrent 当前并发量
     * @return 任务所需并发数
     */
    int compute(int concurrent);

}
