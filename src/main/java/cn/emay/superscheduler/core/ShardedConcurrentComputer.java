package cn.emay.superscheduler.core;


import cn.emay.superscheduler.base.SuperComputer;

import java.util.Map;

/**
 * 分片任务并发计算器<br/>
 * 计算每个分片所需并发数
 */
public interface ShardedConcurrentComputer extends SuperComputer {

    /**
     * 计算每个分片所需并发数
     *
     * @param concurrent <分片,并发数> 当前分片并发数
     * @return <分片,并发数>
     */
    Map<String, Integer> compute(Map<String, Integer> concurrent);


}
