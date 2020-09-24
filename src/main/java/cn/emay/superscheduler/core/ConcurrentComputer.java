package cn.emay.superscheduler.core;

import java.util.Map;

/**
 * 并发计算器
 */
public interface ConcurrentComputer {

    /**
     * 计算每个分片所需并发数
     *
     * @param concurrent <分片,并发数> 当前分片并发数
     * @return 所需分片并发数 <分片,并发数>
     */
    Map<String, Integer> compute(Map<String, Integer> concurrent);
}
