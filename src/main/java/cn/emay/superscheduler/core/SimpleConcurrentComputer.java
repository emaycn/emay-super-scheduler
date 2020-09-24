package cn.emay.superscheduler.core;


import cn.emay.superscheduler.SuperExecutor;

import java.util.HashMap;
import java.util.Map;

/**
 * 简单并发计算器
 */
public interface SimpleConcurrentComputer extends ConcurrentComputer {

    /**
     * 计算任务所需并发数
     *
     * @param concurrent 当前并发量
     * @return 任务所需并发数
     */
    int compute(int concurrent);

    /**
     * 将计算好的任务并发数放入默认分片
     *
     * @param concurrent <分片,并发数> 当前分片并发数
     * @return 所需分片并发数 <分片,并发数>
     */
    default Map<String, Integer> compute(Map<String, Integer> concurrent) {
        int now = concurrent.getOrDefault(SuperExecutor.DEFAULT_SHARDED, 0);
        int needNumber = this.compute(now);
        Map<String, Integer> need = new HashMap<>();
        need.put(SuperExecutor.DEFAULT_SHARDED, Math.max(needNumber, 0));
        return need;
    }

}
