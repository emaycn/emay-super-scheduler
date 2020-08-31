package cn.emay.superscheduler;

import cn.emay.redis.RedisClient;
import cn.emay.redis.impl.RedisClusterClient;
import cn.emay.redis.impl.RedisSentinelClient;
import cn.emay.redis.impl.RedisShardedClient;
import cn.emay.redis.impl.RedisSingleClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

/**
 * Redis组件配置
 *
 * @author Frank
 */
@Configuration
@ConfigurationProperties(prefix = "redis")
@Order(0)
public class RedisConfiguration {

    /**
     * 单点Redis参数
     */
    private SingleInfo single;
    /**
     * 分片Redis参数
     */
    private ShardedInfo sharded;
    /**
     * 集群Redis参数
     */
    private ClusterInfo cluster;
    /**
     * 哨兵Redis参数
     */
    private SentinelInfo sentinelInfo;
    /**
     * 超时时间
     */
    private int timeoutMillis;
    /**
     * 最大空闲线程数
     */
    private int maxIdle;
    /**
     * 最大线程数
     */
    private int maxTotal;
    /**
     * 最小空闲线程数
     */
    private int minIdle;
    /**
     * 最大等待时间
     */
    private long maxWaitMillis;
    /**
     * 日期格式
     */
    private String datePattern;
    /**
     * 密码
     */
    private String password;

    @Bean(name = "RedisClient", destroyMethod = "close")
    public RedisClient redisClient() {
        RedisClient redis = null;
        if (single != null) {
            redis = new RedisSingleClient(single.getHost(), single.getPort(), timeoutMillis, maxIdle, maxTotal, minIdle, maxWaitMillis, datePattern, password);
        } else if (sharded != null) {
            StringBuilder hosts = new StringBuilder();
            for (String host : sharded.getHosts()) {
                hosts.append(host).append(",");
            }
            hosts = new StringBuilder(hosts.substring(0, hosts.length() - 1));
            redis = new RedisShardedClient(hosts.toString(), timeoutMillis, maxIdle, maxTotal, minIdle, maxWaitMillis, datePattern, password);
        } else if (cluster != null) {
            StringBuilder hosts = new StringBuilder();
            for (String host : cluster.getHosts()) {
                hosts.append(host).append(",");
            }
            hosts = new StringBuilder(hosts.substring(0, hosts.length() - 1));
            redis = new RedisClusterClient(hosts.toString(), timeoutMillis, cluster.getMaxRedirections(), maxIdle, maxTotal, minIdle, maxWaitMillis, datePattern,
                    password);
        } else if (sentinelInfo != null) {
            StringBuilder sentinels = new StringBuilder();
            for (String host : sentinelInfo.getSentinels()) {
                sentinels.append(host).append(",");
            }
            sentinels = new StringBuilder(sentinels.substring(0, sentinels.length() - 1));
            redis = new RedisSentinelClient(sentinelInfo.getMasterName(), sentinels.toString(), timeoutMillis, maxIdle, maxTotal, minIdle, maxWaitMillis, datePattern,
                    password);
        }
        return redis;
    }

    public SingleInfo getSingle() {
        return single;
    }

    public void setSingle(SingleInfo single) {
        this.single = single;
    }

    public ShardedInfo getSharded() {
        return sharded;
    }

    public void setSharded(ShardedInfo sharded) {
        this.sharded = sharded;
    }

    public ClusterInfo getCluster() {
        return cluster;
    }

    public void setCluster(ClusterInfo cluster) {
        this.cluster = cluster;
    }

    public int getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(int timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public long getMaxWaitMillis() {
        return maxWaitMillis;
    }

    public void setMaxWaitMillis(long maxWaitMillis) {
        this.maxWaitMillis = maxWaitMillis;
    }

    public String getDatePattern() {
        return datePattern;
    }

    public void setDatePattern(String datePattern) {
        this.datePattern = datePattern;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public SentinelInfo getSentinelInfo() {
        return sentinelInfo;
    }

    public void setSentinelInfo(SentinelInfo sentinelInfo) {
        this.sentinelInfo = sentinelInfo;
    }

    /**
     * 集群参数
     *
     * @author Frank
     */
    public static class ClusterInfo {

        /**
         * 节点地址
         */
        private String[] hosts;

        /**
         * 最大寻址次数
         */
        int maxRedirections = 6;

        public String[] getHosts() {
            return hosts;
        }

        public void setHosts(String[] hosts) {
            this.hosts = hosts;
        }

        public int getMaxRedirections() {
            return maxRedirections;
        }

        public void setMaxRedirections(int maxRedirections) {
            this.maxRedirections = maxRedirections;
        }

    }

    /**
     * 切片参数
     *
     * @author Frank
     */
    public static class ShardedInfo {

        /**
         * 节点地址
         */
        private String[] hosts;

        public String[] getHosts() {
            return hosts;
        }

        public void setHosts(String[] hosts) {
            this.hosts = hosts;
        }
    }

    /**
     * 单点参数
     *
     * @author Frank
     */
    public static class SingleInfo {

        /**
         * 地址
         */
        private String host;

        /**
         * 端口
         */
        private int port;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

    }

    /**
     * 哨兵参数
     *
     * @author frank
     */
    public static class SentinelInfo {

        private String masterName;

        private String[] sentinels;

        public String getMasterName() {
            return masterName;
        }

        public void setMasterName(String masterName) {
            this.masterName = masterName;
        }

        public String[] getSentinels() {
            return sentinels;
        }

        public void setSentinels(String[] sentinels) {
            this.sentinels = sentinels;
        }
    }

}
