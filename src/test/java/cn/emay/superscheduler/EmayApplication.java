package cn.emay.superscheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * 启动入口类
 *
 * @author Frank
 */
@SpringBootApplication(scanBasePackages = "cn.emay")
@EnableConfigurationProperties
public class EmayApplication {

    public static void main(String[] args) {
        SpringApplication.run(EmayApplication.class, args);
    }

}
