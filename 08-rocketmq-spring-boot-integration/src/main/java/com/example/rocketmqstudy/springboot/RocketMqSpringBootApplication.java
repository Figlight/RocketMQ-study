package com.example.rocketmqstudy.springboot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * RocketMQ 集成 Spring Boot 学习示例入口。
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class RocketMqSpringBootApplication {

    /**
     * 启动 Spring Boot 应用并初始化 RocketMQ 生产者和消费者。
     *
     * @param args Spring Boot 启动参数。
     */
    public static void main(String[] args) {
        SpringApplication.run(RocketMqSpringBootApplication.class, args);
    }
}
