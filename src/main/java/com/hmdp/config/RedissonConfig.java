package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RedissonConfig  
 * @author wendong 
 * @version V1.0
 * @date 2022/05/18 03:19
**/
@Configuration
public class RedissonConfig {
    
    @Bean
    public RedissonClient redissonClient() {
        // 配置
        Config config = new Config();
        config.useClusterServers()
                .addNodeAddress(
                "redis://175.178.242.121:7001", "redis://175.178.242.121:7002", "redis://175.178.242.121:7003", 
                "redis://175.178.242.121:7004", "redis://175.178.242.121:7005", "redis://175.178.242.121:7006"
                )
                .setPassword("Asdzxc2222");

        // 创建 RedissonClient 对象
        return Redisson.create(config);
    }
}
