package br.com.fiap.springbatchchunk.config;

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BatchConfig {

    @Bean
    public JobLauncherTestUtils launcherTestUtils() {
        return new JobLauncherTestUtils();
    }
}
