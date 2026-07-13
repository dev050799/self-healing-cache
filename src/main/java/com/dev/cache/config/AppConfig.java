package com.dev.cache.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class AppConfig {

    @Bean
    public RestClient internodeRestClient(CacheProperties properties) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(properties.getClient().getConnectTimeoutMs());
        factory.setReadTimeout(properties.getClient().getRequestTimeoutMs());
        return RestClient.builder().requestFactory(factory).build();
    }
}
