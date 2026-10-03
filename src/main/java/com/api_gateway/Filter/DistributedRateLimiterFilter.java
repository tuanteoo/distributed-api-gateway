package com.api_gateway.Filter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class DistributedRateLimiterFilter implements GlobalFilter, Ordered {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final RedisScript<Long> rateLimiterScript;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String clientIp = exchange.getRequest().getRemoteAddress().getAddress().getHostAddress();
        String path = exchange.getRequest().getURI().getPath();
        String redisKey = "rate_limit:ip:" + clientIp;

        String capacity = "5";
        String refillRate = "1";
        String requestedTokens = "1";

        Mono<Long> redisResult = redisTemplate.execute(
                        rateLimiterScript,
                        List.of(redisKey),
                        List.of(capacity, refillRate, requestedTokens)
                ).next()
                .timeout(Duration.ofMillis(500))
                .onErrorResume(throwable -> {
                    log.error("Redis Rate Limiter Error: {}", throwable.getMessage());

                    String errorMessage = String.format("REDIS FAILURE (FAIL-OPEN) | IP: %s | ERROR: %s", clientIp, throwable.getMessage());
                    kafkaTemplate.send("rate-limit-alerts", "SYSTEM_ALERT", errorMessage);

                    return Mono.just(1L);
                });

        return redisResult.flatMap(result -> {
            if (result == 1L) {
                return chain.filter(exchange);
            } else {
                exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                exchange.getResponse().getHeaders().add("Retry-After", "1");

                String alertMessage = String.format("BLOCKED IP: %s | PATH: %s", clientIp, path);
                kafkaTemplate.send("rate-limit-alerts", clientIp, alertMessage);

                return exchange.getResponse().setComplete();
            }
        });
    }

    @Override
    public int getOrder() {
        return -1;
    }
}
