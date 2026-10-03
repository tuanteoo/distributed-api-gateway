package com.api_gateway.Filter;

import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@RequiredArgsConstructor
public class DistributedRateLimiterFilter implements GlobalFilter, Ordered {
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final RedisScript<Long> rateLimiterScript;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String clientIp = exchange.getRequest().getRemoteAddress().getAddress().getHostAddress();
        String redisKey = "rate_limit:ip:" + clientIp;

        String capacity = "5";
        String refillRate = "1";
        String requestedTokens = "1";

        return redisTemplate.execute(
                rateLimiterScript,
                List.of(redisKey),
                List.of(capacity, refillRate, requestedTokens)
        ).next().flatMap(result -> {
            if (result != null && result == 1L) {
                return chain.filter(exchange);
            } else {
                exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
                exchange.getResponse().getHeaders().add("X-RateLimit-Retry-After", "1");
                return exchange.getResponse().setComplete();
            }
        });
    }

    @Override
    public int getOrder() {
        return -1;
    }
}
