local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local redis_time = redis.call('TIME')
local now = tonumber(redis_time[1])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

if not tokens then
    tokens = capacity
    last_refill = now
else
    local time_passed = now - last_refill
    local refill_amount = math.floor(time_passed * refill_rate)

    if refill_amount > 0 then
        tokens = math.min(capacity, tokens + refill_amount)
        last_refill = now
    end
end

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', last_refill)
    redis.call('EXPIRE', key, math.ceil(capacity / refill_rate) * 2)
    return 1 --
else
    return 0 --
end