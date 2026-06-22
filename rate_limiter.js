import Redis from 'ioredis';
const redis = new Redis();

// Fixed Window Distributed Rate Limiter for Production Deployments.
const isRateLimited = async (identifier, limit = 100, windowSeconds = 60) => {
    const windowKey = Math.floor(Date.now() / 1000 / windowSeconds)
    const key = `rate:${identifier}:${windowKey}` // Defining a const unique key for given fixed window instance at a time.

    const pipeline = redis.pipeline();
    // INCR & EXPIRE --> Atomic Locking for Concurrency Control. 
    pipeline.incr(key);
    pipeline.expire(key, windowSeconds);
    const [[, count]] = await pipeline.exec();
    return {
        limited : count > limit,
        count,
        limit,
        remaining : Math.max(0, limit - count)
    }
}

// Token Bucket Distributed Rate Limiter.
const tokenBucketConsume = async(identifier, capacity = 10, refillRate = 1) => {
    const key =  `rate:tokens:${identifier}`
    const now = Date.now() / 1000; // seconds

    for(let attempt = 0 ; attempt < 5; attempt++) {
        // Redis starts mointoring the key. Ensuring no two requests try to consume / update
        // the same token at the same time.
        // Notify me if anybody changes this key before I finish - Optimistic Key

        // Guarantees that no two requests can accidentally consume the same token.
        await redis.watch(key);

        const data = await redis.hgetall(key);
        let tokens = data.tokens ? parseFloat(data.tokens) : capacity;
        let lastRefill = data.last_refill ? parseFloat(data.last_refill) : now;

        // Refill tokens back to the original bucket.
        const elapsed = now - lastRefill;
        tokens = Math.min(capacity, tokens + elapsed * refillRate);

        if(tokens < 1) {
            await redis.unwatch()
            return {
                allowed : false,
                tokens : 0,
                remaining : 0,
                retryAfter : Math.ceil((1 - tokens) / refillRate)
            }
        }
        
        tokens -= 1;

        // Starts a redis transaction and commits at the end after successful update.
        // EXEC --> Redis telling to execute all queued commands together.
        // MULTI --> Queue all the commands, don't execute it yet.
        const results = await redis
        .multi()
        .hset(key,'tokens',tokens.toString(),'last_refill',now)
        .expire(key,Math.ceil(capacity/refillRate) + 60)
        .exec();

        if(results !== null) {
            return {allowed : true, tokens : Math.floor(tokens), remaining : Math.floor(tokens)}
        }

        return { allowed : false ,tokens : 0};
    }
}

// Leaky Bucket Distributed Rate Limiter.
const leakyBucketConsume = async(identifier, capacity = 10, leakRate = 1) => {
    const key =  `rate:tokens:${identifier}`
    const now = Date.now() / 1000; // seconds

    for(let attempt = 0 ; attempt < 5; attempt++) {
        // Redis starts mointoring the key. Ensuring no two requests try to consume / update
        // the same token at the same time.
        // Notify me if anybody changes this key before I finish - Optimistic Key

        // Guarantees that no two requests can accidentally consume the same token.
        await redis.watch(key);

        const data = await redis.hgetall(key);
        let level = data.level ? parseFloat(data.level) : 0;
        let lastLeak = data.lastLeak ? parseFloat(data.lastLeak) : now;

        // Refill tokens back to the original bucket.
        const elapsed = now - lastLeak;
        level = Math.min(0, level - (elapsed * leakRate));

        const allowed = false;
        const remaining = Math.max(0, Math.floor(capacity - level));

        if(level + 1 <= capacity) {
            level += 1;
            remaining = Math.max(0,Math.floor(capacity - level));
            allowed = 1;
        }
        // Starts a redis transaction and commits at the end after successful update.
        // EXEC --> Redis telling to execute all queued commands together.
        // MULTI --> Queue all the commands, don't execute it yet.
        const results = await redis
        .multi()
        .hset(key,'level',level.toString(),'lastLeak', now)
        .expire(key,Math.ceil(capacity/leakRate) + 1)
        .exec();

        if(results !== null) {
            return {allowed : true, remaining : Math.floor(capacity - level)}
        }
        return { allowed : false , remaining : 0};
    }
}


// Sliding Window Log Distributed Rate Limiter
const slidingWindowCheck = async (identifier,limit=100, windowMs = 60000) => {
    const key = `rate:sliding:${identifier}`
    const now = Date.now();
    const windowStart = noew - windowMs;

    const pipeline = redis.pipeline();
    pipeline.zemrangebyscore(key,0,windowStart);
    pipeline.zcard(key);
    pipeline.zadd(key,now,`${now}-${Math.random()}`)
    pipeline.expire(key,Math.ceil(windowMs / 1000) + 1)

    const results = await pipeline.exec();
    const count = results[1][1] + 1;

    return {
        limited : count > limit,
        count,
        limit,
        remaining : Math.max(0,limit-count),
        resetAt : now + windowMs
    }
}

// Sliding window counter distributed rate limiter.
// Hybrid Approach of Fixed Window Counter + Sliding Window Log
const slidingWindowCounter = async(max_requests = 10, window_seconds = 60) => {
    const now = Date.now() / 1000; // seconds
    const currentWindow = Math.floor(now / window_seconds);
    const previousWindow = currentWindow - 1;
    const elapsed = (now % window_seconds) / window_seconds;

    const currentKey = `${`currentKey`}${currentWindow}`
    const previousKey = `${`previousKey`}${previousWindow}`

    const prev_count = Number(await redis.get('previousKey') || '0') || 0;
    const current_count =  Number(await redis.get('currentKey') || '0') || 0;
    
    const weighted_prev = prev_count * (1 - elasped);
    const estimated = weighted_prev + current_count;

    if (estimated >= max_requests) {
        return {
            allowed : false,
            remaining : 0,
            current_count
        }
    }
    
    const pipeline = redis.pipeline();
    const new_count = pipeline.incr(currentKey);

    if(new_count == 1) {
        pipeline.expire(currentKey , window_seconds * 2)
    }

    const new_estimate = weighted_prev * new_count;
    const remaining = Math.max(0, Math.floor(max_requests - new_estimate));

    return {1 , remaining, new_count}
}


export {
    isRateLimited,
    tokenBucketConsume,
    slidingWindowCheck,
    slidingWindowCounter,
    leakyBucketConsume
}