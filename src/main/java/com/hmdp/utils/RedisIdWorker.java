package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    private static final long COUNT_BITS = 32L;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String keyPrefix){
        //1. 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;

        //2. 生成序列号
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("inc:"+keyPrefix+":"+date);
        //3. 拼接并返回，简单位运算
        return timestamp << COUNT_BITS | count;
    }
    public static void main(String[] args) {
        //设置一下起始时间，时间戳就是起始时间与当前时间的秒数差
        LocalDateTime tmp = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        System.out.println(tmp.toEpochSecond(ZoneOffset.UTC));
        //结果为1640995200L
    }
}
