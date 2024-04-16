package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.RedisData;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    /**
     * 根据ID查询商铺信息。
     * 先尝试从Redis缓存中查询，如果缓存存在则直接返回缓存数据；
     * 如果缓存不存在，则从数据库中查询，查询到后将结果写入Redis缓存，并返回该数据。
     *
     * @param id 商铺的ID
     * @return 返回查询到的商铺信息，如果不存在则返回错误信息。
     */
    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //用互斥锁解决缓存击穿
        //Shop shop = queryWithMutex(id);
        //逻辑过期解决缓存击穿
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 10L, TimeUnit.SECONDS);
        if (shop == null) {
            return Result.fail("店铺不存在！！");
        }
        //返回
        return Result.ok(shop);
    }

    //这里需要声明一个线程池，因为下面我们需要新建一个现成来完成重构缓存
    //private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    /*public Shop queryWithLogicalExpire(Long id) {
        //1. 从redis中查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);
        //2. 如果未命中，则返回空
        if (StrUtil.isBlank(json)) {
            return null;
        }
        //3. 命中，将json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        //3.1 将data转为Shop对象
        JSONObject shopJson = (JSONObject) redisData.getData();
        Shop shop = JSONUtil.toBean(shopJson, Shop.class);
        //3.2 获取过期时间
        LocalDateTime expireTime = redisData.getExpireTime();
        //4. 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 5.1.未过期，直接返回店铺信息
            return shop;
        }
        //6. 过期，尝试获取互斥锁
        boolean isLock = tryLock(LOCK_SHOP_KEY + id);
        //7. 获取到了锁
        if (isLock) {
            //8. 开启独立线程
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    this.saveShop2Redis(id, LOCK_SHOP_TTL);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(LOCK_SHOP_KEY + id);
                }
            });
            //9. 直接返回商铺信息
            return shop;
        }
        //10. 未获取到锁，直接返回商铺信息
        return shop;
    }

    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1. 从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
            //3. 存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //判断命中的是否是空值
        if (shopJson != null){
            //返回错误信息
            return null;
        }

        //4. 开始实现缓存重建
        //4.1 获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //4.2 判断是否获取成功
            if (!isLock){
                //4.3 失败，则休眠并重试
                Thread.sleep(50);
                return queryWithMutex(id);

            }
            //TODO 这里应该再次检查redis缓存是否存在，做DoubleCheck，如果存在则无需重建缓存

            //4.4 成功，根据id查询数据库
            shop = getById(id);
            //5. 不存在，返回错误
            if (shop == null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6. 存在，写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //7. 释放互斥锁
            unlock(lockKey);
        }
        //8. 返回
        return shop;
    }

    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1. 从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
            //3. 存在，直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }

        //判断命中的是否是空值
        if (shopJson != null){
            //返回错误信息
            return null;
        }
        //4. 不存在，根据id查询数据库
        Shop shop = getById(id);
        //5. 不存在，返回错误
        if (shop == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6. 存在，写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //7. 返回
        return shop;
    }
*/
    /*
    private boolean tryLock(String key) {
        // 使用Redis的SETNX命令尝试设置key的值，如果key不存在，则设置成功并返回true，否则返回false。
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 判断设置结果，如果设置成功（即锁获取成功），返回true，否则返回false。
        return BooleanUtil.isTrue(flag);
    }

    /*
    private void unlock(String key) {
        // 直接删除key来释放锁。
        stringRedisTemplate.delete(key);
    }*/


    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺ID不能为空");
        }
        //1. 更新数据库
        updateById(shop);
        //2. 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    /*public void saveShop2Redis(Long id, Long expireSeconds){
        Shop shop = getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }*/
}

