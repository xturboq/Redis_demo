package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

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
        //Shop shop = queryWithPassThrough(id);
        //用互斥锁解决缓存击穿

        Shop shop = queryWithMutex(id);
        //7. 返回
        return Result.ok(shop);
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

        //开始实现缓存重建
        //4.1 获取互斥锁
        String lockkey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockkey);
        //4.2 判断是否获取成功
        if (!isLock){
            //4.3 失败，则休眠并重试
            try {
                Thread.sleep(50);
                return queryWithMutex(id);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //TODO 这里应该再次检查redis缓存是否存在，做DoubleCheck，如果存在则无语重建缓存
        //4.4 成功，根据id查询数据库

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

    /**
     * 尝试获取锁。使用Redis的SETNX命令来实现锁的获取，如果key不存在，则设置key的值为"1"，并设置过期时间为10秒，表示获取锁成功；如果key已存在，表示获取锁失败。
     * @param key 锁的key，对应Redis中存储的键。
     * @return 返回一个布尔值，表示是否成功获取锁。true表示成功，false表示失败。
     */
    private boolean tryLock(String key) {
        // 使用Redis的SETNX命令尝试设置key的值，如果key不存在，则设置成功并返回true，否则返回false。
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        // 判断设置结果，如果设置成功（即锁获取成功），返回true，否则返回false。
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 解锁。直接删除Redis中对应的key，来释放锁。
     * @param key 锁的key，对应Redis中存储的键。
     */
    private void unlock(String key) {
        // 直接删除key来释放锁。
        stringRedisTemplate.delete(key);
    }


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
}
