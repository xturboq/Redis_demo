package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONNull;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryTypeList() {
        //1.从redis中查询商铺类型缓存
        List<String> shopTypes = stringRedisTemplate.opsForList().range(CACHE_SHOP_TYPE_KEY, 0, -1);
        //2. 判断是否存在
        if (!shopTypes.isEmpty()){
            List<ShopType> tmp = new ArrayList<>();
            //3. 存在，直接返回
            for (String s : shopTypes) {
                ShopType shopType = JSONUtil.toBean(s, ShopType.class);
                tmp.add(shopType);
            }
            return Result.ok(tmp);

        }
        //4. 不存在，查询数据库
        List<ShopType> tmp = query().orderByAsc("sort").list();
        //5. 不存在，返回错误
        if (tmp == null){
            return Result.fail("店铺类型不存在");
        }
        //6. 存在，写入redis
        for (ShopType shopType : tmp){
            String jsonstr = JSONUtil.toJsonStr(shopType);
            shopTypes.add(jsonstr);
        }
        stringRedisTemplate.opsForList().leftPushAll(CACHE_SHOP_TYPE_KEY,shopTypes);

        //7. 返回
        return Result.ok(tmp);
    }
}
