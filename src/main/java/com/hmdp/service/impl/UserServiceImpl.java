package com.hmdp.service.impl;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import lombok.extern.slf4j.Slf4j;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_CODE_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.如果不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }
        //3.如果符合，生成验证码
        String code = RandomUtil.randomNumbers(6);
        //4.保存验证码到session
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, 2, TimeUnit.MINUTES);
        //5.发送验证码
        log.debug("验证码为：{}", code);
        //6.返回ok
        return Result.ok();
    }

    /**
     * 用户登录接口
     *
     * @param loginForm 登录表单，包含手机号和验证码
     * @param session   用户会话，用于存储和读取验证码、用户信息
     * @return 登录结果，成功返回成功结果，失败返回失败原因
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(loginForm.getPhone())) {
            return Result.fail("手机号格式错误");
        }
        //2.校验验证码
        //Object cacheCode = session.getAttribute("code");
        Object cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if (cacheCode == null || !cacheCode.equals(loginForm.getCode())) {
            return Result.fail("验证码错误");
        }
        //3.根据手机号查询用户
        User user = query().eq("phone", phone).one();
        //4.判断用户是否存在
        if (user == null) {
            //5.不存在，创建新用户并保存
            user = createUserWithPhone(phone);
        }
        //6. 保存用户到Redis中并返回
        //7.1 随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString(true);
        //7.2 将User转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true)
                                    .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        //7.3 储存数据
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        //7.4 设置token有效期
        stringRedisTemplate.expire(tokenKey, 30, TimeUnit.MINUTES);
        //8. 返回token
        //session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        return Result.ok(token);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        //保存用户
        save(user);
        return user;
    }
}
