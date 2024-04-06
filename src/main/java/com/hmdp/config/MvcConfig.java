package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class MvcConfig implements WebMvcConfigurer {
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new LoginInterceptor()).excludePathPatterns(
                "/user/code",
                "/user/login",
                "/blog/hot",
                "/shop/**",
                "/shop-type/**",
                "/voucher/**",
                "/upload/**",
                "/blog/detail/**",
                "/blog/of/me",
                "/blog/of/follow",
                "/webjars/**",
                "/swagger-resources",
                "/swagger-ui.html",
                "/swagger-ui/**",
                "/webjars/springfox-swagger-ui/**"
        );
    }
}
