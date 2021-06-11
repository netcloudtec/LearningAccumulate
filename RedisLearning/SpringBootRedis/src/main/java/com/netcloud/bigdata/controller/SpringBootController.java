package com.netcloud.bigdata.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/4/21 15:56
 */
@RestController
public class SpringBootController {
    public static final Logger log = LoggerFactory.getLogger(SpringBootController.class);

//    @Autowired
//    private RedisService redisService;
//
//    @Autowired
//    private RedisTemplate<String, String> redisTemplate;
//
//
//    @RequestMapping(value = "/hello/{id}")
//    public String hello(@PathVariable(value = "id") String id){
//        //1、通过redisTemplate设        redisTemplate.boundValueOps("测试").set("杨少军");置值
////        redisTemplate.boundValueOps("StringKey").set("StringValue",1, TimeUnit.MINUTES);
//        return "";
//    }


}
