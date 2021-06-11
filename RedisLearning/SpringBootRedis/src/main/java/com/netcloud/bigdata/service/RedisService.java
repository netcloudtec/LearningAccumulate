package com.netcloud.bigdata.service;

import com.netcloud.bigdata.domain.UserVo;
import com.netcloud.bigdata.mapper.TaskMapper;
import com.netcloud.bigdata.until.RedisUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;


/**
 * redis key值操作
 *
 * @author ysj
 * @version 1.0
 * @date 2021/4/21 15:34
 */
@Service
public class RedisService {
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    @Autowired
    private TaskMapper taskMapper;

    public String searchTaskById(String userId) {
        Object user = redisTemplate.opsForValue().get(userId);
        if (user != null) {
            return (String) user;
        }
        String userName = taskMapper.selectByPrimaryKey(userId);
        //数据库查询不为空，将值放入缓存
        if (userName != null) {
            redisTemplate.opsForValue().set(String.valueOf(userId), userName, 60, TimeUnit.MINUTES);
        } else {
            //数据库查询为空，存储一个空值缓存60s
            redisTemplate.opsForValue().set(String.valueOf(userId), null, 60, TimeUnit.SECONDS);
        }
        return userName;
    }




}
