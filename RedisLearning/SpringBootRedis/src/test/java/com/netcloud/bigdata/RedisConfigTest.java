package com.netcloud.bigdata;

import com.netcloud.bigdata.service.RedisService;
import com.netcloud.bigdata.until.RedisUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.*;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/4/21 15:37
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisConfigTest {


    @Autowired
    private RedisService redisService;
    @Resource
    private RedisUtil redisUtil;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    @Test
    public void testObj() throws Exception {
//        redisUtil.set("user1", "yangshaojun");
//        redisUtil.set("001", "yangshaojun");
        redisService.searchTaskById("001");

    }

}
