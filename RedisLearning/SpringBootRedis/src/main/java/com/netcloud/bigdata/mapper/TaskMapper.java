package com.netcloud.bigdata.mapper;

import com.netcloud.bigdata.domain.UserVo;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/4/28 10:08
 */
public interface TaskMapper {

    String selectByPrimaryKey(String userId);
}
