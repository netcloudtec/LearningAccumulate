package com.netcloudai.bigdata.api.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ysj
 * @version 1.0
 * @date 2021/8/9 00:27
 * 传感器温度读数的数据类型
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorReading {
    // 属性：id，时间戳，温度值
    private String id;
    private Long timestamp;
    private Double temperature;
}
