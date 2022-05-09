package com.cfh.disruptordemo.disruptor.common;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * description: 事件类，这是事件的定义：
 * date: 2022/5/9 16:28
 * author: fenghua.cai
 */
@Data
@ToString
@NoArgsConstructor
public class StringEvent {
    private String value;
}
