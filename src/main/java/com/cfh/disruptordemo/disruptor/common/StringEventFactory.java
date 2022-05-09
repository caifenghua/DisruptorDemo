package com.cfh.disruptordemo.disruptor.common;

import com.lmax.disruptor.EventFactory;

/**
 * description: 事件工厂，定义如何在内存中创建事件对象
 * date: 2022/5/9 16:29
 * author: fenghua.cai
 */
public class StringEventFactory implements EventFactory<StringEvent> {
    @Override
    public StringEvent newInstance() {
        return new StringEvent();
    }
}
