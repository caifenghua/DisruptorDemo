package com.cfh.disruptordemo.disruptor.common;

import com.lmax.disruptor.RingBuffer;

/**
 * description: 事件生产类，定义如何将业务逻辑的事件转为disruptor事件发布到环形队列，用于消费
 * date: 2022/5/9 16:32
 * author: fenghua.cai
 */
public class StringEventProducer {

    // 存储数据的环形缓冲区
    private final RingBuffer<StringEvent> ringBuffer;

    // 构造方法
    public StringEventProducer(RingBuffer<StringEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void onData(String content){

        // ringBuffer是个队列，其next方法返回的是下最后一条记录之后的位置，这是个可用位置
        long sequence = ringBuffer.next();

        // 从sequence取出的事件是空事件
        StringEvent stringEvent = ringBuffer.get(sequence);
        // 空事件添加业务信息
        stringEvent.setValue(content);
        // 发布
        ringBuffer.publish(sequence);
    }
}
