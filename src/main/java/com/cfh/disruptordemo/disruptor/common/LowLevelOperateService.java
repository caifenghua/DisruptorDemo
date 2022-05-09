package com.cfh.disruptordemo.disruptor.common;

/**
 * description: LowLevelOperateService
 * date: 2022/5/9 16:42
 * author: fenghua.cai
 */
public interface LowLevelOperateService {
    /**
     * 消费者数量
     */
    int CONSUMER_NUM = 3;

    /**
     * 环形缓冲区大小
     */
    int BUFFER_SIZE = 16;

    /**
     * 发布一个事件
     * @param value
     * @return
     */
    void publish(String value);

    /**
     * 返回已经处理的任务总数
     * @return
     */
    long eventCount();
}
