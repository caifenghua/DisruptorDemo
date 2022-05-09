package com.cfh.disruptordemo.disruptor.scene;

import com.cfh.disruptordemo.disruptor.common.StringEvent;
import com.lmax.disruptor.WorkHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;

/**
 * description: 100个事件，三个消费者共同消费这个100个事件
 * date: 2022/5/9 17:17
 * author: fenghua.cai
 */
@Slf4j
public class StringWorkHandler implements WorkHandler<StringEvent> {

    public StringWorkHandler(Consumer<?> consumer) {
        this.consumer = consumer;
    }

    // 外部可以传入Consumer实现类，每处理一条消息的时候，consumer的accept方法就会被执行一次
    private Consumer<?> consumer;

    @Override
    public void onEvent(StringEvent event) throws Exception {
        log.info("work handler event : {}", event);

        // 这里延时100ms，模拟消费事件的逻辑的耗时
        Thread.sleep(100);

        // 如果外部传入了consumer，就要执行一次accept方法
        if (null!=consumer) {
            consumer.accept(null);
        }
    }
}
