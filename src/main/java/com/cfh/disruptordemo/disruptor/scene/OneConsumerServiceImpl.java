package com.cfh.disruptordemo.disruptor.scene;

import com.cfh.disruptordemo.disruptor.common.*;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * description: 100个事件，单个消费者消费
 * 上述代码有以下几处需要注意：
 * 自己创建环形队列RingBuffer实例
 * 自己准备线程池，里面的线程用来获取和消费消息
 * 自己动手创建BatchEventProcessor实例，并把事件处理类传入
 * 通过ringBuffer创建sequenceBarrier，传给BatchEventProcessor实例使用
 * 将BatchEventProcessor的sequence传给ringBuffer，确保ringBuffer的生产和消费不会出现混乱
 * 启动线程池，意味着BatchEventProcessor实例在一个独立线程中不断的从ringBuffer中获取事件并消费；
 * date: 2022/5/9 16:43
 * author: fenghua.cai
 */
@Service("oneConsumer")
@Slf4j
public class OneConsumerServiceImpl implements LowLevelOperateService {
    private RingBuffer<StringEvent> ringBuffer;
    private StringEventProducer producer;
    /**
     * 统计消息总数
     */
    private final AtomicLong eventCount = new AtomicLong();

    private ExecutorService executors;

    @PostConstruct
    private void init(){
        // 准备一个匿名类，传给disruptor的事件处理类，
        // 这样每次处理事件时，都会将已经处理事件的总数打印出来
        Consumer<?> eventCountPrinter = new Consumer<Object>() {
            @Override
            public void accept(Object o) {
                long count = eventCount.incrementAndGet();
                log.info("receive [{}] event", count);
            }
        };

        // 创建环形缓冲区示例
        ringBuffer = RingBuffer.createSingleProducer(new StringEventFactory(), BUFFER_SIZE);
        // 线程池
        executors = Executors.newFixedThreadPool(1);
        //创建SequenceBarrier
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        // 创建事件处理的工作类，里面执行StringEventHandler处理事件
        BatchEventProcessor<StringEvent> batchEventProcessor = new BatchEventProcessor<>(ringBuffer, sequenceBarrier, new StringEventHandler(eventCountPrinter));
        // 将消费者的sequence传递给环形队列
        ringBuffer.addGatingSequences(batchEventProcessor.getSequence());

        // 在一个独立线程钟取事件并消费
        executors.submit(batchEventProcessor);

        // 生产者
        producer = new StringEventProducer(ringBuffer);
    }

    @Override
    public void publish(String value) {
        producer.onData(value);
    }

    @Override
    public long eventCount() {
        return eventCount.get();
    }
}
