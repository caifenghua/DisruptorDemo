package com.cfh.disruptordemo.disruptor.scene;

import com.cfh.disruptordemo.disruptor.common.LowLevelOperateService;
import com.cfh.disruptordemo.disruptor.common.StringEvent;
import com.cfh.disruptordemo.disruptor.common.StringEventFactory;
import com.cfh.disruptordemo.disruptor.common.StringEventProducer;
import com.lmax.disruptor.IgnoreExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkerPool;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * description: WorkerPoolConsumerServiceImpl
 * date: 2022/5/9 17:19
 * author: fenghua.cai
 */
@Service("workerPoolConsumer")
@Slf4j
public class WorkerPoolConsumerServiceImpl implements LowLevelOperateService {

    private RingBuffer<StringEvent> ringBuffer;

    private StringEventProducer producer;

    /**
     * 统计消息总数
     */
    private final AtomicLong eventCount = new AtomicLong();

    @PostConstruct
    public void init(){
        ringBuffer = RingBuffer.createSingleProducer(new StringEventFactory(), BUFFER_SIZE);

        ExecutorService executorService = Executors.newFixedThreadPool(CONSUMER_NUM);

        StringWorkHandler[] handlers = new StringWorkHandler[CONSUMER_NUM];

        // 创建多个StringWorkHandler实例，放入一个数组中
        for (int i=0;i < CONSUMER_NUM;i++) {
            handlers[i] = new StringWorkHandler(o -> {
                long count = eventCount.incrementAndGet();
                log.info("receive [{}] event", count);
            });
        }

        // 创建WorkerPool实例，将StringWorkHandler实例的数组传进去，代表共同消费者的数量
        WorkerPool<StringEvent> workerPool = new WorkerPool<>(ringBuffer, ringBuffer.newBarrier(), new IgnoreExceptionHandler(), handlers);

        // 这一句很重要，去掉就会出现重复消费同一个事件的问题
        ringBuffer.addGatingSequences(workerPool.getWorkerSequences());

        workerPool.start(executorService);

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
