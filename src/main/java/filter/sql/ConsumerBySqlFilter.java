package filter.sql;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * 通过Sql语句过滤消息
 */
public class ConsumerBySqlFilter {
    public static void main(String[] args) throws MQClientException {
        //实例化消息消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //设置NameServerIP与端口
        consumer.setNamesrvAddr("39.105.167.25:9876;47.100.79.166:9876");
        //设置订阅条件
        consumer.subscribe("topic2", MessageSelector.bySql("i > 5"));
        //注册监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : list) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动Consumer
        consumer.start();
        System.out.println("Consumer启动： ");
    }
}
