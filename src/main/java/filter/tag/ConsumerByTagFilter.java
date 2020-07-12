package filter.tag;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;

import java.util.List;

/**
 * 通过tag过滤消息测试
 */
public class ConsumerByTagFilter {
    public static void main(String[] args) throws MQClientException {
        //实例化消息消费者，指定组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        //设置NameServer的IP端口
        consumer.setNamesrvAddr("39.105.167.25:9876;47.100.79.166:9876");
        //订阅的topic与tag
        consumer.subscribe("topic1", "tag1 || tag2");
        //注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println(new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动consumer
        consumer.start();

        System.out.println("consumer启动：");
    }
}
