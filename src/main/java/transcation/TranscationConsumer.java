package transcation;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 事务消息消费者
 */
public class TranscationConsumer {
    public static void main(String[] args) throws MQClientException {
        // 实例化消息消费者,指定组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group2");
        // 指定Namesrv地址信息.
        consumer.setNamesrvAddr("39.105.167.25:9876;47.100.79.166:9876");
        // 订阅Topic
        consumer.subscribe("topic4", "*");
        //负载均衡模式消费
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 注册回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt m : msgs) {
                    System.out.println(new String(m.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //启动消息者
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
