package base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.TimeUnit;

/**
 * 发送同步消息
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception{
        // 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        // 设置NameServer的地址
        producer.setNamesrvAddr("39.105.167.25:9876;47.100.79.166:9876");
        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 10; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            /**
             * 参数一 ： 消息主题 Topic
             * 参数二 ： 消息Tag
             * 参数三 ： 消息内容
             */
            Message msg = new Message("base" /* Topic */,
                    "Tag1" /* Tag */,
                    ("Hello RocketMQ " + i).getBytes() /* Message body */
            );
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
            TimeUnit.SECONDS.sleep(1);
        }
        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
