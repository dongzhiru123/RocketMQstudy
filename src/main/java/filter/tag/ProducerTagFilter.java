package filter.tag;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 *  通过Tag过滤消息生产者
 */
public class ProducerTagFilter {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //实例化生产者，与组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //设置NmaeServer的IP与端口
        producer.setNamesrvAddr("39.105.167.25:9876;47.100.79.166:9876");
        //启动生产者
        producer.start();
        //发送消息
        for (int i = 0; i < 10; i++) {
            Message message = new Message("topic1", "tag1", new String("Hello !!! " + i).getBytes());
            producer.send(message, new SendCallback() {
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送结果 ： " + sendResult);
                }

                public void onException(Throwable throwable) {
                    throwable.printStackTrace();
                }
            });
            //TimeUnit.SECONDS.sleep(1);
        }
        System.out.println("end---");
    }
}
