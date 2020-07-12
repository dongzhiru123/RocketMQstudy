package filter.sql;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 通过Sql过滤消息的消息生产者用例
 */
public class ProducerBySql {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        //实例化消息生产者
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        //设置NameServerIP与端口
        producer.setNamesrvAddr("39.105.167.25:9876;47.100.79.166:9876");
        //启动生产者
        producer.start();
        for (int i = 0; i < 10; i++) {
            //消息
            Message message = new Message("topic2", "tag1", ("Hello MQ" + i).getBytes());
            //设置消息属性
            message.putUserProperty("i", String.valueOf(i));
            //发送消息
            SendResult sendresult = producer.send(message);
            System.out.println("消息结果 ：" + sendresult);
            TimeUnit.SECONDS.sleep(1);
        }
        //关闭消息生产者
        producer.shutdown();
    }
}
