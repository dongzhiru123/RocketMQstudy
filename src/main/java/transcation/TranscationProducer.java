package transcation;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

/**
 * 事务消息生产者
 */
public class TranscationProducer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        //事务消息生产者
        TransactionMQProducer producer = new TransactionMQProducer("group3");
        //设置NameServerIP与端口
        producer.setNamesrvAddr("39.105.167.25:9876;47.100.79.166:9876");
        //启动producer
        producer.start();
        //创建事务监听器
        TransactionListener listener = new TransactionListener() {
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if ("Tag1".equals(message.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if ("Tag2".equals(message.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else return LocalTransactionState.UNKNOW;
            }

            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println(messageExt.getTags() + "消息回查！");
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        };
        //set事务监听器
        producer.setTransactionListener(listener);
        //Tag 数组
        String[] tags = {"Tag1", "Tag2", "Tag3"};
        //发送消息
        for (int i = 0; i < 3; i++) {
            Message message = new Message("topic4", tags[i], "KEY" + i, ("Hello MQ" + i).getBytes());
            SendResult sendResult = producer.sendMessageInTransaction(message, null);
            System.out.println("发送结果 ：" + sendResult);
            TimeUnit.SECONDS.sleep(2);
        }
        //关闭生产者
        //producer.shutdown();
    }
}
