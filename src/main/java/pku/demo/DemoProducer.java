package pku.demo;


import pku.ByteMessage;
import pku.DefaultMessage;
import pku.MessageHeader;
import pku.Producer;

/**
 * Created by yangxiao on 2017/11/14.
 * 这是一个内存存储的
 */
public class DemoProducer extends Producer {
    @Override
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        ByteMessage msg=new DefaultMessage(body);
        msg.putHeaders(MessageHeader.TOPIC,topic);
        return msg;
    }
    @Override
    public void send(ByteMessage defaultMessage){
        String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
        DemoMessageStore.store.push(defaultMessage,topic);
    }
}
