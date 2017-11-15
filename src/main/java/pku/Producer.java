package pku;

/**
 * Created by yangxiao on 2017/11/14.
 */
public class Producer {
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body)throws Exception{
        return null;
    }
    public void send(ByteMessage defaultMessage)throws Exception{

    }
    public void flush()throws Exception{
        System.out.println(1);
    }
}
