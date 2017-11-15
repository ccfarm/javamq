package pku;

/**
 * Created by yangxiao on 2017/11/14.
 */
public class Producer {
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body){
        return null;
    }
    public void send(ByteMessage defaultMessage){

    }
    public void flush(){
        System.out.println(1);
    }
}
