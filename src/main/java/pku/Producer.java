package pku;
import java.util.*;
import java.io.*;

/**
 * Created by yangxiao on 2017/11/14.
 */
public class Producer {
	
	static HashMap<String, Integer> numOfTopic = new HashMap<>();
	HashMap<String, MessageStore> bufs = new HashMap<>();
    
	
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body)throws Exception{
        ByteMessage msg = new DefaultMessage(body);
    	msg.putHeaders(MessageHeader.TOPIC, topic);
        return msg;
    }
    
    
    public void send(ByteMessage defaultMessage)throws Exception{
    	if (defaultMessage == null) {
    		return;
    	}
    	
    	String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
    	
    	if (!bufs.containsKey(topic)) {
    		int index;
	    	synchronized (numOfTopic) {
				if (!numOfTopic.containsKey(topic)) {
					numOfTopic.put(topic, 0);
					index = 0;
				} else {
					index = numOfTopic.get(topic) + 1;
					numOfTopic.put(topic, index);
				}
			}
	    	bufs.put(topic, new MessageStore(index + topic));
    	}
    	
    	bufs.get(topic).push(defaultMessage);
    }
    
    public void flush() throws Exception{
    	Iterator iter = bufs.entrySet().iterator();
    	while (iter.hasNext()) {
    		Map.Entry<String, MessageStore> entry = (Map.Entry<String, MessageStore>) iter.next();
    		entry.getValue().write();
    	}
        System.out.println(1);
    }
    
    
}