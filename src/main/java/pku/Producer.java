package pku;
import java.util.*;
import java.io.*;

/**
 * Created by yangxiao on 2017/11/14.
 */
public class Producer {
    static HashMap<String, ArrayList<ByteMessage>> store = new HashMap<>();
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body)throws Exception{
        ByteMessage msg = new DefaultMessage(body);
    	msg.putHeaders(MessageHeader.TOPIC, topic);
        return msg;
    }
    public void send(ByteMessage defaultMessage)throws Exception{
    	String topic = defaultMessage.headers().getString(MessageHeader.TOPIC);
    	Producer.push(defaultMessage, topic);
    }

    public static synchronized void push(ByteMessage msg, String topic) throws Exception{
    	if (msg == null) {
    		return;
    	}
    	if (!store.containsKey(topic)) {
    		store.put(topic, new ArrayList<ByteMessage>());		
    	}
    	store.get(topic).add(msg);  
    	
    }
    public void flush()throws Exception{
    	for (Iterator iter = store.entrySet().iterator(); iter.hasNext();) {
    		Map.Entry<String, ArrayList<ByteMessage>> entry = (Map.Entry<String, ArrayList<ByteMessage>>)iter.next();
    		String topic = entry.getKey();
    		ArrayList<ByteMessage> val = entry.getValue();
    		File file = new File(topic);
    		PrintWriter output = new PrintWriter(file);
    		for (Iterator iter2 = val.iterator(); iter2.hasNext();) {
    			ByteMessage msg = (DefaultMessage)iter2.next();
    			//output.println("hello world!");
    			output.print(msg.getBody().length);
    			for (int i = 0; i < msg.getBody().length; i++) {
    				output.print(" " + msg.getBody()[i]);
    			}
    			output.println();
    			output.close();
    		}
    	}
        System.out.println(1);
    }
}
