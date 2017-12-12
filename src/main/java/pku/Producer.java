package pku;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;
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
    	ExecutorService executor = Executors.newCachedThreadPool();
    	for (Iterator iter = store.entrySet().iterator(); iter.hasNext();) {
    		Map.Entry<String, ArrayList<ByteMessage>> entry = (Map.Entry<String, ArrayList<ByteMessage>>)iter.next();
    		String topic = entry.getKey();
    		ArrayList<ByteMessage> val = entry.getValue();
    		executor.execute(new SaveTopic(topic, val));
    	}
    	executor.shutdown();
        System.out.println(1);
    }
    
    class SaveTopic implements Runnable{
    	String topic;
    	ArrayList<ByteMessage> val;
    	public SaveTopic(String s, ArrayList<ByteMessage> t) {
    		topic = s;
    		val = t;
    	}
    	@Override
    	public void run(){
    		try {
    		
    		File file = new File(topic);
    		PrintWriter output = new PrintWriter(file);
    		for (Iterator iter2 = val.iterator(); iter2.hasNext();) {
    			ByteMessage msg = (DefaultMessage)iter2.next();
    			int v1 = 0;
    			long v2 = 0;
    			String v3 = null;
    			v3 = msg.headers().getString(MessageHeader.MESSAGE_ID);
    			if (v3 != null) {
    				output.println(MessageHeader.MESSAGE_ID + " "+ v3);
    			}
    			v3 = msg.headers().getString(MessageHeader.TOPIC);
    			if (v3 != null) {
    				output.println(MessageHeader.TOPIC + " "+ v3);
    			}
    			v3 = msg.headers().getString(MessageHeader.BORN_TIMESTAMP);
    			if (v3 != null) {
    				output.println(MessageHeader.BORN_TIMESTAMP + " "+ v3);
    			}
    			v3 = msg.headers().getString(MessageHeader.BORN_HOST);
    			if (v3 != null) {
    				output.println(MessageHeader.BORN_HOST + " "+ v3);
    			}
    			v2 = msg.headers().getLong(MessageHeader.STORE_TIMESTAMP);
    			if (v2 != 0L) {
    				output.println(MessageHeader.STORE_TIMESTAMP + " "+ v2);
    			}
    			v3 = msg.headers().getString(MessageHeader.STORE_HOST);
    			if (v3 != null) {
    				output.println(MessageHeader.STORE_HOST + " "+ v3);
    			}
    			v2 = msg.headers().getLong(MessageHeader.START_TIME);
    			if (v2 != 0L) {
    				output.println(MessageHeader.START_TIME + " "+ v2);
    			}
    			v2 = msg.headers().getLong(MessageHeader.STOP_TIME);
    			if (v2 != 0L) {
    				output.println(MessageHeader.STOP_TIME + " "+ v2);
    			}
    			v2 = msg.headers().getLong(MessageHeader.TIMEOUT);
    			if (v2 != 0L) {
    				output.println(MessageHeader.TIMEOUT + " "+ v2);
    			}
    			v1 = msg.headers().getInt(MessageHeader.PRIORITY);
    			if (v1 != 0) {
    				output.println(MessageHeader.PRIORITY + " "+ v1);
    			}
    			v3 = msg.headers().getString(MessageHeader.RELIABILITY);
    			if (v3 != null) {
    				output.println(MessageHeader.RELIABILITY + " "+ v3);
    			}
    			v3 = msg.headers().getString(MessageHeader.SEARCH_KEY);
    			if (v3 != null) {
    				output.println(MessageHeader.SEARCH_KEY + " "+ v3);
    			}
    			v3 = msg.headers().getString(MessageHeader.SCHEDULE_EXPRESSION);
    			if (v3 != null) {
    				output.println(MessageHeader.SCHEDULE_EXPRESSION + " "+ v3);
    			}
    			v3 = msg.headers().getString(MessageHeader.SHARDING_KEY);
    			if (v3 != null) {
    				output.println(MessageHeader.SHARDING_KEY + " "+ v3);
    			}
    			v3 = msg.headers().getString(MessageHeader.SHARDING_PARTITION);
    			if (v3 != null) {
    				output.println(MessageHeader.SHARDING_PARTITION + " "+ v3);
    			}
    			v3 = msg.headers().getString(MessageHeader.TRACE_ID);
    			if (v3 != null) {
    				output.println(MessageHeader.TRACE_ID + " "+ v3);
    			}
    			output.println("body");
    			output.println(msg.getBody().length);
    			for (int i = 0; i < msg.getBody().length; i++) {
    				output.print(" " + msg.getBody()[i]);
    			}
    			output.println();
    			output.close();
    		}//for
    		
    		}//try
    		catch (Exception e) {
    		}//catch
    		
    	}//run
    }
}
