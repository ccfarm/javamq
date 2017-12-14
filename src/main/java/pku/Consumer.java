package pku;

import java.util.*;
import java.io.*;
import java.util.concurrent.*;

/**
 * Created by yangxiao on 2017/11/14.
 */

public class Consumer {
	static HashSet<String> topicFlag = new HashSet<>();
	List<String> topics = new LinkedList<>();
	String queue;
	static HashMap<String, ArrayList<ByteMessage>> store = new HashMap<>();
	int readPos = 0;
	static HashMap<String, Integer> readPosForMap = new HashMap<>();
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
    	if (queue != null) {
    		throw new Exception("This consumer could bond only one queue.");
    	}
    	queue = queueName;
    	topics.addAll(t);
    	//System.out.println("hello " + queue + topics.get(1));
    	for (Iterator<String> iter = t.iterator(); iter.hasNext(); ) {
    		String topic = iter.next();
    		boolean flag = false;
    		synchronized (store) {
    			if (!store.containsKey(topic)) {
    				store.put(topic, new ArrayList<ByteMessage>());
    				flag = true;
    			}
    		}
			if (flag){
				this.readTopic(topic);
			}
    	}
    }
    public void readTopic(String topic) throws Exception{
    	int index = 0;
    	while (true) {
    	index++;
		File file = new File(topic + index);
		if (!file.exists()) {
			return;
		}
		//System.out.println(topic);
		Scanner input = new Scanner(file);
		while (input.hasNext()) {
			ByteMessage msg = new DefaultMessage();
			String key = input.next();
			while (!key.equals("body")) {
				int v1= 0;
				long v2 = 0l;
				String v3 = null;
				if (key.equals(MessageHeader.MESSAGE_ID)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.TOPIC)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.BORN_TIMESTAMP)) {
					v2 = input.nextLong();
					msg.putHeaders(key, v2);
				} else if (key.equals(MessageHeader.BORN_HOST)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.STORE_TIMESTAMP)) {
					v2 = input.nextLong();
					msg.putHeaders(key, v2);
				} else if (key.equals(MessageHeader.STORE_HOST)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.START_TIME)) {
					v2 = input.nextLong();
					msg.putHeaders(key, v2);
				} else if (key.equals(MessageHeader.STOP_TIME)) {
					v2 = input.nextLong();
					msg.putHeaders(key, v2);
				} else if (key.equals(MessageHeader.TIMEOUT)) {
					v2 = input.nextLong();
					msg.putHeaders(key, v2);
				} else if (key.equals(MessageHeader.PRIORITY)) {
					v1 = input.nextInt();
					msg.putHeaders(key, v1);
				} else if (key.equals(MessageHeader.RELIABILITY)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.SEARCH_KEY)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.SCHEDULE_EXPRESSION)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.SHARDING_KEY)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.SHARDING_PARTITION)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				} else if (key.equals(MessageHeader.TRACE_ID)) {
					v3 = input.next();
					msg.putHeaders(key, v3);
				}
				key = input.next();
				//System.out.println(key);
			}//while
			int size = input.nextInt();
			byte[] body = new byte[size];
			for (int i = 0; i < size; i++) {
				body[i] = input.nextByte();
				//System.out.println(body[i]);
			}
			msg.setBody(body);
			store.get(topic).add(msg);
		}
		input.close();
		//System.out.println("world");
		
    	}//while
    }
    
    public ByteMessage poll()throws Exception{
    	ByteMessage re = null;
    	while (readPos < topics.size()) {
    		re = this.pull(queue, topics.get(readPos));
    		//System.out.println("hello " + queue + topics.get(readPos));
    		if (re != null) {
    			break;
    		} else {
    			readPos += 1;
    		}
    		//System.out.println(readPos);
    	}
        return re;
    }
    
    public synchronized ByteMessage pull(String queue, String topic) {
    	if (!store.containsKey(topic)) {
    		return null;
    	}
    	String k = queue + " " + topic;
    	if (!readPosForMap.containsKey(k)) {
    		readPosForMap.put(k, 0);
    	}
    	int pos = readPosForMap.get(k);
    	//System.out.println(queue + " " + topic + " "+ pos);
    	ArrayList<ByteMessage> list = store.get(topic);
    	if (list.size() <= pos) {
    		return null;
    	} else {
    		ByteMessage msg = list.get(pos);
    		readPosForMap.put(k, pos + 1);
    		return msg;
    	}
    }
}
