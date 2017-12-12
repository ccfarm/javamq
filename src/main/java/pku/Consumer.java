package pku;

import java.util.*;
import java.io.*;

/**
 * Created by yangxiao on 2017/11/14.
 */

public class Consumer {
	List<String> topics = new LinkedList<>();
	String queue;
	static HashMap<String, ArrayList<ByteMessage>> store = new HashMap<>();
	static int readPos = 0;
	static HashMap<String, Integer> readPosForMap = new HashMap<>();
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
    	if (queue != null) {
    		throw new Exception("This consumer could bond only one queue.");
    	}
    	queue = queueName;
    	topics.addAll(t);
    	for (Iterator<String> iter = t.iterator(); iter.hasNext(); ) {
    		String topic = iter.next();
    		if (!store.containsKey(topic)) {
    			readTopic(topic);
    		}
    	}
    }
    public static synchronized void readTopic(String topic) throws Exception{
    	store.put(topic, new ArrayList<ByteMessage>());
		File file = new File(topic);
		Scanner input = new Scanner(file);
		ByteMessage msg = new DefaultMessage();
		while (input.hasNext()) {
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
			}
			int size = input.nextInt();
			byte[] body = new byte[size];
			for (int i = 0; i < size; i++) {
				body[i] = input.nextByte();
			};
			msg.setBody(body);
			store.get(topic).add(msg);
		}
		input.close();
    }
    public ByteMessage poll()throws Exception{
    	ByteMessage re = null;
    	while (readPos < topics.size()) {
    		re = pull(queue, topics.get(readPos));
    		if (re != null) {
    			break;
    		}
    	}
        return re;
    }
    public static ByteMessage pull(String queue, String topic) {
    	String k = queue + " " + topic;
    	if (!readPosForMap.containsKey(k)) {
    		readPosForMap.put(k, 0);
    	}
    	int pos = readPosForMap.get(k);
    	if (!store.containsKey(topic)) {
    		return null;
    	}
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
