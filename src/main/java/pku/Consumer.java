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
		while (input.hasNext()) {
			int size = input.nextInt();
			byte[] body = new byte[size];
			for (int i = 0; i < size; i++) {
				body[i] = input.nextByte();
			};
			ByteMessage msg = new DefaultMessage(body);
			msg.putHeaders(MessageHeader.TOPIC, topic);
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
