package pku;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

/**
 * Created by yangxiao on 2017/11/14.
 */

public class Consumer {
	static Lock lock = new ReentrantLock();
	ByteBuffer buf;
	List<String> topics = new LinkedList<>();
	String queue = null;
	int readPos;
	int index;
	boolean flag = false;
	//String topic = null;
	//static HashMap<String, Integer> readPosForMap = new HashMap<>();
	//static HashMap<String, ArrayList<ByteMessage>> store = new HashMap<>();
	public Consumer() {
		readPos = 0;
		index = 0;
		buf = ByteBuffer.allocateDirect(Producer.CAPACITY);
	}
	
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
    	if (queue != null) {
    		throw new Exception("This consumer could bond only one queue.");
    	}
    	queue = queueName;
    	topics.addAll(t);
    	//topic = topics.get(0);
    	
    	//System.out.println("topicsize"+topics.size());
    	readBuf();
    }
    
    public ByteMessage poll()throws Exception{
    	if (flag) {
    		return null;
    	}
    	byte key = buf.get();
    	//System.out.println(key);
    	while (key == 17) {
    		if (readBuf()) {
    			return null;
    		}
    		key = buf.get();
    	}
    	ByteMessage re = new DefaultMessage();
    	while (key != 18) {
    		switch (key) {
    		case 1:
    			re.putHeaders(MessageHeader.MESSAGE_ID, buf.getInt());
    			break;
    		case 2:
    			re.putHeaders(MessageHeader.TOPIC, getString());
    			break;
    		case 3:
    			re.putHeaders(MessageHeader.BORN_TIMESTAMP, buf.getLong());
    			break;
    		case 4:
    			re.putHeaders(MessageHeader.BORN_HOST, getString());
    			break;
    		case 5:
    			re.putHeaders(MessageHeader.STORE_TIMESTAMP, buf.getLong());
    			break;
    		case 6:
    			re.putHeaders(MessageHeader.STORE_HOST, getString());
    			break;
    		case 7:
    			re.putHeaders(MessageHeader.START_TIME, buf.getLong());
    			break;
    		case 8:
    			re.putHeaders(MessageHeader.STOP_TIME, buf.getLong());
    			break;
    		case 9:
    			re.putHeaders(MessageHeader.TIMEOUT, buf.getInt());
    			break;
    		case 10:
    			re.putHeaders(MessageHeader.PRIORITY, buf.getInt());
    			break;
    		case 11:
    			re.putHeaders(MessageHeader.RELIABILITY, buf.getInt());
    			break;
    		case 12:
    			re.putHeaders(MessageHeader.SEARCH_KEY, getString());
    			break;
    		case 13:
    			re.putHeaders(MessageHeader.SCHEDULE_EXPRESSION, getString());
    			break;
    		case 14:
    			re.putHeaders(MessageHeader.SHARDING_KEY, buf.getDouble());
    			break;
    		case 15:
    			re.putHeaders(MessageHeader.SHARDING_PARTITION, buf.getDouble());
    			break;
    		case 16:
    			re.putHeaders(MessageHeader.TRACE_ID, getString());
    			break;
    		}//switch
    		key = buf.get();
    	}//while
    	byte[] body = new byte[buf.getInt()];
    	buf.get(body);
    	re.setBody(body);
        return re;
    }
    
    public boolean readBuf() throws Exception {
    	//System.out.println(queue + "queue" + buf.position() + "  buf "+buf.get());
    	//System.out.println(readPos + "topicsize"+topics.size());
    	//if (readPos >= topics.size()) {
    		//flag = true;
			//return true;
		//}
    	File file = new File("data/" + topics.get(readPos) + "+" + index);
    	//System.out.println("data/" + topics.get(readPos) + "+" + index);
    	while (!file.exists()) {
    		index = 0;
    		readPos += 1;
    		if (readPos >= topics.size()) {
    			flag = true;
   				return true;
   			}
   			//topic = topics.get(readPos);   	
    		file = new File("data/" + topics.get(readPos) + "+" + index);
    	}
    	//System.out.println(queue + "queue" + buf.position() + "  buf "+buf.get());
    	byte[] bytes;
    	lock.lock();
    	RandomAccessFile rf = new RandomAccessFile("data/" + topics.get(readPos) + "+" + index, "r");
    	//System.out.println("data/" + topics.get(readPos) + "+" + index);
    	bytes = new byte[rf.readInt()];
    	rf.read(bytes);
    	rf.close();
    	lock.unlock();
    	index++;
    	buf = ByteBuffer.wrap(bytes);
    	
    	//System.out.println("queueName" + queue);
    	return false;
    }//readBuf
    
    public String getString() {
    	int l = buf.getInt();
		byte[] bs =  new byte[l];
		buf.get(bs);
		return new String(bs);
    }//getString
    

}
