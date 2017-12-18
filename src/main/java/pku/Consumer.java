package pku;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * Created by yangxiao on 2017/11/14.
 */

public class Consumer {
	ByteBuffer buf = ByteBuffer.allocateDirect(MessageStore.CAPACITY);
	List<String> topics = new LinkedList<>();
	String queue;
	int readPos = 0;
	int index1 = 0;
	int index2 = 0;
	String topic = null;
	//static HashMap<String, Integer> readPosForMap = new HashMap<>();
	//static HashMap<String, ArrayList<ByteMessage>> store = new HashMap<>();
	
	
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
    	if (queue != null) {
    		throw new Exception("This consumer could bond only one queue.");
    	}
    	queue = queueName;
    	topics.addAll(t);
    	topic = topics.get(0);
    	if (readBuf()) {
			return;
		}
    }
    
    public ByteMessage poll()throws Exception{
    	
    	byte key = buf.get();
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
    	File file = new File("data/" + index1 + topic + "+" +index2);
    	
    	while (!file.exists()) {
    		if (index2 != 0) {
    			index1 ++;
    			index2 = 0;
    		} else {
    			index1 = 0;
    			index2 = 0;
    			readPos += 1;
    			if (readPos >= topics.size()) {
    				return true;
    			}
    			topic = topics.get(readPos);
    		}
    		file = new File("data/" + index1 + topic + "+" +index2);
    	}
    	
    	RandomAccessFile rf = new RandomAccessFile("data/" + index1 + topic +"+" +index2, "r");
    	index2++;
    	byte[] bytes = new byte[MessageStore.CAPACITY];
    	rf.read(bytes);
    	rf.close();
    	
    	buf = ByteBuffer.wrap(bytes);
    	
    	//System.out.println(buf.position());
    	return false;
    }//readBuf
    
    public String getString() {
    	StringBuffer sBuf = new StringBuffer();
    	char c = buf.getChar();
    	while (c != '\n') {
    		sBuf.append(c);
    		c = buf.getChar();
    	}
    	return sBuf.toString();
    }//getString
    

}
