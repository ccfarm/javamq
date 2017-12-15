package pku;
import java.util.*;
import java.io.*;

/**
 * Created by yangxiao on 2017/11/14.
 */
public class Producer {
	static HashMap<String, Integer> numOfTopic = new HashMap<>();
	static HashMap<String, String> nameOfFile = new HashMap<>();
    
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
    	
    	int index = 1;
    	if (!nameOfFile.containsKey(topic)) {
	    	synchronized (numOfTopic) {
				if (!numOfTopic.containsKey(topic)) {
					numOfTopic.put(topic, 1);
					index = 1;
				} else {
					index = numOfTopic.get(topic) + 1;
					numOfTopic.put(topic, index);
				}
			}
    	}
    	nameOfFile.put(topic, topic + index);
    	RandomAccessFile output = new RandomAccessFile(nameOfFile.get(topic), "rw");
    	
    	output.seek(output.length());
    	
    	int v1 = 0;
		long v2 = 0;
		String v3 = null;
		v1 = defaultMessage.headers().getInt(MessageHeader.MESSAGE_ID);
		if (v1 != 0) {
			output.writeChars(MessageHeader.MESSAGE_ID + "\n");
			output.writeInt(v1);
			output.writeChar('\n');
		}
		v3 = defaultMessage.headers().getString(MessageHeader.TOPIC);
		if (v3 != null) {
			output.writeChars(MessageHeader.TOPIC + "\n"+ v3 + "\n");
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.BORN_TIMESTAMP);
		if (v2 != 0) {
			output.writeChars(MessageHeader.BORN_TIMESTAMP + "\n");
			output.writeLong(v2);
			output.writeChar('\n');
		}
		v3 = defaultMessage.headers().getString(MessageHeader.BORN_HOST);
		if (v3 != null) {
			output.writeChars(MessageHeader.BORN_HOST + "\n"+ v3 + "\n");
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.STORE_TIMESTAMP);
		if (v2 != 0L) {
			output.writeChars(MessageHeader.STORE_TIMESTAMP + "\n");
			output.writeLong(v2);
			output.writeChar('\n');
		}
		v3 = defaultMessage.headers().getString(MessageHeader.STORE_HOST);
		if (v3 != null) {
			output.writeChars(MessageHeader.STORE_HOST + "\n"+ v3 + "\n");
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.START_TIME);
		if (v2 != 0L) {
			output.writeChars(MessageHeader.START_TIME + "\n");
			output.writeLong(v2);
			output.writeChar('\n');
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.STOP_TIME);
		if (v2 != 0L) {
			output.writeChars(MessageHeader.STOP_TIME + "\n");
			output.writeLong(v2);
			output.writeChar('\n');
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.TIMEOUT);
		if (v2 != 0L) {
			output.writeChars(MessageHeader.TIMEOUT + "\n");
			output.writeLong(v2);
			output.writeChar('\n');
		}
		v1 = defaultMessage.headers().getInt(MessageHeader.PRIORITY);
		if (v1 != 0) {
			output.writeChars(MessageHeader.PRIORITY + "\n");
			output.writeInt(v1);
			output.writeChar('\n');
		}
		v3 = defaultMessage.headers().getString(MessageHeader.RELIABILITY);
		if (v3 != null) {
			output.writeChars(MessageHeader.RELIABILITY + "\n"+ v3 + "\n");
		}
		v3 = defaultMessage.headers().getString(MessageHeader.SEARCH_KEY);
		if (v3 != null) {
			output.writeChars(MessageHeader.SEARCH_KEY + "\n"+ v3 + "\n");
		}
		v3 = defaultMessage.headers().getString(MessageHeader.SCHEDULE_EXPRESSION);
		if (v3 != null) {
			output.writeChars(MessageHeader.SCHEDULE_EXPRESSION + "\n"+ v3 + "\n");
		}
		v3 = defaultMessage.headers().getString(MessageHeader.SHARDING_KEY);
		if (v3 != null) {
			output.writeChars(MessageHeader.SHARDING_KEY + "\n"+ v3 + "\n");
		}
		v3 = defaultMessage.headers().getString(MessageHeader.SHARDING_PARTITION);
		if (v3 != null) {
			output.writeChars(MessageHeader.SHARDING_PARTITION + "\n"+ v3 + "\n");
		}
		v3 = defaultMessage.headers().getString(MessageHeader.TRACE_ID);
		if (v3 != null) {
			output.writeChars(MessageHeader.TRACE_ID + "\n"+ v3 + "\n");
		}
		output.writeChar('\r');
		output.writeInt(defaultMessage.getBody().length);
		output.write(defaultMessage.getBody());
		output.close();
    }
    
    public void flush() throws Exception{
    	
        System.out.println(1);
    }
}