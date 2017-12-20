package pku;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;

/**
 * Created by yangxiao on 2017/11/14.
 */
public class Producer {
	
	static HashMap<String, Integer> numOfTopic = new HashMap<>();
	static final int CAPACITY = 150 * 1024 * 1024;
	ByteBuffer buf;
	String currentTopic = null;
	
	public Producer() {
		buf = ByteBuffer.allocateDirect(CAPACITY);
	}
	
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
    	
    	if (currentTopic != topic) {
    		if (currentTopic != null) {
    			//write buffer to file start
    			this.write();
    			//System.out.println(topic + " " + currentTopic);
    			//write buffer to file end
    		}
    		currentTopic = topic;
    		
		    synchronized (numOfTopic) {
				if (!numOfTopic.containsKey(topic)) {
					numOfTopic.put(topic, 0);
				} else {
					numOfTopic.put(topic, numOfTopic.get(topic) + 1);
				}
			}// syn
    	}// if current
    	
    	//send
    	int v1 = 0;
		long v2 = 0;
		String v3 = null;
		double v4 = 0;
		v1 = defaultMessage.headers().getInt(MessageHeader.MESSAGE_ID);
		if (v1 != 0) {
			buf.put((byte)1);
			buf.putInt(v1);
		}
		v3 = defaultMessage.headers().getString(MessageHeader.TOPIC);
		if (v3 != null) {
			buf.put((byte)2);
			putString(v3);
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.BORN_TIMESTAMP);
		if (v2 != 0) {
			buf.put((byte)3);
			buf.putLong(v2);
		}
		v3 = defaultMessage.headers().getString(MessageHeader.BORN_HOST);
		if (v3 != null) {
			buf.put((byte)4);
			putString(v3);
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.STORE_TIMESTAMP);
		if (v2 != 0L) {
			buf.put((byte)5);
			buf.putLong(v2);
		}
		v3 = defaultMessage.headers().getString(MessageHeader.STORE_HOST);
		if (v3 != null) {
			buf.put((byte)6);
			putString(v3);
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.START_TIME);
		if (v2 != 0L) {
			buf.put((byte)7);
			buf.putLong(v2);
		}
		v2 = defaultMessage.headers().getLong(MessageHeader.STOP_TIME);
		if (v2 != 0L) {
			buf.put((byte)8);
			buf.putLong(v2);
		}
		v1 = defaultMessage.headers().getInt(MessageHeader.TIMEOUT);
		if (v1 != 0) {
			buf.put((byte)9);
			buf.putInt(v1);
		}
		v1 = defaultMessage.headers().getInt(MessageHeader.PRIORITY);
		if (v1 != 0) {
			buf.put((byte)10);
			buf.putInt(v1);
		}
		v1 = defaultMessage.headers().getInt(MessageHeader.RELIABILITY);
		if (v1 != 0) {
			buf.put((byte)11);
			buf.putInt(v1);
		}
		v3 = defaultMessage.headers().getString(MessageHeader.SEARCH_KEY);
		if (v3 != null) {
			buf.put((byte)12);
			putString(v3);
		}
		v3 = defaultMessage.headers().getString(MessageHeader.SCHEDULE_EXPRESSION);
		if (v3 != null) {
			buf.put((byte)13);
			putString(v3);
		}
		v4 = defaultMessage.headers().getDouble(MessageHeader.SHARDING_KEY);
		if (v4 != 0.0d) {
			buf.put((byte)14);
			buf.putDouble(v4);
		}
		v4 = defaultMessage.headers().getDouble(MessageHeader.SHARDING_PARTITION);
		if (v4 != 0.0d) {
			buf.put((byte)15);
			buf.putDouble(v4);
		}
		v3 = defaultMessage.headers().getString(MessageHeader.TRACE_ID);
		if (v3 != null) {
			buf.put((byte)16);
			putString(v3);
		}
		buf.put((byte)18);//18 means the start of the body of this message
		buf.putInt(defaultMessage.getBody().length);
		buf.put(defaultMessage.getBody());
		
		if (buf.remaining() <= 201 * 1024) {
			write();
		}
    	//send

    }
    
    public void flush() throws Exception{
    	this.write();
        System.out.println(1);
    }
    
    public void write() throws Exception {
    	//System.out.println(1);
		if (buf.remaining() == CAPACITY) {
			return;
		}
		RandomAccessFile rf = new RandomAccessFile("data/" + currentTopic + "+" + numOfTopic.get(currentTopic), "rw");
		buf.put((byte)17);//17 means the end of this file and to be continue
		byte[] bytes = new byte[CAPACITY];
		buf.position(0);
		buf.get(bytes);
		rf.write(bytes);
		rf.close();
		buf.clear();
		//System.out.println("  "+buf.position());
	}
    
    public void putString(String st) {
    	
    	buf.putInt(st.length());
		buf.put(st.getBytes());
	}
    
}