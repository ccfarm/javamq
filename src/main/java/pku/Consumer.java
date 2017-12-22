package pku;

import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.zip.Inflater;

/**
 * Created by yangxiao on 2017/11/14.
 */

public class Consumer {
	ByteBuffer buf = ByteBuffer.allocateDirect(MessageStore.CAPACITY);
	InputStream input;
	List<String> topics = new LinkedList<>();
	String queue;
	int readPos = 0;
	int index = 0;
	//int index2 = 0;
	//String topic = null;
	boolean flag = false;
	//static HashMap<String, Integer> readPosForMap = new HashMap<>();
	//static HashMap<String, ArrayList<ByteMessage>> store = new HashMap<>();
	
	
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
    	if (queue != null) {
    		throw new Exception("This consumer could bond only one queue.");
    	}
    	queue = queueName;
    	topics.addAll(t);
    	//topic = topics.get(0);
    	readFile();
    	readBuf();
    }
    
    public ByteMessage poll()throws Exception{
    	if (flag){
    		return null;
    	}
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
    	//System.out.println(10086);
        return re;
    }
    
    public boolean readBuf() throws Exception {
    	if (flag){
    		return true;
    	}
    	
    	int l = readInt();
    	
    	//System.out.println("hello" + l);
    	
    	while (l == -1) {
    		
    		readFile();
    		
    		if (flag){
        		return true;
        	}
    		l = readInt();
    	}

    	
    	byte[] bytes = new byte[l];
    	input.read(bytes, 0, l);
    	bytes = decompress(bytes);
    	buf = ByteBuffer.wrap(bytes);
    	
    	//System.out.println(buf.position());
    	return false;
    }//readBuf
    
    public String getString() {
    	byte l = buf.get();
		byte[] bs =  new byte[l];
		buf.get(bs);
		return new String(bs);
    }//getString
    
    public int readInt() throws Exception {
    	byte[] b = new byte[4];
    	input.read(b, 0, 4);
    	return b[3] & 0xFF |  
        (b[2] & 0xFF) << 8 |  
        (b[1] & 0xFF) << 16 |  
        (b[0] & 0xFF) << 24;   
    }
    
    public void readFile() throws Exception{
    	if (input != null){
    		input.close();
    	}
    	File file = new File("data/" + index + topics.get(readPos));
    	
    	while (!file.exists()) {
    		index = 0;
    		readPos += 1;
    		if (readPos >= topics.size()) {
    			flag = true;
    			return;
    		}//if
    		//topic = topics.get(readPos);
    		file = new File("data/" + index + topics.get(readPos));
    	}
    	
    	
    	index++;
    	input = new BufferedInputStream(new FileInputStream(file), 10 * 1024 * 1024);
    	//System.out.println("hello world");
    	
    }
    
    public static byte[] decompress(byte[] data) {
        byte[] output = new byte[0];

        Inflater decompresser = new Inflater();
        decompresser.reset();
        decompresser.setInput(data);

        ByteArrayOutputStream o = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[1024];
            while (!decompresser.finished()) {
                int i = decompresser.inflate(buf);
                o.write(buf, 0, i);
            }
            output = o.toByteArray();
        } catch (Exception e) {
            output = data;
            e.printStackTrace();
        } finally {
            try {
                o.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        decompresser.end();
        return output;
    }
    

}
