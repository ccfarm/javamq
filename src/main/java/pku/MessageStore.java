package pku;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.*;
import java.io.*;


public class MessageStore {
	
	static final int CAPACITY =  4608 * 1024;
	String filename;
	ByteBuffer buf;
	int index;
	
	
	public MessageStore(String filename) {
		index = 0;
		this.filename = filename;
		buf = ByteBuffer.allocateDirect(CAPACITY);
	}
	
	
	public void putString(String st) {
    	buf.putInt(st.getBytes().length);
		buf.put(st.getBytes());
	}
	
	
	public void write() throws Exception {
		if (buf.remaining() == CAPACITY) {
			return;
		}
		
		buf.put((byte)17);//17 means to be continue
		byte[] bytes = new byte[buf.position()];
		buf.position(0);
		buf.get(bytes);
		bytes = compress(bytes);
		RandomAccessFile rf = new RandomAccessFile("data/" + filename + "+" +index, "rw");
		//FileOutputStream rf = new FileOutputStream("data/" + filename + "+" +index, true);
		index++;
		rf.writeInt(bytes.length);
		rf.write(bytes);
		rf.close();
		buf.clear();
	}
	

	
	
	public void push(ByteMessage defaultMessage) throws Exception{
		if (defaultMessage == null) {
			return;
		}
		
		int v1 = 0;
		long v2 = 0;
		String v3 = null;
		double v4 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.MESSAGE_ID)) {
			v1 = defaultMessage.headers().getInt(MessageHeader.MESSAGE_ID);
			buf.put((byte)1);
			buf.putInt(v1);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.TOPIC)) {
			v3 = defaultMessage.headers().getString(MessageHeader.TOPIC);
			buf.put((byte)2);
			putString(v3);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.BORN_TIMESTAMP)) {
			v2 = defaultMessage.headers().getLong(MessageHeader.BORN_TIMESTAMP);
			buf.put((byte)3);
			buf.putLong(v2);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.BORN_HOST)) {
			v3 = defaultMessage.headers().getString(MessageHeader.BORN_HOST);
			buf.put((byte)4);
			putString(v3);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.STORE_TIMESTAMP)) {
			v2 = defaultMessage.headers().getLong(MessageHeader.STORE_TIMESTAMP);
			buf.put((byte)5);
			buf.putLong(v2);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.STORE_HOST)) {
			v3 = defaultMessage.headers().getString(MessageHeader.STORE_HOST);
			buf.put((byte)6);
			putString(v3);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.START_TIME)) {
			v2 = defaultMessage.headers().getLong(MessageHeader.START_TIME);
			buf.put((byte)7);
			buf.putLong(v2);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.STOP_TIME)) {
			v2 = defaultMessage.headers().getLong(MessageHeader.STOP_TIME);
			buf.put((byte)8);
			buf.putLong(v2);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.TIMEOUT)) {
			v1 = defaultMessage.headers().getInt(MessageHeader.TIMEOUT);
			buf.put((byte)9);
			buf.putInt(v1);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.PRIORITY)) {
			v1 = defaultMessage.headers().getInt(MessageHeader.PRIORITY);
			buf.put((byte)10);
			buf.putInt(v1);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.RELIABILITY)) {
			v1 = defaultMessage.headers().getInt(MessageHeader.RELIABILITY);
			buf.put((byte)11);
			buf.putInt(v1);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.SEARCH_KEY)) {
			v3 = defaultMessage.headers().getString(MessageHeader.SEARCH_KEY);
			buf.put((byte)12);
			putString(v3);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.SCHEDULE_EXPRESSION)) {
			v3 = defaultMessage.headers().getString(MessageHeader.SCHEDULE_EXPRESSION);
			buf.put((byte)13);
			putString(v3);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.SHARDING_KEY)) {
			v4 = defaultMessage.headers().getDouble(MessageHeader.SHARDING_KEY);
			buf.put((byte)14);
			buf.putDouble(v4);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.SHARDING_PARTITION)) {
			v4 = defaultMessage.headers().getDouble(MessageHeader.SHARDING_PARTITION);
			buf.put((byte)15);
			buf.putDouble(v4);
		}
		
		if (defaultMessage.headers().containsKey(MessageHeader.TRACE_ID)) {
			v3 = defaultMessage.headers().getString(MessageHeader.TRACE_ID);
			buf.put((byte)16);
			putString(v3);
		}
		buf.put((byte)18);//18 means the start of the body of this message
		buf.putInt(defaultMessage.getBody().length);
		buf.put(defaultMessage.getBody());
		
		if (buf.remaining() <= 201 * 1024) {
			write();
		}
		
	}
	
    public static byte[] compress(byte[] data) {
        byte[] output = new byte[0];

        Deflater compresser = new Deflater();

        compresser.reset();
        compresser.setInput(data);
        compresser.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[1024];
            while (!compresser.finished()) {
                int i = compresser.deflate(buf);
                bos.write(buf, 0, i);
            }
            output = bos.toByteArray();
        } catch (Exception e) {
            output = data;
            e.printStackTrace();
        } finally {
            try {
                bos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        compresser.end();
        return output;
    }

}
