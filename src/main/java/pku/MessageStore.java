package pku;

import java.nio.ByteBuffer;
import java.util.zip.Deflater;
import java.util.*;
import java.io.*;


public class MessageStore {
	
	static final int CAPACITY = 6660 * 1024;
	static final int BUFOUTPUT = 4660 * 1024;
	static final int BUFINPUT = 2048 * 1024;
	static final int COMPRESS = 5 * 1024;
	//String filename;
	ByteBuffer buf;
	//int index;
	OutputStream output;
	File file;
	
	
	public MessageStore(String filename) throws Exception{
		//index = 0;
		//this.filename = filename;
		buf = ByteBuffer.allocateDirect(CAPACITY);
		file = new File("data/" +filename);
		//System.out.println("data/" +filename);
		output = new BufferedOutputStream(new FileOutputStream(file), BUFOUTPUT);
	}
	
	
	public void putString(String st) {
    	buf.put((byte)st.getBytes().length);
		buf.put(st.getBytes());
	}
	
	
	public void write() throws Exception {
		if (buf.remaining() == CAPACITY) {
			return;
		}
		
		buf.putShort((short)-1);//17 means to be continue
		byte[] bytes = new byte[buf.position()];
		buf.position(0);
		buf.get(bytes);
		bytes = compress(bytes);
		writeInt(bytes.length);
		output.write(bytes, 0, bytes.length);
		//output.flush();
		buf.clear();
	}
	
	public void writeEnd() throws Exception {
		write();
		writeInt(-1);
		//output.flush();
		output.close();
	}
	

	public void writeInt(int a) throws Exception{
		byte[] b = new byte[]{
				(byte) ((a >> 24) & 0xFF),
				(byte) ((a >> 16) & 0xFF),     
		        (byte) ((a >> 8) & 0xFF),     
		        (byte) (a & 0xFF)};  
		output.write(b, 0, 4);
	}
	
	public void push(ByteMessage defaultMessage) throws Exception{
		if (defaultMessage == null) {
			return;
		}
		

		short key = 0;
		int v1 =0;
		if (defaultMessage.headers().containsKey(MessageHeader.MESSAGE_ID)) {
			v1 = defaultMessage.headers().getInt(MessageHeader.MESSAGE_ID);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		//if (defaultMessage.headers().containsKey(MessageHeader.TOPIC)) {
			//v3 = defaultMessage.headers().getString(MessageHeader.TOPIC);
			//buf.put((byte)2);
			//putString(v3);
		//}
		long v2 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.BORN_TIMESTAMP)) {
			v2 = defaultMessage.headers().getLong(MessageHeader.BORN_TIMESTAMP);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		String v3 = null;
		if (defaultMessage.headers().containsKey(MessageHeader.BORN_HOST)) {
			v3 = defaultMessage.headers().getString(MessageHeader.BORN_HOST);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		long v4 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.STORE_TIMESTAMP)) {
			v4 = defaultMessage.headers().getLong(MessageHeader.STORE_TIMESTAMP);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		String v5 = null;
		if (defaultMessage.headers().containsKey(MessageHeader.STORE_HOST)) {
			v5 = defaultMessage.headers().getString(MessageHeader.STORE_HOST);

			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		long v6 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.START_TIME)) {
			v6 = defaultMessage.headers().getLong(MessageHeader.START_TIME);

			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		long v7 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.STOP_TIME)) {
			v7 = defaultMessage.headers().getLong(MessageHeader.STOP_TIME);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		int v8 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.TIMEOUT)) {
			v8 = defaultMessage.headers().getInt(MessageHeader.TIMEOUT);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		int v9 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.PRIORITY)) {
			v9 = defaultMessage.headers().getInt(MessageHeader.PRIORITY);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		int v10 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.RELIABILITY)) {
			v10 = defaultMessage.headers().getInt(MessageHeader.RELIABILITY);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		String v11 = null;
		if (defaultMessage.headers().containsKey(MessageHeader.SEARCH_KEY)) {
			v11 = defaultMessage.headers().getString(MessageHeader.SEARCH_KEY);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		String v12 = null;
		if (defaultMessage.headers().containsKey(MessageHeader.SCHEDULE_EXPRESSION)) {
			v12 = defaultMessage.headers().getString(MessageHeader.SCHEDULE_EXPRESSION);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		
		double v13 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.SHARDING_KEY)) {
			v13 = defaultMessage.headers().getDouble(MessageHeader.SHARDING_KEY);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		double v14 = 0;
		if (defaultMessage.headers().containsKey(MessageHeader.SHARDING_PARTITION)) {
			v14 = defaultMessage.headers().getDouble(MessageHeader.SHARDING_PARTITION);
			key = (short)((key | 1) << 1);
		} else {
			key = (short)(key<< 1);
		}
		String v15 = null;
		if (defaultMessage.headers().containsKey(MessageHeader.TRACE_ID)) {
			v15 = defaultMessage.headers().getString(MessageHeader.TRACE_ID);
			key = (short)(key | 1);
		} 
		//buf.put((byte)18);//18 means the start of the body of this message
		buf.putShort(key);
		if ((key >> 14 & 1) == 1) {
			buf.putInt(v1);
		}
		if ((key >> 13 & 1) == 1) {
			buf.putLong(v2);
		}
		if ((key >> 12 & 1) == 1) {
			putString(v3);
		}
		if ((key >> 11 & 1) == 1) {
			buf.putLong(v4);
		}
		if ((key >> 10 & 1) == 1) {
			putString(v5);
		}
		if ((key >> 9 & 1) == 1) {
			buf.putLong(v6);
		}
		if ((key >> 8 & 1) == 1) {
			buf.putLong(v7);
		}
		if ((key >> 7 & 1) == 1) {
			buf.putInt(v8);
		}
		if ((key >> 6 & 1) == 1) {
			buf.putInt(v9);
		}
		if ((key >> 5 & 1) == 1) {
			buf.putInt(v10);
		}
		if ((key >> 4 & 1) == 1) {
			putString(v11);
		}
		if ((key >> 3 & 1) == 1) {
			putString(v12);
		}
		if ((key >> 2 & 1) == 1) {
			buf.putDouble(v13);
		}
		if ((key >> 1 & 1) == 1) {
			buf.putDouble(v14);
		}
		if ((key & 1) == 1) {
			putString(v15);
		}
		buf.putInt(defaultMessage.getBody().length);
		buf.put(defaultMessage.getBody());
		
		if (buf.remaining() <= 201 * 1024) {
			write();
		}
		
	}
	
    public static byte[] compress(byte[] data) {
        byte[] output = new byte[0];

        Deflater compresser = new Deflater();
        compresser.setLevel(1);
		//compress.setStrategy(2);
        compresser.reset();
        compresser.setInput(data);
        compresser.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length);
        try {
            byte[] buf = new byte[COMPRESS];
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
