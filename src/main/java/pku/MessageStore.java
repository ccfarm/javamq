package pku;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class MessageStore {
	
	static final int CAPACITY = 60 * 1024 * 1024;
	String filename;
	ByteBuffer buf;
	int index;
	
	
	public MessageStore(String filename) {
		index = 0;
		this.filename = filename;
		buf = ByteBuffer.allocateDirect(CAPACITY);
	}
	
	
	public void putString(String st) {
		for (int i = 0; i < st.length(); i++) {
			buf.putChar(st.charAt(i));
		}
		buf.putChar('\n');
	}
	
	
	public void write() throws Exception {
		if (buf.remaining() == CAPACITY) {
			return;
		}
		RandomAccessFile rf = new RandomAccessFile("data/" + filename + "+" +index, "rw");
		index++;
		buf.put((byte)17);
		byte[] bytes = new byte[CAPACITY];
		buf.position(0);
		buf.get(bytes);
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
		buf.put((byte)18);
		buf.putInt(defaultMessage.getBody().length);
		buf.put(defaultMessage.getBody());
		
		if (buf.remaining() <= 205 * 1024) {
			write();
		}
		
	}

}
