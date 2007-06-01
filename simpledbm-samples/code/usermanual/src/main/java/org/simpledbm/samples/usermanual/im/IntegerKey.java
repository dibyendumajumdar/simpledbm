package org.simpledbm.samples.usermanual.im;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.im.IndexKey;

public class IntegerKey implements IndexKey {

	private static final byte NULL_FIELD = 1;
	private static final byte MINUS_INFINITY_FIELD = 2;
	private static final byte VALUE_FIELD = 4;
	private static final byte PLUS_INFINITY_FIELD = 8;

	private byte statusByte = NULL_FIELD;
	private int value;

	public IntegerKey() {
		statusByte = NULL_FIELD;
	}

	public IntegerKey(int value) {
		statusByte = VALUE_FIELD;	
	}
	
	protected IntegerKey(byte statusByte, int value) {
		this.statusByte = statusByte;
		this.value = value;
	}
	
	public int getValue() {
		if (statusByte != VALUE_FIELD) {
			throw new IllegalStateException();
		}
		return value;
	}

	public void setValue(int i) {
		value = i;
		statusByte = VALUE_FIELD;
	}

	public void retrieve(ByteBuffer bb) {
		statusByte = bb.get();
		value = bb.getInt();
	}

	public void store(ByteBuffer bb) {
		bb.put(statusByte);
		bb.putInt(value);
	}

	public int getStoredLength() {
		return 5;
	}

	public int compareTo(IndexKey key) {
		if (key == this) {
			return 0;
		}
		if (key.getClass() != getClass()) {
			throw new IllegalArgumentException(
					"Supplied key is not of the correct type");
		}
		IntegerKey other = (IntegerKey) key;
		int result = statusByte - other.statusByte;
		if (result == 0 && statusByte == VALUE_FIELD) {
			result = value - other.value;
		}
		return result;
	}

	public final boolean isNull() {
		return statusByte == NULL_FIELD;
	}

	public final boolean isMinKey() {
		return statusByte == MINUS_INFINITY_FIELD;
	}

	public final boolean isMaxKey() {
		return statusByte == PLUS_INFINITY_FIELD;
	}

	public final boolean isValue() {
		return statusByte == VALUE_FIELD;
	}

	public boolean equals(Object o) {
		if (o == null || !(o instanceof IntegerKey)) {
			return false;
		}
		return compareTo((IntegerKey) o) == 0;
	}

	public void parseString(String s) {
		if ("<NULL>".equals(s)) {
			statusByte = NULL_FIELD;
		} else if ("<MINKEY>".equals(s)) {
			statusByte = MINUS_INFINITY_FIELD;
		} else if ("<MAXKEY>".equals(s)) {
			statusByte = PLUS_INFINITY_FIELD;
		} else {
			value = Integer.parseInt(s);
			statusByte = VALUE_FIELD;
		}
	}
	
	public String toString() {
		if (isNull()) {
			return "<NULL>";
		}
		else if (isMinKey()) {
			return "<MINKEY>";
		}
		else if (isMaxKey()) {
			return "<MAXKEY>";
		}
		else {
			return Integer.toString(value);
		}
	}
	
	public static IntegerKey createNullKey() {
		return new IntegerKey(NULL_FIELD, 0);
	}
	public static IntegerKey createMinKey() {
		return new IntegerKey(MINUS_INFINITY_FIELD, 0);
	}
	public static IntegerKey createMaxKey() {
		return new IntegerKey(PLUS_INFINITY_FIELD, 0);
	}


}
