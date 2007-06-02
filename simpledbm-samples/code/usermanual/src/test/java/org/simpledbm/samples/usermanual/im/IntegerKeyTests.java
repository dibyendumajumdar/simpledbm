package org.simpledbm.samples.usermanual.im;

import junit.framework.TestCase;

import org.simpledbm.rss.api.im.IndexKeyFactory;

public class IntegerKeyTests extends TestCase {

	public IntegerKeyTests() {
		super();
	}

	public IntegerKeyTests(String name) {
		super(name);
	}

	public void testCase1() {
		int containerId = 1;
		IndexKeyFactory keyFactory = new IntegerKeyFactory();
		IntegerKey nullKey = (IntegerKey) keyFactory.newIndexKey(containerId);
		IntegerKey valueKey = (IntegerKey) keyFactory.newIndexKey(containerId);
		IntegerKey minKey = (IntegerKey) keyFactory.minIndexKey(containerId);
		IntegerKey maxKey = (IntegerKey) keyFactory.maxIndexKey(containerId);
		valueKey.setValue(55);
		assertTrue(nullKey.isNull());
		assertFalse(valueKey.isNull());
		assertFalse(minKey.isNull());
		assertFalse(maxKey.isNull());
		assertTrue(nullKey.compareTo(nullKey) == 0);
		assertTrue(nullKey.equals(nullKey));
		assertTrue(nullKey.compareTo(minKey) < 0);
		assertTrue(minKey.compareTo(valueKey) < 0);
		assertTrue(valueKey.compareTo(maxKey) < 0);
	}
	
}
