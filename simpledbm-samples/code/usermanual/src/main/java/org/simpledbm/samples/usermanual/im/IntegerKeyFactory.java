package org.simpledbm.samples.usermanual.im;

import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.rss.api.im.IndexKeyFactory;

public class IntegerKeyFactory implements IndexKeyFactory {

	public IndexKey maxIndexKey(int containerId) {
		return IntegerKey.createMaxKey();
	}

	public IndexKey minIndexKey(int containerId) {
		return IntegerKey.createMinKey();
	}

	public IndexKey newIndexKey(int containerId) {
		return IntegerKey.createNullKey();
	}

}
