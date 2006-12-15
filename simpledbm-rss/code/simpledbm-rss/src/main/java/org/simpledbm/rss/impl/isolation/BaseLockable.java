package org.simpledbm.rss.impl.isolation;

public class BaseLockable {

	final byte namespace;
	final Object lockable;
	
	protected BaseLockable(byte namespace, Object lockable) {
		this.namespace = namespace;
		this.lockable = lockable;
	}

	@Override
	public int hashCode() {
		final int PRIME = 31;
		int result = 1;
		result = PRIME * result + ((lockable == null) ? 0 : lockable.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final BaseLockable other = (BaseLockable) obj;
		if (lockable == null) {
			if (other.lockable != null)
				return false;
		} else if (!lockable.equals(other.lockable))
			return false;
		if (namespace != other.namespace)
			return false;
		return true;
	}
	
}
