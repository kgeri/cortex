package org.ogreg.cortex.util;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A generic base class for pooling objects of type T.
 * 
 * @author Gergely Kiss
 * @param <T>
 */
public abstract class AbstractPool<T> {
	/** The pool of the objects. */
	private final Object[] pool;

	/** Synchronization object for limiting concurrent access to the pool. */
	private final Lock lock;
	private final Condition notEmpty;

	/** Stores the position of the next free element in the pool. */
	private int pos;

	/**
	 * Creates a pool with the given capacity, and initializes its elements using the
	 * {@link #create()} method.
	 * 
	 * @param capacity
	 */
	public AbstractPool(int capacity) {
		this.pool = new Object[capacity];
		this.lock = new ReentrantLock();
		this.notEmpty = lock.newCondition();
		this.pos = capacity;
	}

	/**
	 * The implementation should create one element of the pool here.
	 * 
	 * @return
	 */
	protected abstract T create();

	/**
	 * The implementation should reset the element's state in this method, before it is returned to
	 * the pool.
	 */
	protected abstract void clear(T element);

	/**
	 * Gets an element from the pool, blocking execution until at least one element becomes
	 * available.
	 * <p>
	 * Warning: do not forget to call {@link #release(Object)} in a finally statement!
	 * </p>
	 * 
	 * @return
	 * @throws InterruptedException
	 * @see {@link #release(Object)}
	 */
	@SuppressWarnings("unchecked")
	public T acquire() throws InterruptedException {
		lock.lockInterruptibly();
		try {
			while (pos == 0) {
				notEmpty.await();
			}
			pos--;
			T element = (T) pool[pos];
			return element == null ? create() : element;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Returns an element back to the pool.
	 * 
	 * @param element
	 */
	public void release(T element) {
		lock.lock();
		try {
			clear(element);
			pool[pos] = element;
			pos++;
			notEmpty.signal();
		} finally {
			lock.unlock();
		}
	}
}
