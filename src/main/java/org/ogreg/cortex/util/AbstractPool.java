package org.ogreg.cortex.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

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
	private final Semaphore sem;

	/** Stores the position of the next free element in the pool. */
	private final AtomicInteger pos;

	/**
	 * Creates a pool with the given capacity, and initializes its elements using the
	 * {@link #create()} method.
	 * 
	 * @param capacity
	 */
	public AbstractPool(int capacity) {
		this.pool = new Object[capacity];
		this.sem = new Semaphore(capacity);
		this.pos = new AtomicInteger(0);
	}

	/**
	 * Implementors should create one element of the pool here.
	 * 
	 * @return
	 */
	public abstract T create();

	/**
	 * Called before a free pooled element is returned with the {@link #get()} method.
	 */
	protected void onElementGet(T element) {
		// Default implementation does nothing
	}

	/**
	 * Called before an used element is returned to the pool with the {@link #release(Object)}
	 * method.
	 */
	protected void onElementReleased(T element) {
		// Default implementation does nothing
	}

	/**
	 * Gets an element from the pool, blocking execution until at least one element becomes
	 * available.
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unchecked")
	public T get() throws InterruptedException {
		sem.acquire();
		T element = (T) pool[pos.getAndIncrement()];
		element = element == null ? create() : element;
		onElementGet(element);
		return element;
	}

	/**
	 * Returns an element back to the pool.
	 * 
	 * @param element
	 */
	public void release(T element) {
		onElementReleased(element);
		pool[pos.decrementAndGet()] = element;
		sem.release();
	}
}
