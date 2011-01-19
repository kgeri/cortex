package org.ogreg.cortex.registry;

/**
 * Common interface for components which provide local service information.
 * 
 * @author Gergely Kiss
 */
public interface ServiceRegistry {

	<T> void register(T service, String identifier);

	/**
	 * Returns the service by <code>type</code>, by <code>identifier</code> or both.
	 * <p>
	 * Singletons may be located by their type only, also every service wich was registered with an
	 * identifier may be located by its identifier only.
	 * </p>
	 * 
	 * @param <T>
	 * @param type The type of the service
	 * @param identifier The identifier of the service
	 * @return The service instance or an exception - never null
	 * @throws ServiceNotFoundException if the service was not found
	 */
	<T> T getService(Class<?> type, String identifier) throws ServiceNotFoundException;

	boolean hasService(String clazz, String identifier);

	void clear();
}
