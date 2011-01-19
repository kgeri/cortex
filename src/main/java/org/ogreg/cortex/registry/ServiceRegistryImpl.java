package org.ogreg.cortex.registry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceRegistryImpl implements ServiceRegistry {
	private final Map<String, Object> services = new ConcurrentHashMap<String, Object>();

	@Override
	public <T> void register(T service, String identifier) {
		services.put(key(service.getClass().getName(), identifier), service);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T getService(Class<?> type, String identifier) throws ServiceNotFoundException {
		String key = key(type.getName(), identifier);
		T svc = (T) services.get(key);
		if (svc == null) {
			throw new ServiceNotFoundException("No service was found by key: " + key);
		}
		return svc;
	}

	@Override
	public boolean hasService(String type, String identifier) {
		return services.containsKey(key(type, identifier));
	}

	private String key(String type, String identifier) {
		return type + identifier;
	}

	@Override
	public void clear() {
		services.clear();
	}
}