package org.ogreg.cortex;

public abstract class Endpoint {
	protected String identifier = null;
	protected Class<?> type;
	
	public String getTypeName() {
		return type == null ? null : type.getName();
	}

	public String getIdentifier() {
		return identifier;
	}
	
	static class SingletonEndpoint extends Endpoint {
		public SingletonEndpoint(Class<?> type) {
			this.type = type;
		}
	}
	
	static class InstanceEndpoint extends Endpoint {
		public InstanceEndpoint(Class<?> type, String identifier) {
			this.type = type;
			this.identifier = identifier;
		}
	}
}