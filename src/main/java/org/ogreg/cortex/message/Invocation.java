package org.ogreg.cortex.message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Invocation extends Message {
	private static final long serialVersionUID = -149555536344976546L;

	private final Class<?> type;
	private final String identifier;
	private final String methodName;
	private final Class<?>[] argTypes;
	private final Object[] args;

	private Invocation(String identifier, Class<?> type, String methodName, Class<?>[] argTypes,
			Object[] args) {
		this.identifier = identifier;
		this.type = type;
		this.methodName = methodName;
		this.argTypes = argTypes;
		this.args = args;
	}

	public Object invoke(Object target) throws SecurityException, NoSuchMethodException,
			IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		
		// TODO Method invoke from bytecode?
		Method method = type.getDeclaredMethod(methodName, argTypes);
		if (!method.isAccessible()) {
			method.setAccessible(true);
		}
		return method.invoke(target, args);

	}

	public Class<?> getType() {
		return type;
	}

	public String getIdentifier() {
		return identifier;
	}

	public static Invocation create(Method method, Object... args) {
		return create(null, method, args);
	}

	public static Invocation create(String identifier, Method method, Object... args) {
		return new Invocation(identifier, method.getDeclaringClass(), method.getName(),
				method.getParameterTypes(), args);
	}
}
