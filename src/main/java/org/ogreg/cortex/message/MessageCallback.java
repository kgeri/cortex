package org.ogreg.cortex.message;

public interface MessageCallback<R> {
	void onSuccess(R response);
	
	void onFailure(Throwable error);
}
