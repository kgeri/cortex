package org.ogreg.cortex.message;

public class ErrorResponse extends Response {
	private static final long serialVersionUID = -5833641743579713268L;

	public ErrorResponse(int requestId, Throwable error) {
		super(requestId, error);
	}

	public Throwable getError() {
		return (Throwable) value;
	}
}
