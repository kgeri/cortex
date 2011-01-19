package org.ogreg.cortex.message;

/**
 * Base class for response messages.
 * <p>
 * A response message may hold an arbitrary, {@link java.io.Serializable} object graph - but it must also
 * specify the identifier of the request message it corresponds to.
 * </p>
 * 
 * @author Gergely Kiss
 */
public class Response extends Message {
	private static final long serialVersionUID = -3156832996620812543L;

	public final int requestId;
	public final Object value;

	/**
	 * Constructs a response message as an answer to a previous request.
	 * 
	 * @param requestId The identifier of the request
	 * @param value The response value to send
	 */
	public Response(int requestId, Object value) {
		this.requestId = requestId;
		this.value = value;
	}
}
