package org.ogreg.cortex.message;

import java.io.Serializable;

/**
 * Common base class for cortex messages.
 * <p>
 * Any serializable object graph may be transferred with a {@link Message}, the only requirement is
 * that each {@link Message} has a locally unique identifier - so that a node can asynchronously
 * pair the incoming response messages to their previously sent request messages.
 * </p>
 * 
 * @author Gergely Kiss
 * @see Response
 */
public abstract class Message implements Serializable {
	private static final long serialVersionUID = -5546574953121839006L;
	private static int ord = 0;

	public final int messageId = ord++;
}
