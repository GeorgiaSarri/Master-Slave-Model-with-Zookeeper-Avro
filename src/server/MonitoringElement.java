package org.thesis.project.server;

import java.util.Date;

public class MonitoringElement {
	
	private final String type;
	private final long responseTime;
	private final long elementId;
	
	public MonitoringElement(String type, long responseTime) {
		this.type = type;
		this.responseTime = responseTime;
		elementId = new Date().getTime();
	}

	public String getType() {
		return type;
	}

	public long getResponseTime() {
		return responseTime;
	}

	public long getElementId() {
		return elementId;
	}
}
