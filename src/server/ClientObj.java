package org.thesis.project.server;

import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

public class ClientObj {
	private final String clientID;
	private final String status;
	private long timestamp;
	private final String data;
	private final Date	startedTime;
	private volatile Date dateLastModified;
	
	private final ReentrantLock dateLastModifiedLock = new ReentrantLock();
	private final ReentrantLock timestampLock = new ReentrantLock();

	public ClientObj(String clientID, String status, String timestamp, String data) {
		this.clientID = clientID;
		this.status = status;
		this.timestamp = Long.valueOf(timestamp);
		this.data = data;
		startedTime = new Date();
		dateLastModified = new Date();
	}

	public Date getDateLastModified() {
		try {
			dateLastModifiedLock.lock();
			return dateLastModified;
		} finally {
			dateLastModifiedLock.unlock();
		}
	}


	public void setDateLastModified(Date dateLastModified) {
		try {
			dateLastModifiedLock.lock();
			this.dateLastModified = dateLastModified;
		} finally {
			dateLastModifiedLock.unlock();
		}
	}


	public String getData() {
		return data;
	}

	public String getClientID() {
		return clientID;
	}


	public String getStatus() {
		return status;
	}
	
	public Date getStartedTime() {
		return startedTime;
	}


	public long getTimestamp() {
		try {
			timestampLock.lock();
			return timestamp;
		}finally {
			timestampLock.unlock();
		}
		
	}


	public void setTimestamp(long timestamp) {
		try {
			timestampLock.lock();
			this.timestamp = timestamp;
		} finally {
			timestampLock.unlock();
		}
	}
}