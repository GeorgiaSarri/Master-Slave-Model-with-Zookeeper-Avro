package org.thesis.project.master;

import java.util.concurrent.locks.ReentrantLock;

public class ServerObj {
	private final 		String 	serverID;
	private final 		String	serverIP;
	private volatile 	int 	srvClients;
	private volatile 	double 	srvUpAvgTime;
	private volatile 	long 	srvTotalRuntime = 0;
	
	private final ReentrantLock servClientsLock = new ReentrantLock();
	private final ReentrantLock servUpAvgTimeLock = new ReentrantLock();
	private final ReentrantLock servTotalRuntimeLock = new ReentrantLock();

	public ServerObj(String serverID, String serverIP, int srvClients, double srvUpAvgTime) {
		this.serverID 	= serverID;
		this.serverIP 	= serverIP;
		this.srvClients = srvClients;
		this.srvUpAvgTime 	= srvUpAvgTime;
	}

	public String getServerID() {
		return serverID;
	}


	public String getServerIP() {
		return serverIP;
	}

	
	public int getSrvClients() {
		try {
			this.servClientsLock.lock();
			return srvClients;
		} finally {
			this.servClientsLock.unlock();
		}
	}

	public void setSrvClients(int srvClients) {
		try {
			this.servClientsLock.lock();
			this.srvClients = srvClients;
		} finally {
			this.servClientsLock.unlock();
		}
		
	}
	
	public void decrementClients() {
		try{
			this.servClientsLock.lock();
			this.srvClients--;
		}finally{
			this.servClientsLock.unlock();
		}
	}
	
	public double getSrvUpAvgTime() {
		try {
			this.servUpAvgTimeLock.lock();
			return srvUpAvgTime;
		}finally {
			this.servUpAvgTimeLock.unlock();
		}
	}

	public void setSrvUpAvgTime(double srvUpAvgTime) {
		try{
			this.servUpAvgTimeLock.lock();
			this.srvUpAvgTime = srvUpAvgTime;
		}finally {
			this.servUpAvgTimeLock.unlock();
		}
	}
	
	public void setSrvTotalRuntime(long srvTotalRuntime) {
		try {
			this.servTotalRuntimeLock.lock();
			this.srvTotalRuntime = srvTotalRuntime;
		}finally {
			this.servTotalRuntimeLock.unlock();
		}
	}
	
	public synchronized long getSrvTotalRuntime() {
		try{
			this.servTotalRuntimeLock.lock();
			return srvTotalRuntime;
		}finally {
			this.servTotalRuntimeLock.unlock();
		}
	}
}