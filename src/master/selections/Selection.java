package org.thesis.project.master.selections;

import java.util.concurrent.ConcurrentSkipListMap;

import org.thesis.project.master.ServerObj;

public interface Selection {
	public String selectServer(ConcurrentSkipListMap<String, ServerObj> serversMap) ;
}
