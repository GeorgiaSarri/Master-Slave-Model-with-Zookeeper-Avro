package org.thesis.project.server;

import java.util.ArrayList;
import java.util.List;

public class ChildrenList {
protected List<String> children;
	
	public ChildrenList() {
		this.children = null;
	}
	
	public ChildrenList(List<String> children) {
		this.children = children;
	}
	
	List<String> getList() {
		return children;
	}
	
	/**
	 * Checks the children of the znode, renews it
	 * and returns the extra children that were added
	 * 
	 * @param List<String> newChildren
	 * 
	 */
	List<String> addAndSet(List<String> newChildren) {
		ArrayList<String> addedChildren = null;
		
		if(children==null) {
			addedChildren = new ArrayList<String>(newChildren);
		}else {
			for(String c : newChildren){
				if (!children.contains(c)) {
					if (addedChildren == null) 
						addedChildren = new ArrayList<String>();
					
					addedChildren.add(c);
				}
			}
		}
		this.children = newChildren;
		
		return addedChildren;
	}
	
	/**
	 * Checks the children of the znode, renews it
	 * and returns the children that were missing
	 * 
	 * @param List<String> newChildren
	 * 
	 */
	List<String> removedAndSet(List<String> newChildren) {
		ArrayList<String> delChildren = null;
		
		if (children != null) {
			for(String c : children) {
				if (!newChildren.contains(c)) {
					if (delChildren == null)
						delChildren = new ArrayList<String>();
					delChildren.add(c);
				}
			}
		}
		this.children = newChildren;
		//System.out.println("blaaa");
		return delChildren;
	}
}
