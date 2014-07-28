package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;

public class Neighbours implements Serializable {
	String pred;
	String succ;
	String isOnly;
	
	Neighbours(String pred,String succ,String isOnly)
	{
		this.pred = pred;
		this.succ = succ;
		this.isOnly = isOnly;
	}

	public String getIsOnly() {
		return isOnly;
	}

	public void setIsOnly(String isOnly) {
		this.isOnly = isOnly;
	}

	public String getPred() {
		return pred;
	}

	public void setPred(String pred) {
		this.pred = pred;
	}

	public String getSucc() {
		return succ;
	}

	public void setSucc(String succ) {
		this.succ = succ;
	}

}
