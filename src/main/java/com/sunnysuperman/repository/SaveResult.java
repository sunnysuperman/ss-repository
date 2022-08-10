package com.sunnysuperman.repository;

public class SaveResult {
	private boolean inserted;
	private boolean updated;
	private Object generatedId;

	public boolean isInserted() {
		return inserted;
	}

	public void setInserted(boolean inserted) {
		this.inserted = inserted;
	}

	public boolean isUpdated() {
		return updated;
	}

	public void setUpdated(boolean updated) {
		this.updated = updated;
	}

	public Object getGeneratedId() {
		return generatedId;
	}

	public void setGeneratedId(Object generatedId) {
		this.generatedId = generatedId;
	}

}
