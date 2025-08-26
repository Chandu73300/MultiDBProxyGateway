package com.example.pam.db.proxy.gateway.server;

class ConnectionState {
	private int connectionId;
	private String username;
	private String currentDatabase;

	ConnectionState(int connectionId, String username) {
		this.connectionId = connectionId;
		this.username = username;
	}

	public String getUsername() { return username; }
	public String getCurrentDatabase() { return currentDatabase; }
	public void setCurrentDatabase(String database) { this.currentDatabase = database; }
	public int getConnectionId() { return connectionId; }
	
	@Override
	public String toString() {
		return "ConnectionState{" +
				"connectionId=" + connectionId +
				", username='" + username + '\'' +
				", currentDatabase='" + currentDatabase + '\'' +
				'}';
	}
}