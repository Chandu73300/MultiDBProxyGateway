package com.example.pam.db.proxy.gateway.server;

import java.util.Map;

class StartupMessage {
    int protocolVersion;
    String username;
    String database;
    String connectionName;
    Integer connectedUser;
    Map<String, String> parameters;
    
    StartupMessage(int protocolVersion, String username, String database, Map<String, String> parameters) {
        this.protocolVersion = protocolVersion;
        this.username = username;
        this.database = database;
        this.parameters = parameters;
    }

	public String getConnectionName() {
		return connectionName;
	}

	public void setConnectionName(String connectionName) {
		this.connectionName = connectionName;
	}

	public Integer getConnectedUser() {
		return connectedUser;
	}

	public void setConnectedUser(Integer connectedUser) {
		this.connectedUser = connectedUser;
	}
}