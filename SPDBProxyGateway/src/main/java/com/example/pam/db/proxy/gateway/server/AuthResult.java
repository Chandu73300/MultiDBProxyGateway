package com.example.pam.db.proxy.gateway.server;

class AuthResult {
    int sequenceId;
    ConnectionState connectionState;
    
    AuthResult(int sequenceId, ConnectionState connectionState) {
        this.sequenceId = sequenceId;
        this.connectionState = connectionState;
    }
}