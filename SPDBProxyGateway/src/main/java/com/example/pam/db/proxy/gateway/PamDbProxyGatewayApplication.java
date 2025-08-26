package com.example.pam.db.proxy.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

import com.example.pam.db.proxy.gateway.server.EnhancedMultiDatabaseProxyServer;

import jakarta.annotation.PostConstruct;

@SpringBootApplication
public class PamDbProxyGatewayApplication {

	private EnhancedMultiDatabaseProxyServer proxyServer;
	
	public PamDbProxyGatewayApplication(EnhancedMultiDatabaseProxyServer proxyServer) {
		this.proxyServer = proxyServer;
	}
	
	public static void main(String[] args) {
		SpringApplication.run(PamDbProxyGatewayApplication.class, args);
	}

	@PostConstruct
    public void startProxyServer() {
        proxyServer.start();
    }
}
