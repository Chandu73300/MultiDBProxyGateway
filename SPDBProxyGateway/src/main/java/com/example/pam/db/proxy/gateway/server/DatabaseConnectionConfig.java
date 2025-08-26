package com.example.pam.db.proxy.gateway.server;

import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "database.proxy")
public class DatabaseConnectionConfig {
 
 private Map<String, DatabaseTypeConfig> databases = new HashMap<>();
 
 public DatabaseConnectionConfig() {
     initializeDefaultConfigurations();
 }
 
 private void initializeDefaultConfigurations() {
     DatabaseTypeConfig postgresConfig = new DatabaseTypeConfig();
     postgresConfig.setHost("localhost");
     postgresConfig.setPort(5432);
     postgresConfig.setUsername("postgres");
     postgresConfig.setPassword("root");
     postgresConfig.setDriverClass("org.postgresql.Driver");
     postgresConfig.setJdbcUrlTemplate("jdbc:postgresql://localhost:5432/postgres");
     postgresConfig.setDefaultDatabase("postgres");
     databases.put("postgresql", postgresConfig);
     
     // MySQL configuration
     DatabaseTypeConfig mysqlConfig = new DatabaseTypeConfig();
     mysqlConfig.setHost("localhost");
     mysqlConfig.setPort(3306);
     mysqlConfig.setUsername("root");
     mysqlConfig.setPassword("root");
     mysqlConfig.setDriverClass("com.mysql.cj.jdbc.Driver");
     mysqlConfig.setJdbcUrlTemplate("jdbc:mysql://localhost:3306/mysql?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC");
     mysqlConfig.setDefaultDatabase("mysql");
     databases.put("mysql", mysqlConfig);
 }
 
 public Map<String, DatabaseTypeConfig> getDatabases() {
     return databases;
 }
 
 public void setDatabases(Map<String, DatabaseTypeConfig> databases) {
     this.databases = databases;
 }
 
 public DatabaseTypeConfig getDatabaseConfig(String databaseType) {
     return databases.get(databaseType.toLowerCase());
 }
 
 public String buildJdbcUrl(String databaseType, String databaseName) {
     DatabaseTypeConfig config = getDatabaseConfig(databaseType);
     if (config == null) {
         throw new IllegalArgumentException("Unsupported database type: " + databaseType);
     }
     
     String url = config.getJdbcUrlTemplate();
     url = url.replace("{host}", config.getHost());
     url = url.replace("{port}", String.valueOf(config.getPort()));
     url = url.replace("{database}", databaseName != null ? databaseName : config.getDefaultDatabase());
     
     return url;
 }
 
 public static class DatabaseTypeConfig {
     private String host;
     private int port;
     private String username;
     private String password;
     private String driverClass;
     private String jdbcUrlTemplate;
     private String defaultDatabase;
     private Map<String, String> properties = new HashMap<>();
     
     // Getters and setters
     public String getHost() {
         return host;
     }
     
     public void setHost(String host) {
         this.host = host;
     }
     
     public int getPort() {
         return port;
     }
     
     public void setPort(int port) {
         this.port = port;
     }
     
     public String getUsername() {
         return username;
     }
     
     public void setUsername(String username) {
         this.username = username;
     }
     
     public String getPassword() {
         return password;
     }
     
     public void setPassword(String password) {
         this.password = password;
     }
     
     public String getDriverClass() {
         return driverClass;
     }
     
     public void setDriverClass(String driverClass) {
         this.driverClass = driverClass;
     }
     
     public String getJdbcUrlTemplate() {
         return jdbcUrlTemplate;
     }
     
     public void setJdbcUrlTemplate(String jdbcUrlTemplate) {
         this.jdbcUrlTemplate = jdbcUrlTemplate;
     }
     
     public String getDefaultDatabase() {
         return defaultDatabase;
     }
     
     public void setDefaultDatabase(String defaultDatabase) {
         this.defaultDatabase = defaultDatabase;
     }
     
     public Map<String, String> getProperties() {
         return properties;
     }
     
     public void setProperties(Map<String, String> properties) {
         this.properties = properties;
     }
 }
}