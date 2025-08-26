package com.example.pam.db.proxy.gateway.server;

import javax.sql.DataSource;

import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class JdbcConfig {
	
	@Bean
    @Primary
    DataSource dataSource(DatabaseConnectionConfig dbConfig) {
		DatabaseConnectionConfig.DatabaseTypeConfig mysql = dbConfig.getDatabaseConfig("postgresql");
		if (mysql == null) {
			throw new IllegalStateException("MySQL configuration not found");
		}
		
		return DataSourceBuilder.create()
                .url(mysql.getJdbcUrlTemplate())
                .username(mysql.getUsername())
                .password(mysql.getPassword())
                .driverClassName(mysql.getDriverClass())
                .build();
    }

    @Bean
    JdbcTemplate defaultJdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
