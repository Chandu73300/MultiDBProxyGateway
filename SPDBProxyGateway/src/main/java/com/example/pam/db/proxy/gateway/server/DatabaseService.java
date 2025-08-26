package com.example.pam.db.proxy.gateway.server;

import org.springframework.stereotype.Service;

@Service
public interface DatabaseService {
    QueryResults executeQuery(String query);
    QueryResults executeQuery(String query, String database);
    boolean databaseExists(String database);
    String[] listDatabases();
    
    class QueryResults {
        public boolean isSelect;
        public String[] columns;
        public String[][] rows;
        public Integer affectedRows;
    }
}