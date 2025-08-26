package com.example.pam.db.proxy.gateway.server;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import com.example.pam.db.proxy.gateway.server.DatabaseService.QueryResults;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Service
public class EnhancedMultiDatabaseService {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(EnhancedMultiDatabaseService.class);
	
	private static final String DB_HOST = "localhost";
    private static final String DB_PORT = "3306";
    private static final String DB_USERNAME = "chandra";
    private static final String DB_PASSWORD = "eMudhra@1";
    private static final String DB_URL_TEMPLATE = "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    
    private static final Pattern SELECT_PATTERN = Pattern.compile("^(select|with)\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern INSERT_PATTERN = Pattern.compile("^insert\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern UPDATE_PATTERN = Pattern.compile("^update\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern DELETE_PATTERN = Pattern.compile("^delete\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern CREATE_PATTERN = Pattern.compile("^create\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern DROP_PATTERN = Pattern.compile("^drop\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern ALTER_PATTERN = Pattern.compile("^alter\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_PATTERN = Pattern.compile("^(show|describe|desc)\\s+", Pattern.CASE_INSENSITIVE);
    private static final Pattern USE_PATTERN = Pattern.compile("^use\\s+", Pattern.CASE_INSENSITIVE);
    
    private final Map<String, JdbcTemplate> connectionCache = new ConcurrentHashMap<>();
    private final Map<String, String> commandCache = new ConcurrentHashMap<>();
    static final Map<Integer, SessionInfo> sessionCommandCache = new ConcurrentHashMap<>();
    
    private JdbcTemplate defaultJdbcTemplate;
    private final ObjectMapper objectMapper;
    private final SimpleDateFormat timestampFormat;
    
    public EnhancedMultiDatabaseService(JdbcTemplate defaultJdbcTemplate) {
        this.defaultJdbcTemplate = Objects.requireNonNull(defaultJdbcTemplate, "defaultJdbcTemplate cannot be null");
        this.objectMapper = new ObjectMapper();
        this.timestampFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    }
    
    public QueryResult executeQuery(String sql, String databaseType, JdbcTemplate jdbcTemplate, StartupMessage startupMessage) {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL query cannot be null or empty");
        }
        if (databaseType == null || databaseType.trim().isEmpty()) {
            throw new IllegalArgumentException("Database type cannot be null or empty");
        }
        if (jdbcTemplate == null) {
            throw new IllegalArgumentException("JdbcTemplate cannot be null");
        }
        
        try {
            String trimmedSql = sql.trim();
            String translatedSql = translateQuery(trimmedSql, databaseType);
            
            cacheCommand(translatedSql, startupMessage);
            
            return executeSpecificQuery(translatedSql, databaseType, jdbcTemplate);
            
        } catch (Exception e) {
            LOGGER.error("Error executing SQL query: {}", e.getMessage(), e);
            throw new DatabaseServiceException("Error executing SQL: " + e.getMessage(), e);
        }
    }
    
    private void cacheCommand(String sql, StartupMessage startupMessage) {
        try {
            String timestamp = timestampFormat.format(new Date());
            commandCache.put(timestamp, sql);
            String jsonCache = objectMapper.writeValueAsString(commandCache);
            SessionInfo sessionInfo = new SessionInfo(startupMessage, jsonCache);
            sessionCommandCache.put(0, sessionInfo);
        } catch (Exception e) {
            LOGGER.warn("Failed to cache command: {}", e.getMessage());
        }
    }
    
    private QueryResult executeSpecificQuery(String sql, String databaseType, JdbcTemplate jdbcTemplate) {
        if (isSelectQuery(sql)) {
            return executeSelectQuery(sql, jdbcTemplate);
        } else if (isInsertQuery(sql)) {
            return executeInsertQuery(sql, databaseType, jdbcTemplate);
        } else if (isUpdateQuery(sql)) {
            return executeUpdateQuery(sql, jdbcTemplate);
        } else if (isDeleteQuery(sql)) {
            return executeDeleteQuery(sql, jdbcTemplate);
        } else if (isCreateQuery(sql)) {
            return executeCreateQuery(sql, databaseType, jdbcTemplate);
        } else if (isDropQuery(sql)) {
            return executeDropQuery(sql, databaseType, jdbcTemplate);
        } else if (isAlterQuery(sql)) {
            return executeAlterQuery(sql, jdbcTemplate);
        } else if (isShowQuery(sql)) {
            return handleShowQuery(sql, databaseType, jdbcTemplate);
        } else if (isUseQuery(sql)) {
            return executeUseQuery(sql, jdbcTemplate);
        } else {
            return executeGenericQuery(sql, jdbcTemplate);
        }
    }
    
    private QueryResult executeSelectQuery(String sql, JdbcTemplate jdbcTemplate) {
        List<Map<String, Object>> results = jdbcTemplate.queryForList(sql);
        
        if (results.isEmpty()) {
            return new QueryResult(true, new String[0], new String[0][0], "SELECT 0");
        }
        
        String[] columns = extractColumns(results.get(0));
        String[][] rows = extractRows(results, columns);
        
        return new QueryResult(true, columns, rows, "SELECT " + results.size());
    }
    
    private QueryResult executeInsertQuery(String sql, String databaseType, JdbcTemplate jdbcTemplate) {
        int rowsAffected = jdbcTemplate.update(sql);
        return new QueryResult(false, null, null, getInsertCommandTag(rowsAffected));
    }
    
    private QueryResult executeUpdateQuery(String sql, JdbcTemplate jdbcTemplate) {
        int rowsAffected = jdbcTemplate.update(sql);
        return new QueryResult(false, null, null, "UPDATE " + rowsAffected);
    }
    
    private QueryResult executeDeleteQuery(String sql, JdbcTemplate jdbcTemplate) {
        int rowsAffected = jdbcTemplate.update(sql);
        return new QueryResult(false, null, null, "DELETE " + rowsAffected);
    }
    
    private QueryResult executeCreateQuery(String sql, String databaseType, JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute(sql);
        return new QueryResult(false, null, null, getCreateCommandTag(sql));
    }
    
    private QueryResult executeDropQuery(String sql, String databaseType, JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute(sql);
        return new QueryResult(false, null, null, getDropCommandTag(sql));
    }
    
    private QueryResult executeAlterQuery(String sql, JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute(sql);
        return new QueryResult(false, null, null, "ALTER TABLE");
    }
    
    private QueryResult executeUseQuery(String sql, JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute(sql);
        return new QueryResult(false, null, null, "USE");
    }
    
    private QueryResult executeGenericQuery(String sql, JdbcTemplate jdbcTemplate) {
        jdbcTemplate.execute(sql);
        return new QueryResult(false, null, null, "OK");
    }
    
    private String[] extractColumns(Map<String, Object> firstRow) {
        return firstRow.keySet().toArray(new String[0]);
    }
    
    private String[][] extractRows(List<Map<String, Object>> results, String[] columns) {
        String[][] rows = new String[results.size()][columns.length];
        for (int i = 0; i < results.size(); i++) {
            Map<String, Object> row = results.get(i);
            for (int j = 0; j < columns.length; j++) {
                Object value = row.get(columns[j]);
                rows[i][j] = value != null ? value.toString() : null;
            }
        }
        return rows;
    }

    private String translateQuery(String sql, String databaseType) {
        DatabaseType dbType = DatabaseType.fromString(databaseType);
        
        switch (dbType) {
            case MYSQL:
                return translateToMySQL(sql);
            case ORACLE:
                return translateToOracle(sql);
            case SQLSERVER:
                return translateToSQLServer(sql);
            case POSTGRESQL:
            default:
                return sql;
        }
    }

    private String translateToMySQL(String sql) {
        return sql.replaceAll("(?i)\\bBOOLEAN\\b", "TINYINT(1)")
                  .replaceAll("(?i)\\bSERIAL\\b", "INT AUTO_INCREMENT")
                  .replaceAll("(?i)\\bTEXT\\b", "LONGTEXT")
                  .replaceAll("(?i)\\bNOW\\(\\)", "NOW()")
                  .replaceAll("(?i)\\bCURRENT_TIMESTAMP\\b", "CURRENT_TIMESTAMP()")
                  .replaceAll("(?i)\\bCURRENT_DATE\\b", "CURDATE()")
                  .replaceAll("(?i)\\bCURRENT_TIME\\b", "CURTIME()")
                  .replaceAll("(?i)\\bOFFSET\\s+(\\d+)\\s+LIMIT\\s+(\\d+)", "LIMIT $1, $2");
    }

    private String translateToOracle(String sql) {
        String translatedSql = sql.replaceAll("(?i)\\bBOOLEAN\\b", "NUMBER(1)")
                                 .replaceAll("(?i)\\bSERIAL\\b", "NUMBER GENERATED BY DEFAULT AS IDENTITY")
                                 .replaceAll("(?i)\\bTEXT\\b", "CLOB")
                                 .replaceAll("(?i)\\bVARCHAR\\b", "VARCHAR2")
                                 .replaceAll("(?i)\\bNOW\\(\\)", "SYSDATE")
                                 .replaceAll("(?i)\\bCURRENT_TIMESTAMP\\b", "CURRENT_TIMESTAMP");

        // Handle LIMIT clause for Oracle
        if (translatedSql.toLowerCase().contains("limit")) {
            translatedSql = translatedSql.replaceAll("(?i)\\bLIMIT\\s+(\\d+)", "AND ROWNUM <= $1");
            if (!translatedSql.toLowerCase().contains("where")) {
                translatedSql = translatedSql.replaceAll("(?i)\\bAND ROWNUM", "WHERE ROWNUM");
            }
        }

        return translatedSql;
    }

    private String translateToSQLServer(String sql) {
        String translatedSql = sql.replaceAll("(?i)\\bBOOLEAN\\b", "BIT")
                                 .replaceAll("(?i)\\bSERIAL\\b", "INT IDENTITY(1,1)")
                                 .replaceAll("(?i)\\bTEXT\\b", "NVARCHAR(MAX)")
                                 .replaceAll("(?i)\\bNOW\\(\\)", "GETDATE()")
                                 .replaceAll("(?i)\\bCURRENT_TIMESTAMP\\b", "GETDATE()")
                                 .replace("\\|\\|", "+");

        // Handle LIMIT clause for SQL Server
        if (translatedSql.toLowerCase().contains("limit")) {
            translatedSql = translatedSql.replaceAll("(?i)SELECT\\s+", "SELECT TOP ");
            translatedSql = translatedSql.replaceAll("(?i)\\bLIMIT\\s+(\\d+)", "$1");
        }

        return translatedSql;
    }

	private boolean isSelectQuery(String sql) {
		String lowerSql = sql.toLowerCase().trim();
		return lowerSql.startsWith("select") || lowerSql.startsWith("with");
	}

	private boolean isInsertQuery(String sql) {
		return sql.toLowerCase().trim().startsWith("insert");
	}

	private boolean isUpdateQuery(String sql) {
		return sql.toLowerCase().trim().startsWith("update");
	}

	private boolean isDeleteQuery(String sql) {
		return sql.toLowerCase().trim().startsWith("delete");
	}

	private boolean isCreateQuery(String sql) {
		return sql.toLowerCase().trim().startsWith("create");
	}

	private boolean isDropQuery(String sql) {
		return sql.toLowerCase().trim().startsWith("drop");
	}

	private boolean isAlterQuery(String sql) {
		return sql.toLowerCase().trim().startsWith("alter");
	}

	private boolean isShowQuery(String sql) {
		String lowerSql = sql.toLowerCase().trim();
		return lowerSql.startsWith("show") || lowerSql.startsWith("describe") || lowerSql.startsWith("desc");
	}

	private boolean isUseQuery(String sql) {
		return sql.toLowerCase().trim().startsWith("use");
	}

	private QueryResult handleShowQuery(String sql, String databaseType, JdbcTemplate jdbcTemplate) {
        try {
            String lowerSql = sql.toLowerCase().trim();

            if (lowerSql.startsWith("show tables")) {
                return handleShowTables(databaseType, jdbcTemplate);
            } else if (lowerSql.startsWith("show databases") || lowerSql.startsWith("show schemas")) {
                return handleShowDatabases(databaseType, jdbcTemplate);
            } else if (lowerSql.startsWith("describe") || lowerSql.startsWith("desc")) {
                return handleDescribeTable(sql, databaseType, jdbcTemplate);
            } else if (lowerSql.startsWith("show columns")) {
                return handleShowColumns(sql, databaseType, jdbcTemplate);
            } else {
                return executeSelectQuery(sql, jdbcTemplate);
            }
        } catch (Exception e) {
            LOGGER.error("Error executing SHOW query: {}", e.getMessage(), e);
            throw new DatabaseServiceException("Error executing SHOW query: " + e.getMessage(), e);
        }
    }

	private QueryResult handleShowTables(String databaseType, JdbcTemplate jdbcTemplate) {
        String query = getShowTablesQuery(databaseType);
        List<Map<String, Object>> results = jdbcTemplate.queryForList(query);

        if (results.isEmpty()) {
            return new QueryResult(true, new String[]{"Tables"}, new String[0][0], "SELECT 0");
        }

        String[] columns = {"Tables"};
        String[][] rows = new String[results.size()][1];

        for (int i = 0; i < results.size(); i++) {
            Map<String, Object> row = results.get(i);
            Object value = row.values().iterator().next();
            rows[i][0] = value != null ? value.toString() : null;
        }

        return new QueryResult(true, columns, rows, "SELECT " + results.size());
    }
	
	private String getShowTablesQuery(String databaseType) {
        DatabaseType dbType = DatabaseType.fromString(databaseType);
        
        switch (dbType) {
            case MYSQL:
                return "SHOW TABLES";
            case ORACLE:
                return "SELECT table_name FROM user_tables";
            case SQLSERVER:
                return "SELECT name FROM sys.tables";
            case POSTGRESQL:
            default:
                return "SELECT tablename FROM pg_tables WHERE schemaname = 'public'";
        }
    }

	private QueryResult handleShowDatabases(String databaseType, JdbcTemplate jdbcTemplate) {
        String query = getShowDatabasesQuery(databaseType);
        List<Map<String, Object>> results = jdbcTemplate.queryForList(query);

        if (results.isEmpty()) {
            return new QueryResult(true, new String[]{"Database"}, new String[0][0], "SELECT 0");
        }

        String[] columns = {"Database"};
        String[][] rows = new String[results.size()][1];

        for (int i = 0; i < results.size(); i++) {
            Map<String, Object> row = results.get(i);
            Object value = row.values().iterator().next();
            rows[i][0] = value != null ? value.toString() : null;
        }

        return new QueryResult(true, columns, rows, "SELECT " + results.size());
    }

    private String getShowDatabasesQuery(String databaseType) {
        DatabaseType dbType = DatabaseType.fromString(databaseType);
        
        switch (dbType) {
            case MYSQL:
                return "SHOW DATABASES";
            case ORACLE:
                return "SELECT name FROM v$database";
            case SQLSERVER:
                return "SELECT name FROM sys.databases";
            case POSTGRESQL:
            default:
                return "SELECT datname FROM pg_database WHERE datistemplate = false";
        }
    }

    private QueryResult handleDescribeTable(String sql, String databaseType, JdbcTemplate jdbcTemplate) {
        String tableName = extractTableName(sql);
        if (tableName.isEmpty()) {
            throw new IllegalArgumentException("Invalid DESCRIBE command: table name not found");
        }

        String query = getDescribeTableQuery(tableName, databaseType);
        List<Map<String, Object>> results = jdbcTemplate.queryForList(query);

        if (results.isEmpty()) {
            return new QueryResult(true, new String[0], new String[0][0], "SELECT 0");
        }

        String[] columns = extractColumns(results.get(0));
        String[][] rows = extractRows(results, columns);

        return new QueryResult(true, columns, rows, "SELECT " + results.size());
    }
    
    private String getDescribeTableQuery(String tableName, String databaseType) {
        DatabaseType dbType = DatabaseType.fromString(databaseType);
        
        switch (dbType) {
            case MYSQL:
                return "DESCRIBE " + sanitizeTableName(tableName);
            case ORACLE:
                return "SELECT column_name, data_type, nullable FROM user_tab_columns WHERE table_name = '" 
                       + sanitizeTableName(tableName).toUpperCase() + "'";
            case SQLSERVER:
                return "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" 
                       + sanitizeTableName(tableName) + "'";
            case POSTGRESQL:
            default:
                return "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '" 
                       + sanitizeTableName(tableName) + "'";
        }
    }

    private QueryResult handleShowColumns(String sql, String databaseType, JdbcTemplate jdbcTemplate) {
        String tableName = extractTableNameFromShowColumns(sql);
        return handleDescribeTable("DESCRIBE " + tableName, databaseType, jdbcTemplate);
    }

    private String extractTableName(String sql) {
        String[] parts = sql.trim().split("\\s+");
        if (parts.length >= 2) {
            return parts[1].replace(";", "");
        }
        return "";
    }

    private String extractTableNameFromShowColumns(String sql) {
        String[] parts = sql.trim().split("\\s+");
        for (int i = 0; i < parts.length; i++) {
            if ("from".equalsIgnoreCase(parts[i]) && i + 1 < parts.length) {
                return parts[i + 1].replace(";", "");
            }
        }
        return "";
    }

    private String sanitizeTableName(String tableName) {
        // Basic sanitization - in production, use proper SQL escaping
        return tableName.replaceAll("\\W", "");
    }

    private String getInsertCommandTag(int rowsAffected) {
        return "INSERT 0 " + rowsAffected;
    }

    private String getCreateCommandTag(String sql) {
        String lowerSql = sql.toLowerCase();
        if (lowerSql.contains("table")) return "CREATE TABLE";
        if (lowerSql.contains("index")) return "CREATE INDEX";
        if (lowerSql.contains("database")) return "CREATE DATABASE";
        if (lowerSql.contains("schema")) return "CREATE SCHEMA";
        if (lowerSql.contains("view")) return "CREATE VIEW";
        if (lowerSql.contains("procedure")) return "CREATE PROCEDURE";
        if (lowerSql.contains("function")) return "CREATE FUNCTION";
        return "CREATE";
    }

    private String getDropCommandTag(String sql) {
        String lowerSql = sql.toLowerCase();
        if (lowerSql.contains("table")) return "DROP TABLE";
        if (lowerSql.contains("index")) return "DROP INDEX";
        if (lowerSql.contains("database")) return "DROP DATABASE";
        if (lowerSql.contains("schema")) return "DROP SCHEMA";
        if (lowerSql.contains("view")) return "DROP VIEW";
        if (lowerSql.contains("procedure")) return "DROP PROCEDURE";
        if (lowerSql.contains("function")) return "DROP FUNCTION";
        return "DROP";
    }

    public boolean databaseExists(String database) {
        if (database == null || database.trim().isEmpty()) {
            return false;
        }
        
        try {
        	defaultJdbcTemplate = getJdbcTemplateForQuery(database);
            String query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?";
            List<String> results = defaultJdbcTemplate.queryForList(query, String.class, database);
            boolean exists = !results.isEmpty();
            LOGGER.debug("Database '{}' exists: {}", database, exists);
            return exists;
        } catch (Exception e) {
            LOGGER.error("Error checking database existence for '{}': {}", database, e.getMessage());
            return false;
        }
    }

    public String[] listDatabases() {
        try {
            String query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA " +
                          "WHERE SCHEMA_NAME NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys')";
            List<String> databases = defaultJdbcTemplate.queryForList(query, String.class);
            LOGGER.debug("Available databases: {}", databases);
            return databases.toArray(new String[0]);
        } catch (Exception e) {
            LOGGER.error("Error listing databases: {}", e.getMessage());
            return new String[]{"java1711042024"};
        }
    }

    public QueryResults executeMysqlQuery(String query, String database) {
        if (query == null || query.trim().isEmpty()) {
            throw new IllegalArgumentException("Query cannot be null or empty");
        }
        
        QueryResults result = new QueryResults();

        try {
            JdbcTemplate jdbcTemplate = getJdbcTemplateForQuery(database);

            String trimmedQuery = query.trim().toLowerCase();
            if (isReadOnlyQuery(trimmedQuery)) {
                result.isSelect = true;
                executeReadOnlyQuery(query, jdbcTemplate, result);
            } else {
                result.isSelect = false;
                result.affectedRows = jdbcTemplate.update(query);
            }

            LOGGER.debug("Query executed successfully on database: {}", database);

        } catch (Exception e) {
            LOGGER.error("Query execution failed on database '{}': {}", database, e.getMessage());
            throw new DatabaseServiceException("Query execution failed: " + e.getMessage(), e);
        }

        return result;
    }

    private JdbcTemplate getJdbcTemplateForQuery(String database) {
        if (database != null && !database.trim().isEmpty()) {
            LOGGER.debug("Executing query in database: {}", database);
            return getJdbcTemplateForDatabase(database);
        } else {
            return defaultJdbcTemplate;
        }
    }

    private boolean isReadOnlyQuery(String trimmedQuery) {
        return trimmedQuery.startsWith("select") || trimmedQuery.startsWith("show") ||
               trimmedQuery.startsWith("describe") || trimmedQuery.startsWith("desc");
    }

    private void executeReadOnlyQuery(String query, JdbcTemplate jdbcTemplate, QueryResults result) {
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(query);

        if (!rows.isEmpty()) {
            Set<String> columnSet = rows.get(0).keySet();
            result.columns = columnSet.toArray(new String[0]);

            result.rows = new String[rows.size()][];
            for (int i = 0; i < rows.size(); i++) {
                Map<String, Object> row = rows.get(i);
                result.rows[i] = new String[result.columns.length];
                for (int j = 0; j < result.columns.length; j++) {
                    Object value = row.get(result.columns[j]);
                    result.rows[i][j] = value != null ? value.toString() : null;
                }
            }
        } else {
            result.columns = new String[0];
            result.rows = new String[0][];
        }
    }

    private JdbcTemplate getJdbcTemplateForDatabase(String database) {
        return connectionCache.computeIfAbsent(database, this::createJdbcTemplateForDatabase);
    }

    private JdbcTemplate createJdbcTemplateForDatabase(String database) {
        try {
            LOGGER.debug("Creating new database connection for: {}", database);

            String url = String.format(DB_URL_TEMPLATE, DB_HOST, DB_PORT, database);

            HikariConfig config = createHikariConfig(url);
            HikariDataSource dataSource = new HikariDataSource(config);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

            LOGGER.debug("Successfully created connection to database: {}", database);
            return jdbcTemplate;

        } catch (Exception e) {
            LOGGER.error("Failed to create connection to database '{}': {}", database, e.getMessage());
            throw new DatabaseServiceException("Failed to create database connection for: " + database, e);
        }
    }

    private HikariConfig createHikariConfig(String url) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(DB_USERNAME);
        config.setPassword(DB_PASSWORD);
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");

        // Connection pool settings
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);

        // Test connection
        config.setConnectionTestQuery("SELECT 1");

        return config;
    }

    // Enums and Inner Classes
    private enum DatabaseType {
        MYSQL("mysql"),
        ORACLE("oracle"),
        SQLSERVER("sqlserver"),
        POSTGRESQL("postgresql");

        private final String value;

        DatabaseType(String value) {
            this.value = value;
        }

        public static DatabaseType fromString(String value) {
            for (DatabaseType type : values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            return POSTGRESQL; // default
        }
    }

    public static class QueryResult {
		public boolean isSelect;
		public String[] columns;
		public String[][] rows;
		public String commandTag;

		public QueryResult(boolean isSelect, String[] columns, String[][] rows, String commandTag) {
			this.isSelect = isSelect;
			this.columns = columns;
			this.rows = rows;
			this.commandTag = commandTag;
		}
	}
	
	public static class SessionInfo {
	    private final String commandCache;
	    private final StartupMessage startupMessage;
	    
	    public SessionInfo(StartupMessage startupMessage, String commandCache) {
	        this.commandCache = commandCache;
	        this.startupMessage = startupMessage;
	    }
	    
	    public StartupMessage getStartupMessage() { return startupMessage; }
	    public String getCommandCache() { return commandCache; }
	}

    // Custom exception class
    public static class DatabaseServiceException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public DatabaseServiceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}