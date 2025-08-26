package com.example.pam.db.proxy.gateway.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import com.example.pam.db.proxy.gateway.server.DatabaseService.QueryResults;
import com.example.pam.db.proxy.gateway.server.EnhancedMultiDatabaseService.SessionInfo;

@Service
public class EnhancedMultiDatabaseProxyServer {
	
    private static final int POSTGRES_PORT = 5433;
    private static final int MYSQL_PORT = 3307;
    private static final int ORACLE_PORT = 1522;
    private static final int SQLSERVER_PORT = 1434;
    
    private static final String EXPECTED_USERNAME = "chandra";
    private static final String DEFAULT_DATABASE = "java1711042024";
    
    private final byte[] scramble = new byte[20];
    private final AtomicInteger connectionCounter = new AtomicInteger(1);
    
    // Database configuration for multiple databases per type
    private static final Map<String, DatabaseConfig> DB_CONFIGS = new HashMap<String, DatabaseConfig>() {{
        put("postgresql", new DatabaseConfig(
            "org.postgresql.Driver",
            "jdbc:postgresql://localhost:5432/",
            "postgres",
            "root"
        ));
        put("mysql", new DatabaseConfig(
            "com.mysql.cj.jdbc.Driver",
            "jdbc:mysql://localhost:3306/",
            "root",
            "root"
        ));
        put("oracle", new DatabaseConfig(
            "oracle.jdbc.driver.OracleDriver",
            "jdbc:oracle:thin:@localhost:1521:",
            "system",
            "oracle"
        ));
        put("sqlserver", new DatabaseConfig(
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "jdbc:sqlserver://localhost:1433;databaseName=",
            "sa",
            "sa"
        ));
    }};
    
    @Autowired
    private EnhancedMultiDatabaseService databaseService;
    
    // Cache for database connections
    private final Map<String, JdbcTemplate> connectionCache = new ConcurrentHashMap<>();
    
    public void start() {
        // Start PostgreSQL proxy
        startProxy(POSTGRES_PORT, "postgresql");
        
        // Start MySQL proxy
        startProxy(MYSQL_PORT, "mysql");
        
        // Start Oracle proxy
        startProxy(ORACLE_PORT, "oracle");
        
        // Start SQL Server proxy
        startProxy(SQLSERVER_PORT, "sqlserver");
    }
    
    private void startProxy(int port, String databaseType) {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                System.out.println(databaseType.toUpperCase() + " Jump Server listening on port " + port);
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(() -> handleClient(clientSocket, databaseType)).start();
                }
            } catch (IOException e) {
                System.err.println("Error starting " + databaseType + " proxy on port " + port + ": " + e.getMessage());
            }
        }).start();
    }
    
    private void handleClient(Socket socket, String databaseType) {
        try {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            
            String token = null;
            String targetDatabase = null;
            if (databaseType.equals("postgresql")) {
            	handleSSLRequest(in, out);
            	
            	StartupMessage startupMessage = readStartupMessage(in, databaseType);
                if (startupMessage == null) {
                    socket.close();
                    return;
                }
                
                String dbWithToken = startupMessage.database;
                if (dbWithToken != null && dbWithToken.contains("#")) {
                    String[] parts = dbWithToken.split("#", 2);
                    targetDatabase = parts[0];
                    token = parts[1];
                }
                if (targetDatabase == null || targetDatabase.trim().isEmpty()) {
                    targetDatabase = getDefaultDatabase(databaseType);
                }
                
                // Authenticate based on database type
                AuthenticationResult authResult = authenticateUser(startupMessage, databaseType, targetDatabase, token);
                if (!authResult.success) {
                    sendErrorResponse(out, "28P01", authResult.errorMessage);
                    socket.close();
                    return;
                }
                
                sendAuthenticationOK(out);
                sendBackendKeyData(out);
                sendReadyForQuery(out, 'I'); // 'I' = idle
                String connectionKey = databaseType + ":" + targetDatabase;
                handleQueries(in, out, socket, databaseType, targetDatabase, connectionKey, startupMessage);
            } else if (databaseType.equals("mysql")) {
                handleMySQLAuth(socket);
            } else {
                sendAuthenticationOK(out);
                sendBackendKeyData(out);
                sendReadyForQuery(out, 'I');
            }
        } catch (IOException e) {
            System.err.println("Client disconnected: " + e.getMessage());
        } finally {
            try {
                socket.close();
                
                Map<Integer, SessionInfo> sessioncommandcache = EnhancedMultiDatabaseService.sessionCommandCache;
                SessionInfo sessionInfo = sessioncommandcache.get(0);
                JdbcTemplate jdbcTemplate = getOrCreateConnection("postgresql", "securepass_450", "");
                
                if(sessionInfo != null && sessionInfo.getCommandCache() != null) {
                	logCommand(sessionInfo.getCommandCache().toString(), sessionInfo.getStartupMessage().getConnectedUser(), 
                    		sessionInfo.getStartupMessage().getConnectionName(), jdbcTemplate);
                }
            } catch (IOException ignored) {}
        }
    }
    
    private void handleMySQLAuth(Socket socket) {
    	int sequenceId = 0;
        ConnectionState connectionState = null;
        
        try {
            socket.setSoTimeout(30000);
            socket.setKeepAlive(true);
            socket.setTcpNoDelay(true);

            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            // Generate scramble for authentication
            generateScramble();
            
            sequenceId = sendInitialHandshake(out, sequenceId);

            AuthResult authResult = handleAutoAuthentication(in, out, sequenceId);
            if (authResult.sequenceId == -1) {
                System.out.println("Authentication failed");
                return;
            }
            
            sequenceId = authResult.sequenceId;
            connectionState = authResult.connectionState;
            
            System.out.println("Authentication successful for user: " + connectionState.getUsername());
            System.out.println("Auto-connected to database: " + connectionState.getCurrentDatabase());

            // Handle queries with connection state
            handleQueries(in, out, socket, connectionState);

        } catch (IOException e) {
            if (!isExpectedDisconnectError(e)) {
                System.err.println("Client handling error: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                System.err.println("Error closing socket: " + e.getMessage());
            }
        }
    }
    
    private void sendMySQLAuthRequest(OutputStream out) throws IOException {
        // Send authentication method request for MySQL
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte) 'R'); // Authentication message type
        buffer.putInt(8); // Message length
        buffer.putInt(3); // Authentication method (3 = clear text password)
        
        out.write(buffer.array());
        out.flush();
    }
    
    private byte[] readAuthResponse(InputStream in) throws IOException {
        // Read authentication response
        byte[] lengthBytes = new byte[4];
        if (in.read(lengthBytes) != 4) return new byte[0];
        
        int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        
        byte[] response = new byte[length - 4];
        if (in.read(response) != response.length) return new byte[0];
        
        return response;
    }
    
    private String getDefaultDatabase(String databaseType) {
        switch (databaseType.toLowerCase()) {
            case "mysql":
                return "mysql";
            case "oracle":
                return "xe";
            case "sqlserver":
                return "master";
            default:
                return "postgres";
        }
    }
    
    private AuthenticationResult authenticateUser(StartupMessage startupMessage, String databaseType, String targetDatabase, String token) {
        DatabaseConfig config = DB_CONFIGS.get(databaseType.toLowerCase());
        if (config == null) {
            return new AuthenticationResult(false, "Unsupported database type: " + databaseType);
        }
        
        // Check username
        if (!config.defaultUsername.equals(startupMessage.username)) {
            return new AuthenticationResult(false, "authentication failed for user \"" + startupMessage.username + "\"");
        }
        
        if(isValidTokenDate(token) && authenticateToken(token, databaseType, targetDatabase, startupMessage)) {
			// Send authentication success response
			return new AuthenticationResult(true, null);
        } else {
			return new AuthenticationResult(false, "Invalid token provided");
		}
    }
    
    private boolean authenticateToken(String token, String databaseType, String targetDatabase, StartupMessage startupMessage) {
    	JdbcTemplate jdbcTemplate = getOrCreateConnection(databaseType, targetDatabase, "initialConnection");
    	
		String sql = "SELECT connection_name, securepass_user_id FROM data_base_mapping WHERE user_token = ? AND status = 'LIVE'";
		try {
	        Map<String, Object> result = jdbcTemplate.queryForMap(sql, token);
	        
	        if (result != null && !result.isEmpty()) {
	            String connectionName = (String) result.get("connection_name");
	            Integer userId = (Integer) result.get("securepass_user_id");
	            startupMessage.setConnectionName(connectionName);
	            startupMessage.setConnectedUser(userId);
	        }
	        return true;
	    } catch (Exception e) {
	        System.err.println("Error validating token: " + e.getMessage());
	        return false;
	    }
	}

	public static boolean isValidTokenDate(String token) {
        if (token == null || !token.contains("#")) {
            return false;
        }

        String[] parts = token.split("#");
        if (parts.length != 2) {
            return false;
        }

        String dateStr = parts[1];  // e.g., "20250717"
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        try {
        	LocalDate tokenDate = LocalDate.parse(dateStr, formatter);
            LocalDate today = LocalDate.now();
            if (tokenDate.isAfter(today)) {
                return true; // Valid token
            } else {
                return false; // Expired or invalid date
            }
        } catch (DateTimeParseException e) {
            return false;
        }
    }

	private boolean handleSSLRequest(InputStream in, OutputStream out) throws IOException {
        if (in.markSupported()) {
            in.mark(8);
        }
        
        byte[] buffer = new byte[8];
        int bytesRead = in.read(buffer);
        
        if (bytesRead == 8) {
            ByteBuffer bb = ByteBuffer.wrap(buffer).order(ByteOrder.BIG_ENDIAN);
            int length = bb.getInt();
            int sslCode = bb.getInt();
            
            if (length == 8 && sslCode == 80877103) {
                out.write('N');
                out.flush();
                return true;
            }
        }
        
        if (in.markSupported()) {
            in.reset();
        } else {
            throw new IOException("SSL negotiation failed - please disable SSL and reconnect");
        }
        
        return false;
    }
    
    private StartupMessage readStartupMessage(InputStream in, String databaseType) throws IOException {
        // Read length
        byte[] lengthBytes = new byte[4];
        if (in.read(lengthBytes) != 4) return null;
        
        int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        
        // Read protocol version
        byte[] protocolBytes = new byte[4];
        if (in.read(protocolBytes) != 4) return null;
        
        int protocolVersion = ByteBuffer.wrap(protocolBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        
        // Read parameters
        byte[] paramBytes = new byte[length - 8];
        if (in.read(paramBytes) != paramBytes.length) return null;
        
        Map<String, String> parameters = new HashMap<>();
        String paramString = new String(paramBytes, StandardCharsets.UTF_8);
        String[] pairs = paramString.split("\0");
        
        for (int i = 0; i < pairs.length - 1; i += 2) {
            if (i + 1 < pairs.length && !pairs[i].isEmpty()) {
                parameters.put(pairs[i], pairs[i + 1]);
            }
        }
        
        return new StartupMessage(protocolVersion, parameters.get("user"), parameters.get("database"), parameters);
    }
    
    private void handleQueries(InputStream in, OutputStream out, Socket socket, String databaseType, String targetDatabase, String connectionKey, StartupMessage startupMessage) throws IOException {
        while (!socket.isClosed()) {
            int messageType = in.read();
            if (messageType == -1) break;
            
            switch (messageType) {
                case 'Q': // Simple query
                    handleSimpleQuery(in, out, databaseType, targetDatabase, connectionKey, startupMessage);
                    break;
                case 'X': // Terminate
                    socket.close();
                    return;
                default:
                    skipMessage(in);
            }
        }
    }
    
    private void handleSimpleQuery(InputStream in, OutputStream out, String databaseType, String targetDatabase, String connectionKey, StartupMessage startupMessage) throws IOException {
        // Read message length
        byte[] lengthBytes = new byte[4];
        if (in.read(lengthBytes) != 4) return;
        
        int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        
        // Read query
        byte[] queryBytes = new byte[length - 4];
        if (in.read(queryBytes) != queryBytes.length) return;
        
        String query = new String(queryBytes, StandardCharsets.UTF_8).replace("\0", "");
        
        try {
            // Get or create connection for this specific database
            JdbcTemplate jdbcTemplate = getOrCreateConnection(databaseType, targetDatabase, connectionKey);
            
            // Execute query with database-specific connection
            EnhancedMultiDatabaseService.QueryResult result = databaseService.executeQuery(query, databaseType, jdbcTemplate, startupMessage);
            
            if (result.isSelect) {
                sendRowDescription(out, result.columns);
                sendDataRows(out, result.rows);
                sendCommandComplete(out, "SELECT " + (result.rows != null ? result.rows.length : 0));
            } else {
                sendCommandComplete(out, result.commandTag);
            }
            
        } catch (Exception e) {
            sendErrorResponse(out, "42000", "Error executing query: " + e.getMessage());
        }
        
        sendReadyForQuery(out, 'I');
    }
    
    private JdbcTemplate getOrCreateConnection(String databaseType, String targetDatabase, String connectionKey) {
        return connectionCache.computeIfAbsent(connectionKey, key -> {
            DatabaseConfig config = DB_CONFIGS.get(databaseType.toLowerCase());
            if (config == null) {
                throw new RuntimeException("Unsupported database type: " + databaseType);
            }
            
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName(config.driverClass);
            dataSource.setUrl(config.jdbcUrl + targetDatabase);
            dataSource.setUsername(config.defaultUsername);
            dataSource.setPassword(config.defaultPassword);
            
            return new JdbcTemplate(dataSource);
        });
    }
    
    // ... (keep all the existing send* methods unchanged)
    
    private void sendAuthenticationOK(OutputStream out) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(9);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte) 'R');
        buffer.putInt(8);
        buffer.putInt(0);
        
        out.write(buffer.array());
        out.flush();
    }
    
    private void sendBackendKeyData(OutputStream out) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(13);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte) 'K');
        buffer.putInt(12);
        buffer.putInt(12345);
        buffer.putInt(54321);
        
        out.write(buffer.array());
        out.flush();
    }
    
    private void sendReadyForQuery(OutputStream out, char status) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(6);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.put((byte) 'Z');
        buffer.putInt(5);
        buffer.put((byte) status);
        
        out.write(buffer.array());
        out.flush();
    }
    
    private void sendErrorResponse(OutputStream out, String sqlState, String message) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write('E');
        
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write('S');
        body.write("ERROR\0".getBytes(StandardCharsets.UTF_8));
        body.write('C');
        body.write((sqlState + "\0").getBytes(StandardCharsets.UTF_8));
        body.write('M');
        body.write((message + "\0").getBytes(StandardCharsets.UTF_8));
        body.write(0);
        
        byte[] bodyBytes = body.toByteArray();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.order(ByteOrder.BIG_ENDIAN);
        lengthBuffer.putInt(bodyBytes.length + 4);
        
        buffer.write(lengthBuffer.array());
        buffer.write(bodyBytes);
        
        out.write(buffer.toByteArray());
        out.flush();
    }
    
    private void sendRowDescription(OutputStream out, String[] columns) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write('T');
        
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        
        ByteBuffer fieldCountBuffer = ByteBuffer.allocate(2);
        fieldCountBuffer.order(ByteOrder.BIG_ENDIAN);
        fieldCountBuffer.putShort((short) columns.length);
        body.write(fieldCountBuffer.array());
        
        for (String column : columns) {
            body.write(column.getBytes(StandardCharsets.UTF_8));
            body.write(0);
            body.write(new byte[18]);
        }
        
        byte[] bodyBytes = body.toByteArray();
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.order(ByteOrder.BIG_ENDIAN);
        lengthBuffer.putInt(bodyBytes.length + 4);
        
        buffer.write(lengthBuffer.array());
        buffer.write(bodyBytes);
        
        out.write(buffer.toByteArray());
        out.flush();
    }
    
    private void sendDataRows(OutputStream out, String[][] rows) throws IOException {
        for (String[] row : rows) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            buffer.write('D');
            
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            
            ByteBuffer columnCountBuffer = ByteBuffer.allocate(2);
            columnCountBuffer.order(ByteOrder.BIG_ENDIAN);
            columnCountBuffer.putShort((short) row.length);
            body.write(columnCountBuffer.array());
            
            for (String value : row) {
                if (value == null) {
                    ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                    lengthBuffer.order(ByteOrder.BIG_ENDIAN);
                    lengthBuffer.putInt(-1);
                    body.write(lengthBuffer.array());
                } else {
                    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                    ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                    lengthBuffer.order(ByteOrder.BIG_ENDIAN);
                    lengthBuffer.putInt(valueBytes.length);
                    body.write(lengthBuffer.array());
                    body.write(valueBytes);
                }
            }
            
            byte[] bodyBytes = body.toByteArray();
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            lengthBuffer.order(ByteOrder.BIG_ENDIAN);
            lengthBuffer.putInt(bodyBytes.length + 4);
            
            buffer.write(lengthBuffer.array());
            buffer.write(bodyBytes);
            
            out.write(buffer.toByteArray());
            out.flush();
        }
    }
    
    private void sendCommandComplete(OutputStream out, String commandTag) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write('C');
        
        byte[] tagBytes = (commandTag + "\0").getBytes(StandardCharsets.UTF_8);
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        lengthBuffer.order(ByteOrder.BIG_ENDIAN);
        lengthBuffer.putInt(tagBytes.length + 4);
        
        buffer.write(lengthBuffer.array());
        buffer.write(tagBytes);
        
        out.write(buffer.toByteArray());
        out.flush();
    }
    
    private void skipMessage(InputStream in) throws IOException {
        byte[] lengthBytes = new byte[4];
        if (in.read(lengthBytes) != 4) return;
        
        int length = ByteBuffer.wrap(lengthBytes).order(ByteOrder.BIG_ENDIAN).getInt();
        in.skip(length - 4);
    }
    
    // Helper classes
    private static class DatabaseConfig {
        String driverClass;
        String jdbcUrl;
        String defaultUsername;
        String defaultPassword;
        
        DatabaseConfig(String driverClass, String jdbcUrl, String defaultUsername, String defaultPassword) {
            this.driverClass = driverClass;
            this.jdbcUrl = jdbcUrl;
            this.defaultUsername = defaultUsername;
            this.defaultPassword = defaultPassword;
        }
    }
    
    private static class AuthenticationResult {
        boolean success;
        String errorMessage;
        
        AuthenticationResult(boolean success, String errorMessage) {
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }
    
	private void logCommand(String command, Integer userId, String connectionName, JdbcTemplate jdbcTemplate) {
		try {
			String insertLogSql = "INSERT INTO data_base_command_logs (securepass_user_id, connection_name, command, executed_at) VALUES (?, ?, ?, ?)";
			jdbcTemplate.update(insertLogSql, userId, connectionName, command, LocalDateTime.now());
		} catch (Exception e) {
			System.err.println("Failed to log command: " + e.getMessage());
		}
	}
	
	private void generateScramble() {
        new SecureRandom().nextBytes(scramble);
    }
	
	private boolean isExpectedDisconnectError(IOException e) {
        String message = e.getMessage();
        return message != null && (
            message.contains("Connection reset") ||
            message.contains("aborted") ||
            message.contains("Broken pipe") ||
            message.contains("Connection closed") ||
            message.contains("socket write error")
        );
    }
	
	private int sendInitialHandshake(OutputStream out, int sequenceId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        
        // Protocol version
        buffer.write(10);
        
        // Server version string (null-terminated)
        buffer.write("8.0.19-proxy\0".getBytes(StandardCharsets.UTF_8));
        
        // Connection ID (4 bytes little endian)
        int connectionId = connectionCounter.getAndIncrement();
        writeInt32LE(buffer, connectionId);
        
        // Auth plugin data part 1 (first 8 bytes of scramble)
        buffer.write(Arrays.copyOfRange(scramble, 0, 8));
        buffer.write(0); // Filler
        
        // Server capabilities (lower 16 bits)
        int capabilities = 0;
        capabilities |= 0x00000001; // CLIENT_LONG_PASSWORD
        capabilities |= 0x00000002; // CLIENT_FOUND_ROWS
        capabilities |= 0x00000004; // CLIENT_LONG_FLAG
        capabilities |= 0x00000008; // CLIENT_CONNECT_WITH_DB
        capabilities |= 0x00000200; // CLIENT_PROTOCOL_41
        capabilities |= 0x00008000; // CLIENT_SECURE_CONNECTION
        capabilities |= 0x00080000; // CLIENT_PLUGIN_AUTH
        
        writeInt16LE(buffer, capabilities & 0xFFFF);
        
        // Character set (utf8mb4_0900_ai_ci = 255)
        buffer.write(255);
        
        // Status flags (2 bytes) - SERVER_STATUS_AUTOCOMMIT
        writeInt16LE(buffer, 0x0002);
        
        // Server capabilities (upper 16 bits)
        writeInt16LE(buffer, (capabilities >> 16) & 0xFFFF);
        
        // Auth plugin data length
        buffer.write(21);
        
        // Reserved (10 bytes of 0x00)
        buffer.write(new byte[10]);
        
        // Auth plugin data part 2 (remaining 12 bytes of scramble)
        buffer.write(Arrays.copyOfRange(scramble, 8, 20));
        buffer.write(0); // Null terminator
        
        // Auth plugin name - use mysql_native_password instead of caching_sha2_password
        buffer.write("mysql_native_password\0".getBytes(StandardCharsets.UTF_8));

        byte[] packet = buffer.toByteArray();
        return writePacket(out, packet, sequenceId);
    }
	
	private AuthResult handleAutoAuthentication(InputStream in, OutputStream out, int sequenceId) throws IOException {
        try {
            PacketResult packetResult = readPacket(in);
            if (packetResult.data == null) {
                System.out.println("No authentication packet received");
                return new AuthResult(-1, null);
            }

            byte[] packet = packetResult.data;
            System.out.println("Authentication packet received, auto-approving connection...");

            if (packet.length < 32) {
                System.out.println("Authentication packet too short");
                sendErrorPacket(out, 1045, "Access denied: Invalid packet", sequenceId);
                return new AuthResult(-1, null);
            }

            // Parse authentication packet to extract username and database
            ByteBuffer buffer = ByteBuffer.wrap(packet);
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            // Read client capabilities (4 bytes)
            int clientCapabilities = buffer.getInt();
            System.out.println("Client capabilities: 0x" + Integer.toHexString(clientCapabilities));

            // Skip max packet size (4 bytes)
            buffer.getInt();

            // Skip charset (1 byte)
            buffer.get();

            // Skip reserved (23 bytes)
            buffer.position(buffer.position() + 23);

            // Read username (null-terminated)
            StringBuilder username = new StringBuilder();
            byte b;
            while (buffer.hasRemaining() && (b = buffer.get()) != 0) {
                username.append((char) b);
            }

            String clientUsername = username.toString();
            System.out.println("Client username: '" + clientUsername + "'");

            // Skip password data completely - we don't need it
            if (buffer.hasRemaining()) {
                int passwordLength = buffer.get() & 0xFF;
                System.out.println("Password length: " + passwordLength);
                if (passwordLength > 0 && passwordLength <= 250) {
                    buffer.position(buffer.position() + passwordLength);
                }
            }

            // Read database name if present
            String requestedDatabase = null;
            if (buffer.hasRemaining() && (clientCapabilities & 0x00000008) != 0) {
                StringBuilder dbName = new StringBuilder();
                while (buffer.hasRemaining() && (b = buffer.get()) != 0) {
                    dbName.append((char) b);
                }
                requestedDatabase = dbName.toString();
            }

            String token = null;
            String targetDatabase = null;
            if (requestedDatabase != null && !requestedDatabase.isEmpty()) {
            	if (requestedDatabase != null && requestedDatabase.contains("#")) {
                    String[] parts = requestedDatabase.split("#", 2);
                    targetDatabase = parts[0];
                    token = parts[1];
                }
            } else {
                targetDatabase = DEFAULT_DATABASE;
            }

            // Check if the target database exists using our hardcoded credentials
            boolean dbExists = false;
            try {
                dbExists = databaseService.databaseExists(targetDatabase);
            } catch (Exception e) {
                System.err.println("Error checking database existence: " + e.getMessage());
            }

            if (!dbExists) {
                System.out.println("Database '" + targetDatabase + "' does not exist");
                sendErrorPacket(out, 1049, "Unknown database '" + targetDatabase + "'", sequenceId + 1);
                return new AuthResult(-1, null);
            }

            // AUTO-APPROVE: Send OK packet immediately without any password verification
            System.out.println("Auto-approving connection (no password required from client)");
            System.out.println("Internal authentication will use: username='" + EXPECTED_USERNAME + "', database='" + targetDatabase + "'");
            
            int newSequenceId = sendOKPacket(out, 0, 0, sequenceId + 1);

            // Create connection state - use the client's username for display purposes
            // but internally we'll use hardcoded credentials for database operations
            ConnectionState connectionState = new ConnectionState(connectionCounter.get(), clientUsername);
            connectionState.setCurrentDatabase(targetDatabase);

            System.out.println("Session created successfully for client user: " + clientUsername);
            System.out.println("Connected to database: " + targetDatabase);
            System.out.println("Ready to accept commands (password-less authentication complete)");

            return new AuthResult(newSequenceId, connectionState);

        } catch (Exception e) {
            System.err.println("Connection setup error: " + e.getMessage());
            e.printStackTrace();
            try {
                sendErrorPacket(out, 1045, "Access denied: Connection error", sequenceId);
            } catch (IOException sendError) {
                System.err.println("Error sending error packet: " + sendError.getMessage());
            }
            return new AuthResult(-1, null);
        }
    }
	
	private void handleQueries(InputStream in, OutputStream out, Socket socket, ConnectionState connectionState) throws IOException {
        int sequenceId = 0;
        
        while (!socket.isClosed()) {
            try {
                PacketResult packetResult = readPacket(in);
                if (packetResult.data == null || packetResult.data.length == 0) {
                    System.out.println("Empty packet received, client disconnected");
                    break;
                }

                byte[] packet = packetResult.data;
                int command = packet[0] & 0xFF;
                System.out.println("Received command: " + command + ", Sequence ID: " + packetResult.sequenceId);

                switch (command) {
                    case 3: // COM_QUERY
                        String query = new String(Arrays.copyOfRange(packet, 1, packet.length), StandardCharsets.UTF_8).trim();
                        System.out.println("Query: " + query);
                        sequenceId = handleQueryWithDatabase(query, out, packetResult.sequenceId + 1, connectionState);
                        break;
                    case 1: // COM_QUIT
                        System.out.println("Client quit");
                        return;
                    case 2: // COM_INIT_DB (USE database)
                        String dbName = new String(Arrays.copyOfRange(packet, 1, packet.length), StandardCharsets.UTF_8).trim();
                        System.out.println("USE database: " + dbName);
                        sequenceId = handleUseDatabase(dbName, out, packetResult.sequenceId + 1, connectionState);
                        break;
                    case 14: // COM_PING
                        sequenceId = sendOKPacket(out, 0, 0, packetResult.sequenceId + 1);
                        break;
                    case 0: // COM_SLEEP
                        sequenceId = packetResult.sequenceId + 1;
                        break;
                    default:
                        System.out.println("Unknown command: " + command);
                        sequenceId = sendErrorPacket(out, 1047, "Unknown command", packetResult.sequenceId + 1);
                }
            } catch (SocketTimeoutException e) {
                System.out.println("Client timeout");
                break;
            } catch (IOException e) {
                if (!isExpectedDisconnectError(e)) {
                    System.err.println("Query handling error: " + e.getMessage());
                }
                break;
            }
        }
    }
	
	private void writeInt32LE(ByteArrayOutputStream buffer, int value) {
        buffer.write(value & 0xFF);
        buffer.write((value >> 8) & 0xFF);
        buffer.write((value >> 16) & 0xFF);
        buffer.write((value >> 24) & 0xFF);
    }
	
	private void writeInt16LE(ByteArrayOutputStream buffer, int value) {
        buffer.write(value & 0xFF);
        buffer.write((value >> 8) & 0xFF);
    }
	
	private int writePacket(OutputStream out, byte[] packet, int sequenceId) throws IOException {
        try {
            // Write packet header
            out.write(packet.length & 0xFF);
            out.write((packet.length >> 8) & 0xFF);
            out.write((packet.length >> 16) & 0xFF);
            out.write(sequenceId & 0xFF);
            
            // Write packet data
            out.write(packet);
            out.flush();
            
            System.out.println("Sent packet - Length: " + packet.length + ", Sequence ID: " + sequenceId);
            return sequenceId + 1;
            
        } catch (IOException e) {
            System.err.println("Error writing packet (SeqID: " + sequenceId + ", Length: " + packet.length + "): " + e.getMessage());
            throw e;
        }
    }
	
	private PacketResult readPacket(InputStream in) throws IOException {
        try {
            byte[] header = new byte[4];
            int totalRead = 0;
            
            // Read complete header
            while (totalRead < 4) {
                int bytesRead = in.read(header, totalRead, 4 - totalRead);
                if (bytesRead == -1) {
                    return new PacketResult(null, -1);
                }
                totalRead += bytesRead;
            }
            
            // Parse header
            int length = (header[0] & 0xFF) | ((header[1] & 0xFF) << 8) | ((header[2] & 0xFF) << 16);
            int receivedSeqId = header[3] & 0xFF;
            
            System.out.println("Packet - Length: " + length + ", Sequence ID: " + receivedSeqId);
            
            if (length == 0) {
                return new PacketResult(new byte[0], receivedSeqId);
            }
            
            if (length > 16777215) {
                System.err.println("Packet too large: " + length);
                return new PacketResult(null, -1);
            }
            
            // Read packet body
            byte[] body = new byte[length];
            totalRead = 0;
            
            while (totalRead < length) {
                int bytesRead = in.read(body, totalRead, length - totalRead);
                if (bytesRead == -1) {
                    return new PacketResult(null, -1);
                }
                totalRead += bytesRead;
            }
            
            return new PacketResult(body, receivedSeqId);
            
        } catch (SocketTimeoutException e) {
            System.out.println("Socket timeout");
            return new PacketResult(null, -1);
        }
    }
	
	private int sendErrorPacket(OutputStream out, int errorCode, String message, int sequenceId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(0xFF); // Error packet header
        writeInt16LE(buffer, errorCode);
        buffer.write('#');
        buffer.write("HY000".getBytes(StandardCharsets.UTF_8)); // SQL state
        buffer.write(message.getBytes(StandardCharsets.UTF_8));
        return writePacket(out, buffer.toByteArray(), sequenceId);
    }
	
	private int sendOKPacket(OutputStream out, int affectedRows, int lastInsertId, int sequenceId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(0x00); // OK packet header
        buffer.write(encodeLengthCodedBinary(affectedRows));
        buffer.write(encodeLengthCodedBinary(lastInsertId));
        writeInt16LE(buffer, 0x0002); // Status flags (SERVER_STATUS_AUTOCOMMIT)
        writeInt16LE(buffer, 0); // Warning count
        return writePacket(out, buffer.toByteArray(), sequenceId);
    }
	
	private int handleQueryWithDatabase(String query, OutputStream out, int sequenceId, ConnectionState connectionState) throws IOException {
        try {
            System.out.println("Executing query on database '" + connectionState.getCurrentDatabase() + "': " + query);
            
            // Handle USE database statement in SQL
            if (query.toLowerCase().startsWith("use ")) {
                String dbName = query.substring(4).trim();
                if (dbName.endsWith(";")) {
                    dbName = dbName.substring(0, dbName.length() - 1);
                }
                // Remove quotes if present
                if ((dbName.startsWith("'") && dbName.endsWith("'")) || 
                    (dbName.startsWith("`") && dbName.endsWith("`")) ||
                    (dbName.startsWith("\"") && dbName.endsWith("\""))) {
                    dbName = dbName.substring(1, dbName.length() - 1);
                }
                return handleUseDatabase(dbName, out, sequenceId, connectionState);
            }
            
            // Handle common MySQL client initialization queries
            if (query.toLowerCase().contains("@@version_comment")) {
                String[][] rows = {{"MySQL Proxy Server"}};
                String[] columns = {"@@version_comment"};
                return sendSelectResult(out, sequenceId, columns, rows);
            }
            
            // Handle SELECT DATABASE() query
            if (query.toLowerCase().contains("select database()") || 
                query.toLowerCase().contains("@@database")) {
                String[][] rows = {{connectionState.getCurrentDatabase()}};
                String[] columns = {"database()"};
                return sendSelectResult(out, sequenceId, columns, rows);
            }
            
            // Handle SHOW DATABASES
            if (query.toLowerCase().contains("show databases")) {
                try {
                    String[] databases = databaseService.listDatabases();
                    String[] columns = {"Database"};
                    String[][] rows = new String[databases.length][];
                    for (int i = 0; i < databases.length; i++) {
                        rows[i] = new String[]{databases[i]};
                    }
                    return sendSelectResult(out, sequenceId, columns, rows);
                } catch (Exception e) {
                    System.err.println("Error listing databases: " + e.getMessage());
                    // Fallback if DatabaseService doesn't support listDatabases
                    String[] columns = {"Database"};
                    String[][] rows = {{"information_schema"}, {"mysql"}, {"performance_schema"}, {"sys"}, {"java1711042024"}};
                    return sendSelectResult(out, sequenceId, columns, rows);
                }
            }
            
            // Ensure we have a current database before executing queries
            if (connectionState.getCurrentDatabase() == null || connectionState.getCurrentDatabase().isEmpty()) {
                return sendErrorPacket(out, 1046, "No database selected", sequenceId);
            }
            
            // Handle other queries with database context
            QueryResults result = databaseService.executeMysqlQuery(query, connectionState.getCurrentDatabase());
            
            if (result.isSelect) {
                return sendSelectResult(out, sequenceId, result.columns, result.rows);
            } else {
                return sendOKPacket(out, result.affectedRows != null ? result.affectedRows : 0, 0, sequenceId);
            }

        } catch (Exception e) {
            System.err.println("Query execution error on database '" + connectionState.getCurrentDatabase() + "': " + e.getMessage());
            return sendErrorPacket(out, 1064, "SQL Error: " + e.getMessage(), sequenceId);
        }
    }
	
	private int handleUseDatabase(String dbName, OutputStream out, int sequenceId, ConnectionState connectionState) throws IOException {
        try {
            System.out.println("Attempting to switch to database: '" + dbName + "'");
            
            // Validate database exists
            boolean dbExists = false;
            try {
                dbExists = databaseService.databaseExists(dbName);
            } catch (Exception e) {
                System.err.println("Error checking database existence for '" + dbName + "': " + e.getMessage());
            }
            
            if (dbExists) {
                String previousDb = connectionState.getCurrentDatabase();
                connectionState.setCurrentDatabase(dbName);
                System.out.println("Successfully changed database from '" + previousDb + "' to '" + dbName + "'");
                return sendOKPacket(out, 0, 0, sequenceId);
            } else {
                System.out.println("Database not found: '" + dbName + "'");
                return sendErrorPacket(out, 1049, "Unknown database '" + dbName + "'", sequenceId);
            }
        } catch (Exception e) {
            System.err.println("Error changing database to '" + dbName + "': " + e.getMessage());
            return sendErrorPacket(out, 1049, "Unknown database '" + dbName + "'", sequenceId);
        }
    }
	
	private byte[] encodeLengthCodedBinary(int value) {
        if (value < 251) {
            return new byte[]{(byte) value};
        } else if (value < 65536) {
            return new byte[]{(byte) 0xFC, (byte) (value & 0xFF), (byte) ((value >> 8) & 0xFF)};
        } else if (value < 16777216) {
            return new byte[]{(byte) 0xFD, (byte) (value & 0xFF), (byte) ((value >> 8) & 0xFF), (byte) ((value >> 16) & 0xFF)};
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(9);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.put((byte) 0xFE);
            buffer.putLong(value);
            return buffer.array();
        }
    }
	
	private int sendSelectResult(OutputStream out, int sequenceId, String[] columns, String[][] rows) throws IOException {
        // Send column count
        sequenceId = writePacket(out, new byte[]{(byte) columns.length}, sequenceId);
        
        // Send column definitions
        for (String column : columns) {
            sequenceId = sendColumnDefinition(out, column, sequenceId);
        }
        
        // Send EOF packet after column definitions
        sequenceId = sendEOFPacket(out, sequenceId);
        
        // Send result rows
        for (String[] row : rows) {
            sequenceId = sendResultRow(out, row, sequenceId);
        }
        
        // Send EOF packet after rows
        return sendEOFPacket(out, sequenceId);
    }
	
	private int sendColumnDefinition(OutputStream out, String columnName, int sequenceId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        
        // Column definition packet format
        writeStringLengthCoded(buffer, "def");  // catalog
        writeStringLengthCoded(buffer, "");     // schema
        writeStringLengthCoded(buffer, "");     // table
        writeStringLengthCoded(buffer, "");     // org_table
        writeStringLengthCoded(buffer, columnName); // name
        writeStringLengthCoded(buffer, columnName); // org_name
        
        // Length of fixed fields
        buffer.write(0x0C);
        
        // Character set (utf8mb4_0900_ai_ci = 255)
        writeInt16LE(buffer, 255);
        
        // Column length (4 bytes)
        writeInt32LE(buffer, 65535);
        
        // Column type (VARCHAR = 253)
        buffer.write(253);
        
        // Flags (2 bytes)
        writeInt16LE(buffer, 0);
        
        // Decimals
        buffer.write(0);
        
        // Reserved (2 bytes)
        writeInt16LE(buffer, 0);
        
        return writePacket(out, buffer.toByteArray(), sequenceId);
    }
	
	private int sendEOFPacket(OutputStream out, int sequenceId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.write(0xFE); // EOF packet header
        writeInt16LE(buffer, 0); // Warning count
        writeInt16LE(buffer, 0x0002); // Status flags (SERVER_STATUS_AUTOCOMMIT)
        return writePacket(out, buffer.toByteArray(), sequenceId);
    }
	
	private int sendResultRow(OutputStream out, String[] row, int sequenceId) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        for (String value : row) {
            if (value == null) {
                buffer.write(0xFB); // NULL value
            } else {
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                buffer.write(encodeLengthCodedBinary(bytes.length));
                buffer.write(bytes);
            }
        }
        return writePacket(out, buffer.toByteArray(), sequenceId);
    }
	
	private void writeStringLengthCoded(ByteArrayOutputStream buffer, String str) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        buffer.write(encodeLengthCodedBinary(bytes.length));
        buffer.write(bytes);
    }
	
	private static class PacketResult {
        byte[] data;
        int sequenceId;
        
        PacketResult(byte[] data, int sequenceId) {
            this.data = data;
            this.sequenceId = sequenceId;
        }
    }
}