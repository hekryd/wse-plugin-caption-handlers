package com.wowza.wms.plugin.captions.mongo;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.logging.WMSLogger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.KeyManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class Mongo {
    private static final long ROLE_REFRESH_INTERVAL_MILLIS = 1_000L;
    private static final WMSLogger logger = WMSLoggerFactory.getLogger(Mongo.class);
    
    private MongoClient mongoClient;
    private MongoDatabase database;
    private String connectionString;
    private String databaseName;
    private String keystorePath;
    private String keystorePassword;
    private String truststorePath;
    private String truststorePassword;
    private boolean allowInvalidHostname = false;
    private boolean mainServer = false;
    private boolean backupServer = true;
    private final ConcurrentMap<String, EventRoleState> eventRoles = new ConcurrentHashMap<>();

    private static class EventRoleState {
        private volatile boolean backupActive;
        private volatile long nextRefreshAtMillis;
    }
    
    // Default constructor that loads from properties file
    public Mongo() {
        loadConfiguration();
    }

    // Constructor that accepts database name and then loads remaining configuration
    public Mongo(String databaseName) {
        this.databaseName = databaseName;
        loadConfiguration();
    }
    
    private void loadConfiguration() {
        Properties props = new Properties();
        
        // Try to load local configuration first
        String configPath = "/usr/local/WowzaStreamingEngine/custom-plugin-resources/env.properties";
        try (FileInputStream fis = new FileInputStream(configPath)) {
            props.load(fis);
            logger.info("Loaded MongoDB configuration from: " + configPath);
        } catch (IOException e) {
            logger.warn("Could not load local config file: " + configPath + ". Error: " + e.getMessage());
        }
        
        this.connectionString = props.getProperty("mongo.connection.string");
        this.keystorePath = props.getProperty("mongo.keystore.path");
        this.keystorePassword = props.getProperty("mongo.keystore.password");
        this.truststorePath = props.getProperty("mongo.truststore.path");
        this.truststorePassword = props.getProperty("mongo.truststore.password");
        this.allowInvalidHostname = Boolean.parseBoolean(props.getProperty("mongo.tls.allowInvalidHostname", "false"));
        configureServerRole(props);
        
        // Validate configuration
        if (connectionString == null || databaseName == null || keystorePath == null || keystorePassword == null) {
            throw new IllegalArgumentException("MongoDB configuration is incomplete. Check your properties file or constructor parameter.");
        }
    }
    
    public void connect() {
        try {
            logger.info("Connecting to MongoDB with X.509 authentication");
            // Load keystore for X.509 authentication
            KeyStore keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(new FileInputStream(keystorePath), keystorePassword.toCharArray());
            
            // Initialize KeyManagerFactory
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(keyStore, keystorePassword.toCharArray());
            
            // Initialize TrustManagerFactory. If a separate truststore is provided, load it; otherwise use the keystore.
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            if (truststorePath != null && !truststorePath.isEmpty() && truststorePassword != null) {
                KeyStore trustKeyStore = KeyStore.getInstance("PKCS12");
                try (FileInputStream tfs = new FileInputStream(truststorePath)) {
                    trustKeyStore.load(tfs, truststorePassword.toCharArray());
                }
                trustManagerFactory.init(trustKeyStore);
            } else {
                trustManagerFactory.init(keyStore);
            }
            
            // Create SSL context
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);
            
            // Create MongoDB client settings with X.509 authentication
            MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
                .credential(MongoCredential.createMongoX509Credential())
                .applyToSslSettings(builder -> builder.enabled(true).context(sslContext).invalidHostNameAllowed(allowInvalidHostname))
                .build();
            
            mongoClient = MongoClients.create(settings);
            database = mongoClient.getDatabase(databaseName);
            
            logger.info("Successfully connected to MongoDB with X.509 authentication");
            
        } catch (Exception e) {
            logger.error("Failed to connect to MongoDB: " + e.getMessage(), e);
            throw new RuntimeException("MongoDB connection failed", e);
        }
    }
    
    public MongoDatabase getDatabase() {
        return database;
    }

    public boolean isMainServer() {
        return mainServer;
    }

    /**
     * Returns whether this server currently owns speech-to-text generation for an event.
     * The local server role is configured once in env.properties. The shared
     * event stream.backup flag is read at most once per second, so changing it
     * moves generation between the two already-running Wowza instances without
     * a stream restart.
     */
    public boolean isCaptionWriter(String eventKey) {
        return isBackupActive(eventKey) ? backupServer : mainServer;
    }

    private void configureServerRole(Properties props) {
        String configuredRole = props.getProperty("wowza.serverRole", "").trim();
        if ("primary".equalsIgnoreCase(configuredRole)) {
            mainServer = true;
            backupServer = false;
            return;
        }
        if ("backup".equalsIgnoreCase(configuredRole)) {
            mainServer = false;
            backupServer = true;
            return;
        }

        // Keep the existing wowza.mainServer setting compatible with existing
        // deployments. In a two-server pair, the non-main instance is the backup.
        mainServer = Boolean.parseBoolean(props.getProperty("wowza.mainServer", "false"));
        backupServer = Boolean.parseBoolean(props.getProperty("wowza.backupServer", Boolean.toString(!mainServer)));
    }

    private boolean isBackupActive(String eventKey) {
        if (eventKey == null || eventKey.isEmpty() || database == null)
            return false;

        EventRoleState state = eventRoles.computeIfAbsent(eventKey, key -> new EventRoleState());
        long now = System.currentTimeMillis();
        if (now < state.nextRefreshAtMillis)
            return state.backupActive;

        synchronized (state) {
            if (now < state.nextRefreshAtMillis)
                return state.backupActive;

            try {
                org.bson.Document event = database.getCollection("events")
                        .find(new org.bson.Document("eventKey", eventKey)).first();
                if (event != null) {
                    boolean previousValue = state.backupActive;
                    state.backupActive = getBackupFlag(event);
                    if (previousValue != state.backupActive) {
                        logger.info("Caption failover role changed for event " + eventKey
                                + ": backup=" + state.backupActive);
                    }
                }
            } catch (Exception e) {
                logger.warn("Could not refresh caption failover role for event " + eventKey + ": " + e.getMessage());
            } finally {
                state.nextRefreshAtMillis = now + ROLE_REFRESH_INTERVAL_MILLIS;
            }
        }
        return state.backupActive;
    }

    private boolean getBackupFlag(org.bson.Document event) {
        org.bson.Document stream = event.get("stream", org.bson.Document.class);
        if (stream != null && stream.containsKey("backup"))
            return stream.getBoolean("backup", false);
        return event.getBoolean("backup", false);
    }
    
    public MongoClient getClient() {
        return mongoClient;
    }
    
    public void disconnect() {
        if (mongoClient != null) {
            mongoClient.close();
            logger.info("MongoDB connection closed");
        }
    }
}
