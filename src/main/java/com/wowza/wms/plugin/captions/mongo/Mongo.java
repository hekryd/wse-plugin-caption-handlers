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


public class Mongo {
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
        String configPath = "/usr/local/WowzaStreamingEngine/custom-plugin-resources/mongo-local.properties";
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