package com.yugabyte.sample.apps;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

public class YBCqlHelloWorld {
    static int ttl = 30;
    static CqlSession session;
    static int v1Value = 49;

    // Load the cluster root certificate
    private SSLContext createSSLHandler(String certfile) {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            FileInputStream fis = new FileInputStream(certfile);
            X509Certificate ca;
            try {
                ca = (X509Certificate) cf.generateCertificate(fis);
            } catch (Exception e) {
                System.err.println("Exception generating certificate from input file: " + e);
                return null;
            } finally {
                fis.close();
            }

            // Create a KeyStore containing our trusted CAs
            String keyStoreType = KeyStore.getDefaultType();
            KeyStore keyStore = KeyStore.getInstance(keyStoreType);
            keyStore.load(null, null);
            keyStore.setCertificateEntry("ca", ca);

            // Create a TrustManager that trusts the CAs in our KeyStore
            String tmfAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(tmfAlgorithm);
            tmf.init(keyStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            System.err.println("Exception creating sslContext: " + e);
            return null;
        }
    }

    public static void main(String[] args)
    {
        
        if (args[0].equals("indexUpdateIncludeColumnDiscrepancy"))
            indexUpdateIncludeColumnDiscrepancy();
        else if (args[0].equals("indexUpdateIndexColumnDiscrepancy"))
            indexUpdateIndexColumnDiscrepancy();
        else if (args[0].equals("indexUpdateNonIndexNonIncludeColumnDiscrepancy"))
            indexUpdateNonIndexNonIncludeColumnDiscrepancy();
        else if (args[0].equals("indexBackfillUpdateIncludeColumnDiscrepancy"))
            indexBackfillUpdateIncludeColumnDiscrepancy();
        else if (args[0].equals("indexBackfillUpdateIndexColumnDiscrepancy"))
            indexBackfillUpdateIndexColumnDiscrepancy();
        else if (args[0].equals("indexBackfillUpdateNonIndexNonIncludeColumnDiscrepancy"))
            indexBackfillUpdateNonIndexNonIncludeColumnDiscrepancy();
        
    }


    public static void connect()
    {
        session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .build();
    }

    public static void createKeyspace()
    {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS ybdemo;";
        session.execute(createKeyspace);
        System.out.println("Created keyspace ybdemo");
    }

    public static void createTable()
    {
        session.execute("DROP TABLE IF EXISTS ybdemo.t;");
        String createTable = "CREATE TABLE IF NOT EXISTS ybdemo.t (k int PRIMARY KEY, v1 int, v2 int, v3 int) WITH default_time_to_live = " + ttl +"AND transactions = {'enabled': 'true'};";
        session.execute(createTable);
        System.out.println("Created table t");
    }

    public static void createIndex()
    {
        String createIndex = "create index idx on ybdemo.t(v1) include (v2) WITH default_time_to_live = " + ttl+ ";";
        session.execute(createIndex);
        System.out.println("Created index on t");
    }

    public static void insertData()
    {
         for (int i=0; i< 50; i++ ) {
            String insert = "INSERT INTO ybdemo.t (k, v1, v2, v3)" +
                    " VALUES (" + i +"," + i + ","+ i+"," + i+ ");";
            session.execute(insert);
            System.out.println("Inserted data: " + insert);
        }
    }
    
    public static void nonIndexQuery()
    {
        String selectFromTable = "SELECT k, v1, v2, v3, ttl(v1), ttl(v2), ttl(v3) FROM ybdemo.t where k=49;";
            ResultSet selectResult = session.execute(selectFromTable);
            List < Row > rows = selectResult.all();
            if (!rows.isEmpty()) {
                int k = rows.get(0).getInt(0);
                int v1 = rows.get(0).getInt(1);
                int v2 = rows.get(0).getInt(2);
                int v3 = rows.get(0).getInt(3);
                Long ttlV1 = rows.get(0).getLong(4);
                Long ttlV2 = rows.get(0).getLong(5);
                Long ttlV3 = rows.get(0).getLong(6);
                System.out.println("Query returned " + rows.size() + " row: " + "k=" + k +
                        ", v1=" + v1 + ", v2: " + v2 + ", v3: " + v3 + ", TTLv1: " + ttlV1 + ", TTLv2: " + ttlV2 + ", TTLv3:" + ttlV3);
            }
            else {
                System.out.println("GCed from Table");
            }
    }
    public static void indexQuery()
    {
         String selectFromIndex = "SELECT v1, v2 FROM ybdemo.t WHERE v1 = "+ v1Value+";";
                ResultSet selectIndexResult = session.execute(selectFromIndex);
                List < Row > rows = selectIndexResult.all();
                if (!rows.isEmpty()) {
                    int k = rows.get(0).getInt(0);
                    int v1 = rows.get(0).getInt(0);
                    int v2 = rows.get(0).getInt(1);
                    System.out.println("Index Query returned " + rows.size() + " row: " + "k=" + k +
                            ", v1=" + v1 + ", v2: " + v2);
                }
                else {
                    System.out.println("GCed from Index");
                }
    }
    public static void indexUpdateIncludeColumnDiscrepancy() {
        try {
            connect();
            createKeyspace();
            createTable();
            createIndex();
            insertData();
            Thread.sleep(2*ttl/3);

            for (int i=0; i< ttl*2; i++ ) {
                System.out.println("Executing query");
                Thread.sleep(1000);
                nonIndexQuery();
                /**-----**/
                indexQuery();

                if (i==ttl/2)
                {
                    String updateStmt = "update ybdemo.t set v2 = 49 where k = 49; ";
                    session.execute(updateStmt);
                }
            }
            session.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

     public static void indexUpdateIndexColumnDiscrepancy() {
        try {
            connect();
            createKeyspace();
            createTable();
            createIndex();
            insertData();
            Thread.sleep(2*ttl/3);

            for (int i=0; i< ttl*2; i++ ) {
                System.out.println("Executing query");
                Thread.sleep(1000);
                nonIndexQuery();
                /**-----**/
                indexQuery();

                if (i==ttl/2)
                {
                    String updateStmt = "update ybdemo.t set v1 = 49 where k = 49; ";
                    session.execute(updateStmt);
                }
            }
            session.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
     public static void indexUpdateNonIndexNonIncludeColumnDiscrepancy() {
        try {
            connect();
            createKeyspace();
            createTable();
            createIndex();
            insertData();
            Thread.sleep(2*ttl/3);

            for (int i=0; i< ttl*2; i++ ) {
                System.out.println("Executing query");
                Thread.sleep(1000);
                nonIndexQuery();
                /**-----**/
                indexQuery();

                if (i==ttl/2)
                {
                    String updateStmt = "update ybdemo.t set v3 = 49 where k = 49; ";
                    session.execute(updateStmt);
                }
            }
            session.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }

     public static void indexBackfillUpdateNonIndexNonIncludeColumnDiscrepancy() {
        try {
            connect();
            createKeyspace();
            createTable();
            insertData();
            createIndex();
            Thread.sleep(2*ttl/3);

            for (int i=0; i< ttl*2; i++ ) {
                System.out.println("Executing query");
                Thread.sleep(1000);
                nonIndexQuery();
                /**-----**/
                indexQuery();

                if (i==ttl/2)
                {
                    String updateStmt = "update ybdemo.t set v3 = 49 where k = 49; ";
                    session.execute(updateStmt);
                }
            }
            session.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    public static void indexBackfillUpdateIncludeColumnDiscrepancy() {
        try {
            connect();
            createKeyspace();
            createTable();
            insertData();
            createIndex();
            Thread.sleep(2*ttl/3);

            for (int i=0; i< ttl*2; i++ ) {
                System.out.println("Executing query");
                Thread.sleep(1000);
                nonIndexQuery();
                /**-----**/
                indexQuery();

                if (i==ttl/2)
                {
                    String updateStmt = "update ybdemo.t set v2 = 49 where k = 49; ";
                    session.execute(updateStmt);
                }
            }
            session.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    public static void indexBackfillUpdateIndexColumnDiscrepancy() {
        try {
            connect();
            createKeyspace();
            createTable();
            insertData();
            createIndex();
            Thread.sleep(2*ttl/3);

            for (int i=0; i< ttl*2; i++ ) {
                System.out.println("Executing query");
                Thread.sleep(1000);
                nonIndexQuery();
                /**-----**/
                indexQuery();

                if (i==ttl/2)
                {
                    String updateStmt = "update ybdemo.t set v1 = 49 where k = 49; ";
                    session.execute(updateStmt);
                }
            }
            session.close();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}