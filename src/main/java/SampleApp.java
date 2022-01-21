import com.yugabyte.ysql.YBClusterAwareDataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SampleApp {
    private static final String TABLE_NAME = "DemoAccount";

    public static void main(String[] args) {

        Properties settings = new Properties();
        try {
            settings.load(SampleApp.class.getResourceAsStream("app.properties"));
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }

        YBClusterAwareDataSource ds = new YBClusterAwareDataSource();

        ds.setUrl("jdbc:yugabytedb://" + settings.getProperty("host") + ":"
            + settings.getProperty("port") + "/yugabyte");
        ds.setUser(settings.getProperty("dbUser"));
        ds.setPassword(settings.getProperty("dbPassword"));
        ds.setSsl(true);
        ds.setSslMode("verify-full");
        ds.setSslRootCert(settings.getProperty("sslRootCert"));

        try {
            Connection conn = ds.getConnection();
            System.out.println(">>>> Successfully connected to Yugabyte Cloud.");

            createDatabase(conn);

            selectAccounts(conn);
            transferMoneyBetweenAccounts(conn, 800);
            selectAccounts(conn);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createDatabase(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        DatabaseMetaData meta = conn.getMetaData();

        ResultSet resultSet = meta.getTables(null, null, TABLE_NAME.toLowerCase(),
            new String[] {"TABLE"});

        if (resultSet.next()) {
            System.out.println(">>>> Table " + TABLE_NAME + " already exists.");
            return;
        }

        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
            "(" +
            "id int PRIMARY KEY," +
            "name varchar," +
            "age int," +
            "country varchar," +
            "balance int" +
            ")");

        stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES" +
            "(1, 'Jessica', 28, 'USA', 10000)," +
            "(2, 'John', 28, 'Canada', 9000)");

        System.out.println(">>>> Successfully created " + TABLE_NAME + " table.");
    }

    private static void selectAccounts(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        System.out.println(">>>> Selecting accounts:");

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);

        while (rs.next()) {
            System.out.println(String.format("name = %s, age = %s, country = %s, balance = %s",
                rs.getString(2), rs.getString(3),
                rs.getString(4), rs.getString(5)));
        }
    }

    private static void transferMoneyBetweenAccounts(Connection conn, int amount) throws SQLException {
        Statement stmt = conn.createStatement();

        stmt.execute(
            "BEGIN TRANSACTION;" +
                "UPDATE " + TABLE_NAME + " SET balance = balance - " + amount + "" + " WHERE name = 'Jessica';" +
                "UPDATE " + TABLE_NAME + " SET balance = balance + " + amount + "" + " WHERE name = 'John';" +
                "COMMIT;"
        );

        System.out.println();
        System.out.println(">>>> Transferred " + amount + " between accounts.");
    }
}