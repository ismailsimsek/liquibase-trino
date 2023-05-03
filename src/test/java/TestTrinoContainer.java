import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.google.common.base.Strings;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TestTrinoContainer {


    public static String TRINO_SERVICE_NAME = "trino_1";
    
    @ClassRule
    public static DockerComposeContainer TRINO =
            new DockerComposeContainer("trino-iceberg", new File("src/test/resources/trino-iceberg/docker-compose.yml"))
                    .withOptions("--compatibility")
                    .withExposedService(TRINO_SERVICE_NAME, 8080)
                    .waitingFor(TRINO_SERVICE_NAME, Wait.forLogMessage(".*SERVER.*STARTED.*", 1))
                    .withLocalCompose(true);

    public String getJdbcUrl() {
        return String.format("jdbc:trino://%s:%s/%s", TRINO.getServiceHost(TRINO_SERVICE_NAME, 8080), TRINO.getServicePort(TRINO_SERVICE_NAME, 8080), Strings.nullToEmpty(""));
    }

    public String getUsername() {
        return "admin";
    }

    public String getPassword() {
        return "";
    }

    public String getDatabaseName() {
        return null;
    }

    @Test
    public void queryMemoryAndTpchConnectors() throws SQLException {
        System.out.println("pre-start");
        TRINO.start();
        System.out.println("post-start");

        try (Connection connection = this.createConnection();
             Statement statement = connection.createStatement()) {
            // Prepare data
            statement.execute("CREATE TABLE iceberg.default.example_iceberg_table ( c1 integer, c2 date, c3 double) WITH (format = 'PARQUET')");
            //statement.execute("CREATE TABLE memory.default.table_with_array AS SELECT 1 id, ARRAY[1, 42, 2, 42, 4, 42] my_array");

            // Query Trino using newly created table and a builtin connector
            try (ResultSet resultSet = statement.executeQuery("" +
                    "SELECT nationkey, element " +
                    "FROM tpch.tiny.nation " +
                    "JOIN memory.default.table_with_array twa ON nationkey = twa.id " +
                    "LEFT JOIN UNNEST(my_array) a(element) ON true " +
                    "ORDER BY element OFFSET 1 FETCH NEXT 3 ROWS WITH TIES ")) {
                List<Integer> actualElements = new ArrayList<>();
                while (resultSet.next()) {
                    actualElements.add(resultSet.getInt("element"));
                }
                Assert.assertEquals(Arrays.asList(2, 4, 42, 42, 42), actualElements);
            }
        }
        
        System.out.println(TRINO.getServicePort(TRINO_SERVICE_NAME, 8080));
        System.out.println(this.getJdbcUrl());
        System.out.println(this.getUsername());
        System.out.println(this.getPassword());
        System.out.println(this.getDatabaseName());
    }


    public Connection createConnection() throws SQLException, JdbcDatabaseContainer.NoDriverFoundException {
        return this.createConnection("", new Properties());
    }
    public String getDriverClassName() {
        return "io.trino.jdbc.TrinoDriver";
    }

    public Connection createConnection(String queryString, Properties info) throws SQLException, JdbcDatabaseContainer.NoDriverFoundException {
        Properties properties = new Properties(info);
        properties.put("user", this.getUsername());
        properties.put("password", this.getPassword());
        String url = this.constructUrlForConnection(queryString);

        Driver jdbcDriverInstance;
        try {
            jdbcDriverInstance = (Driver) Class.forName(this.getDriverClassName()).newInstance();
        } catch (IllegalAccessException | ClassNotFoundException | InstantiationException var4) {
            throw new JdbcDatabaseContainer.NoDriverFoundException("Could not get Driver", var4);
        }
        
        return jdbcDriverInstance.connect(url, properties);
    }

    protected String constructUrlForConnection(String queryString) {
        String baseUrl = this.getJdbcUrl();
        if ("".equals(queryString)) {
            return baseUrl;
        } else if (!queryString.startsWith("?")) {
            throw new IllegalArgumentException("The '?' character must be included");
        } else {
            return baseUrl.contains("?") ? baseUrl + "&" + queryString.substring(1) : baseUrl + queryString;
        }
    }

}