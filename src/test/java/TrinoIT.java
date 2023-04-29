import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.database.DatabaseConnection;
import liquibase.database.DatabaseFactory;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.TrinoContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;

public class TrinoIT {

    @Rule
    public TrinoContainer trinoSql = new TrinoContainer("trinodb/trino:352")
            .waitingFor(Wait.forLogMessage(".*SERVER.*STARTED.*", 1));
    public Liquibase liquibase;
    public DatabaseConnection connection;

    @Before
    public void setup() throws LiquibaseException {
        trinoSql.start();
        System.out.println("getDriverClassName => " + trinoSql.getDriverClassName());
        System.out.println("getJdbcUrl => " + trinoSql.getJdbcUrl());
        System.out.println("getUsername => " + trinoSql.getUsername());
        System.out.println("getPassword => " + trinoSql.getPassword());
        connection = DatabaseFactory.getInstance().openConnection(
                trinoSql.getJdbcUrl(),
                trinoSql.getUsername(),
                trinoSql.getPassword(),
                null,
                new ClassLoaderResourceAccessor()
        ); //your openConnection logic here
        System.out.println("getCatalog => " + connection.getCatalog());
        System.out.println("class => " + connection.getClass().getName());
        System.out.println("getConnectionUserName => " + connection.getConnectionUserName());
        
        Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(connection);
        File resourcesDirectory = new File("src/test/resources/trino.xml");
        DatabaseChangeLog cl = new DatabaseChangeLog(resourcesDirectory.getAbsolutePath());
        liquibase = new Liquibase(cl, new ClassLoaderResourceAccessor(), database);
        System.out.println("getChangeLogFile=> " + liquibase.getChangeLogFile());
    }


    @Test
    public void test() throws LiquibaseException {
        liquibase.update(new Contexts(), new LabelExpression());
        //liquibase.listUnexpectedChangeSets(new Contexts(), new LabelExpression());
        //System.out.println("claaa=> " + connection.getClass().getName());
        return;
    }

    @Test
    public void test_validate() throws LiquibaseException {
        System.out.println("getCatalog=> " + liquibase.getDatabase());

        System.out.println("getCatalog=> " + liquibase.getDatabase().getConnection().getCatalog());
        System.out.println("getConnectionUserName=> " + liquibase.getDatabase().getConnection().getConnectionUserName());
        System.out.println("getLiquibaseCatalogName=> " + liquibase.getDatabase().getLiquibaseCatalogName());
        System.out.println("getLiquibaseSchemaName=> " + liquibase.getDatabase().getLiquibaseSchemaName());
        System.out.println("getLiquibaseTablespaceName=> " + liquibase.getDatabase().getLiquibaseTablespaceName());
        System.out.println("getDatabaseProductName=> " + liquibase.getDatabase().getDatabaseProductName());
        System.out.println("getDefaultSchema=> " + liquibase.getDatabase().getDefaultSchema());
        System.out.println("getDefaultSchemaName=> " + liquibase.getDatabase().getDefaultSchemaName());
        System.out.println("getDefaultCatalogName=> " + liquibase.getDatabase().getDefaultCatalogName());
        System.out.println("getDatabaseChangeLogTableName=> " + liquibase.getDatabase().getDatabaseChangeLogTableName());
        liquibase.validate();
        System.out.println("cl=> " + connection.getClass().getName());
    }
}