package liquibase.ext.trino.sqlgenerator;

import liquibase.database.Database;
import liquibase.ext.trino.database.TrinoDatabase;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;

public class TrinoCreateDatabaseChangeLogTableGenerator extends CreateDatabaseChangeLogTableGenerator {

    @Override
    public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
        return !(database instanceof TrinoDatabase);
    }

    @Override
    protected String getCharTypeName(Database database) {
        return database instanceof TrinoDatabase ? "string" : "varchar";
    }

}
