package liquibase.ext.trino.database;

import liquibase.database.jvm.JdbcConnection;

import java.sql.Connection;

/**
 * A Trino specific Delegate that removes the calls to autocommit
 */

public class TrinoConnection extends JdbcConnection {
    
    public TrinoConnection() {}

    public TrinoConnection(Connection delegate) {
        super(delegate);
    }

//    @Override
//    public boolean getAutoCommit() throws DatabaseException {
//        return true;
//    }

//    @Override
//    public void setAutoCommit(boolean autoCommit) throws DatabaseException {
//
//    }
//
//    @Override
//    public String getDatabaseProductVersion() throws DatabaseException {
//        return "1.0";
//    }
//
//    @Override
//    public int getDatabaseMajorVersion() throws DatabaseException {
//        return 1;
//    }
//
//    @Override
//    public int getDatabaseMinorVersion() throws DatabaseException {
//        return 0;
//    }
}
