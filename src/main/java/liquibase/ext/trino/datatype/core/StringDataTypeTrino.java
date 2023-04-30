package liquibase.ext.trino.datatype.core;

import liquibase.change.core.LoadDataChange;
import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.ext.trino.database.TrinoDatabase;

import static liquibase.ext.trino.database.TrinoDatabase.TRINO_PRIORITY_DATABASE;


@DataTypeInfo(
        name = "string",
        minParameters = 0,
        maxParameters = 0,
        priority = TRINO_PRIORITY_DATABASE
)
public class StringDataTypeTrino extends LiquibaseDataType {
    public StringDataTypeTrino() {
    }

    public boolean supports(Database database) {
        return database instanceof TrinoDatabase;
    }

    public DatabaseDataType toDatabaseDataType(Database database) {
        if (database instanceof TrinoDatabase) {

            DatabaseDataType type = new DatabaseDataType("STRING", this.getParameters());
            if (this.getParameters().length == 0) {
                type.setType("STRING");
            } else {
                String firstParameter = String.valueOf(this.getParameters()[0]);
                int stringSize = Integer.parseInt(firstParameter);
                if (stringSize == 65535) {
                    type.setType("STRING");
                }
            }
            return type;
        } else {
            return super.toDatabaseDataType(database);
        }

    }

    public LoadDataChange.LOAD_DATA_TYPE getLoadTypeName() {
        return LoadDataChange.LOAD_DATA_TYPE.STRING;
    }
}
