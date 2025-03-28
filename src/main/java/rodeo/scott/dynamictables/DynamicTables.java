package rodeo.scott.dynamictables;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.json.JSONObject;
import org.json.JSONTokener;
import java.util.logging.*;

/**
* A utility class to manage dynamic PostgreSQL tables based on column values.
* Tables are created automatically using a specified dynamic column (e.g., "domain").
*/
public class DynamicTables {

	private static final Logger logger = Logger.getLogger(DynamicTables.class.getName());
	
    private Connection conn;
    private String tablePrefix = "dtbl_";
    private List<ColumnDefinition> columnList = new ArrayList<>();
    private String columnDynamic;
    private String tableNameDynamic = "";
    private FileHandler logFileHandler;
    private String version;
    
    /**
    * Constructs a new DynamicTables instance with default settings.
    * Initializes internal variables and sets up logging.
    */
    public DynamicTables() {
        initialize();	
    }
 
    /**
    * Initializes default values for internal fields used in the DynamicTables class.
    * <p>
    * This method sets the default table prefix, clears the dynamic column and table name,
    * sets the version string, and configures logging with a default level of {@code SEVERE}
    * and file logging disabled.
    */   
    private void initialize() {
        columnDynamic = "";
        tablePrefix = "dtbl_";
        tableNameDynamic = "";
        version = "0.2.1";
        
        logFileHandler = null;
        setupLogging(Level.SEVERE, false);
    }    
    
    /**
    * Configures the logging behavior for the application.
    * <p>
    * This method sets the logging level for the root logger, replaces all existing handlers,
    * and sets up a console logger. Optionally, it can also enable or disable file logging to
    * {@code dynamic_tables.log}.
    *
    * @param level       the desired logging level (e.g., {@code Level.INFO}, {@code Level.SEVERE})
    * @param logToFile   if {@code true}, logs will also be written to {@code dynamic_tables.log};
    *                    if {@code false}, file logging will be disabled
    */
    public void setupLogging(Level level, boolean logToFile) {
        try {
            // Configure logging format
            Logger rootLogger = Logger.getLogger("");
            rootLogger.setLevel(level);

            // Remove existing handlers to prevent duplicate logs
            for (Handler handler : rootLogger.getHandlers()) {
                rootLogger.removeHandler(handler);
            }

            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(level);
            consoleHandler.setFormatter(new SimpleFormatter());
            rootLogger.addHandler(consoleHandler);

            // Enable file logging if requested
            if (logToFile && logFileHandler == null) {
                logFileHandler = new FileHandler("dynamic_tables.log", true);
                logFileHandler.setFormatter(new SimpleFormatter());
                rootLogger.addHandler(logFileHandler);
            } 
            // Disable file logging if needed
            else if (!logToFile && logFileHandler != null) {
                rootLogger.removeHandler(logFileHandler);
                logFileHandler.close();
                logFileHandler = null;
            }

            logger.info("Logging level set to " + level.getName());
            if (logToFile) {
                logger.info("File logging enabled (dynamic_tables.log)");
            }
        } catch (Exception e) {
            logger.severe("Failed to set up logging: " + e.getMessage());
        }
    }

    /**
    * Dynamically changes the logging level for the application.
    * <p>
    * This updates both the root logger and all associated handlers to reflect
    * the new level. Acceptable values include standard log level names such as
    * {@code "SEVERE"}, {@code "WARNING"}, {@code "INFO"}, {@code "FINE"}, etc.
    *
    * @param levelName the name of the desired logging level (case-insensitive)
    */
    public void changeLogLevel(String levelName) {
        try {
            Level level = Level.parse(levelName.toUpperCase());
            Logger rootLogger = Logger.getLogger("");
            rootLogger.setLevel(level);

            for (Handler handler : rootLogger.getHandlers()) {
                handler.setLevel(level); // Ensure handlers respect new log level
            }

            logger.info("Log level changed to " + levelName.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.severe("Invalid log level: " + levelName);
        }
    }

    /**
    * Sets the list of column definitions to be used when creating new tables.
    * <p>
    * The input should be a comma-separated string of column definitions, where each
    * column is defined by a name and SQL type (e.g., {@code "id INT, name VARCHAR(100)"}).
    * This will overwrite any existing column definitions.
    * <p>
    * Valid entries are added to the internal column list. Invalid formats are logged
    * as warnings and skipped.
    *
    * @param inputColumns a comma-separated string of column definitions
    */
    public void setColumns(String inputColumns) {
        columnList.clear(); // Reset column list

        for (String column : inputColumns.split(",")) {
            column = column.trim();
            String[] parts = column.split("\\s+", 2); // Split into max 2 parts

            if (parts.length == 2) {
                columnList.add(new ColumnDefinition(parts[0], parts[1]));
                logger.info("Adding column definition: '" + parts[0] + ", " + parts[1] + "'");
            } else {
                logger.warning("Invalid column format: '" + column + "'");
            }
        }
    }
    
    /**
    * Sets the prefix to be used when naming dynamically generated tables.
    * <p>
    * This prefix is prepended to table names that are generated based on a dynamic column,
    * helping to namespace or categorize related tables.
    * </p>
    *
    * @param prefix the table name prefix to use (e.g., {@code "dt_"})
    */
    public void setTablePrefix(String prefix) {   
    	this.tablePrefix = prefix;
    }

    /**
    * Sets the name of the dynamic column whose values will determine the table name.
    * <p>
    * When inserting data, the value of this column is sanitized and used to
    * generate the name of the table into which the data will be inserted.
    *
    * @param columnName the name of the column to use for dynamic table generation (e.g., {@code "domain"})
    */    
    public void setDynamicColumn(String columnName) {
        this.columnDynamic = columnName;
    }    

    /**
    * Establishes a connection to the PostgreSQL database.
    *
    * @param database the name of the database
    * @param user the database user
    * @param password the user's password
    * @param host the database host (e.g., "localhost")
    * @throws SQLException if a database access error occurs
    */
    public void connectionOpen(String database, String user, String password, String host) throws SQLException {
        try {
            // Explicitly load the PostgreSQL driver
            Class.forName("org.postgresql.Driver");

            // Construct the database URL
            String dbUrl = "jdbc:postgresql://" + host + "/" + database;

            // Establish connection
            conn = DriverManager.getConnection(dbUrl, user, password);
            logger.info("Connected to the database: " + database);

        } catch (ClassNotFoundException e) {
            logger.severe("PostgreSQL JDBC Driver not found. Ensure postgresql.jar is in the classpath.");
            logger.log(Level.SEVERE, "Exception Details: ", e);
        } catch (SQLException e) {
            logger.severe("Database connection failed: " + e.getMessage());
            logger.log(Level.SEVERE, "Exception Details: ", e);
        }
    }

    /**
    * Establishes a connection to a PostgreSQL database using the provided credentials and host information.
    * <p>
    * This method attempts to load the PostgreSQL JDBC driver explicitly and then
    * connect to the specified database. If the driver is missing or the connection fails,
    * errors are logged but not rethrown.
    * </p>
    *
    * @param database the name of the PostgreSQL database
    * @param user the username to connect with
    * @param password the password associated with the username
    * @param host the hostname or IP address of the PostgreSQL server
    * @throws SQLException if a database access error occurs
    */
    public void connectHC(String database, String user, String password, String host) throws SQLException {
    	connectionOpen(database, user, password, host);
    }    

    /**
    * Loads PostgreSQL connection credentials from a JSON configuration file and establishes a connection.
    * <p>
    * The JSON file must contain the following keys: {@code database}, {@code user}, {@code password}, and {@code host}.
    * If {@code configPath} is null or empty, it defaults to {@code config.json}.
    *
    * @param configPath the path to the JSON configuration file (can be {@code null} or empty to use default)
    * @throws IOException if the file is not found or cannot be read
    * @throws SQLException if a database access error occurs during connection
    */
    public void connectJSON(String configPath) throws IOException, SQLException {
        // Default to "config.json" if no path is provided
        if (configPath == null || configPath.isEmpty()) {
            configPath = "config.json";
        }

        File configFile = new File(configPath);
        if (!configFile.exists()) {
            throw new IOException("Configuration file not found: " + configPath);
        }

        // Load JSON config
        try (FileReader reader = new FileReader(configFile)) {
            JSONObject config = new JSONObject(new JSONTokener(reader));

            String database = config.getString("database");
            String user = config.getString("user");
            String password = config.getString("password");
            String host = config.getString("host");

            connectionOpen(database, user, password, host);
        }
    }
    
    /**
    * Establishes a PostgreSQL database connection using environment variables.
    * <p>
    * The following environment variables must be set:
    * <ul>
    *   <li>{@code DTABLES_ENVS_PGSQL_DATABASE} – the database name</li>
    *   <li>{@code DTABLES_ENVS_PGSQL_USER} – the username</li>
    *   <li>{@code DTABLES_ENVS_PGSQL_PASSWORD} – the password</li>
    *   <li>{@code DTABLES_ENVS_PGSQL_HOST} – the database host</li>
    * </ul>
    *
    * @throws SQLException if a database access error occurs during connection
    */
    public void connectENVS() throws SQLException{
        // Retrieve database credentials from environment variables
        String database = System.getenv("DTABLES_ENVS_PGSQL_DATABASE");
        String user = System.getenv("DTABLES_ENVS_PGSQL_USER");
        String password = System.getenv("DTABLES_ENVS_PGSQL_PASSWORD");
        String host = System.getenv("DTABLES_ENVS_PGSQL_HOST");
        connectionOpen(database, user, password, host);
    }
    
    /**
    * Closes the current database connection if it is open.
    * <p>
    * Logs an informational message when the connection is successfully closed.
    * If an error occurs while closing, it logs the exception details at the SEVERE level.
    * </p>
    */
    public void connectionClose() {
        if (conn != null) {
            try {
                conn.close();
                logger.info("Database connection closed.");
            } catch (SQLException e) {
                logger.log(Level.SEVERE, "Error closing database connection: ", e);
            }
        }
    }
    
    /**
    * Convenience method to close the current database connection.
    * <p>
    * Internally calls {@link #connectionClose()} to handle the actual closure.
    * This method exists to provide a simpler and more semantically intuitive name
    * for external callers.
    */
    public void close() {
    	connectionClose();
    }    
    
    /**
    * Retrieves the names of all tables in the connected PostgreSQL database.
    *
    * @return an array of table names as strings
    * @throws SQLException if a database access error occurs while retrieving metadata
    */
    String[] getTables() throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet resultSet = metaData.getTables(null, null, "%", new String[]{"TABLE"});
        
        // Collect table names
        List<String> tableList = new java.util.ArrayList<>();
        while (resultSet.next()) {
            tableList.add(resultSet.getString("TABLE_NAME"));
        }
        resultSet.close();

        return tableList.toArray(new String[0]);
    }
    
    /**
    * Retrieves the column names and data types for a specified table.
    *
    * @param tableName the name of the table to retrieve column information from
    * @return a list of strings representing each column in the format " - columnName: dataType"
    */
    public List<String> getColumns(String tableName) {
        String query = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position;
        """;
        List<String> columns = new ArrayList<>();

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String columnName = rs.getString("column_name");
                    String dataType = rs.getString("data_type");
                    columns.add(" - " + columnName + ": " + dataType);
                }
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error retrieving columns for '" + tableName + "': ", e);
        }

        return columns;
    }
    
    /**
    * Returns the current database connection.
    *
    * @return the active {@link Connection} object, or {@code null} if not connected
    */
    public Connection getConnection(){
        return conn;
    }
    
    /**
    * Retrieves all rows from the specified table in the connected PostgreSQL database.
    * <p>
    * This method uses a prepared statement for execution, but because table names cannot be parameterized,
    * it performs manual validation to ensure the table name is safe (alphanumeric with underscores only).
    *
    * @param tableName the name of the table to query (should be validated or generated internally)
    * @return a list of rows, where each row is represented as a list of column values; 
    *         an empty list if the table doesn't exist or has no rows
    */
    public List<List<Object>> getTableRows(String tableName) {
        // Retrieves all rows from the specified table safely.
        List<List<Object>> rows = new ArrayList<>();

        // Validate table name to prevent SQL injection
        if (!isValidTableName(tableName)) {
            logger.warning("Invalid table name: " + tableName);
            return rows;
        }
        
        String query = "SELECT * FROM \"" + tableName + "\""; // Using double quotes for PostgreSQL table names

        try (PreparedStatement pstmt = conn.prepareStatement(query);
             ResultSet resultSet = pstmt.executeQuery()) {

            int columnCount = resultSet.getMetaData().getColumnCount();

            while (resultSet.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(resultSet.getObject(i)); // Get all column values
                }
                rows.add(row);
            }

            if (!rows.isEmpty()) {
                logger.info("Retrieved " + rows.size() + " rows from table: " + tableName);
            } else {
                logger.info("Table '" + tableName + "' exists but has no data.");
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error retrieving rows from table: " + tableName, e);
        }

        return rows;
    }

    /**
    * Validates a table name to ensure it is safe for use in SQL queries.
    * <p>
    * This method checks that the name consists of only alphanumeric characters and underscores,
    * preventing potential SQL injection or syntax errors when used in SQL identifiers.
    *
    * @param name the table name to validate
    * @return {@code true} if the name is valid (only letters, digits, or underscores); {@code false} otherwise
    */
    private boolean isValidTableName(String name) {
        return name.matches("[a-zA-Z0-9_]+"); // Alphanumeric + underscore only
    }
    
    /**
    * Displays the name of the currently connected PostgreSQL database.
    * <p>
    * Executes a query to retrieve the name of the current database and logs the result.
    * If an error occurs during execution, it is logged at the SEVERE level.
    */
    public void showDB() {
        String query = "SELECT current_database();"; // Fetch the current database

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            if (rs.next()) {
                logger.info("Connected to database: " + rs.getString(1));
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error retrieving current database: ", e);
        }
    }
    
    /**
    * Displays a list of all table names in the connected PostgreSQL database.
    * <p>
    * Uses the {@link #getTables()} method to retrieve table names and prints them to the standard output.
    * If no tables are found or an error occurs, an appropriate message is displayed.
    */
    public void showTables() {
        // Prints a list of all table names (uses getTables).
        try {
            String[] tables = getTables();

            if (tables.length > 0) {
                System.out.println("Tables in the database:");
                for (String table : tables) {
                    System.out.println(table);
                }
            } else {
                System.out.println("No tables found or an error occurred.");
            }
        } catch (SQLException e) {
            System.out.println("Error retrieving tables: " + e.getMessage());
        }
    }
    
    /**
    * Displays all column names and data types for a specified table.
    * <p>
    * Uses the {@link #getColumns(String)} method to retrieve column metadata and logs each column's name and type.
    * If no columns are found or the table does not exist, a warning is logged.
    *
    * @param tableName the name of the table whose columns should be displayed
    */
    public void showColumns(String tableName) {
        List<String> columns = getColumns(tableName);

        if (!columns.isEmpty()) {
            logger.info("Columns in table '" + tableName + "':");
            for (String column : columns) {
                logger.info(column);
            }
        } else {
            logger.warning("No columns found for table '" + tableName + "' or the table does not exist.");
        }
    }
    
    /**
    * Displays column information for all tables in the public schema of the connected database.
    * <p>
    * This method queries the {@code information_schema.tables} to retrieve all table names,
    * then calls {@link #showColumns(String)} for each one to print its columns and types.
    */
    public void showColumnsAll() {
        String query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String tableName = rs.getString("table_name");
                showColumns(tableName); // Call the showColumns method for each table
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    /**
    * Displays all rows from the specified table by retrieving them with {@link #getTableRows(String)}.
    * <p>
    * If the table contains data, each row is printed to the console. If the table is empty
    * or an error occurs, an appropriate message is printed instead.
    *
    * @param tableName the name of the table to display rows from
    */
    public void showTable(String tableName) {
        // Displays all rows from the specified table (uses getTableRows).
        List<List<Object>> rows = getTableRows(tableName);

        if (rows != null && !rows.isEmpty()) {
            System.out.println("Rows in '" + tableName + "':");
            for (List<Object> row : rows) {
                System.out.println(" - " + row);
            }
        } else {
            System.out.println("Table '" + tableName + "' exists but has no data or an error occurred.");
        }
    }
    
    /**
    * Prints the current version of the DynamicTables library to the console.
    * <p>
    * Useful for debugging, diagnostics, or confirming the library version at runtime.
    */
    public void showVersion() {
        System.out.println(this.version);
    }

    /**
    * Displays the contents of the specified table by name.
    * <p>
    * This is a convenience method that delegates to {@link #showTable(String)}.
    *
    * @param tableName the name of the table to display
    */    
    public void selectTable(String tableName) {
    	showTable(tableName);
    }   
    
     /**
    * Inserts a row of data into the specified table.
    * <p>
    * This method constructs a parameterized SQL INSERT statement from the provided
    * map of column names to values, ensuring safe insertion and preventing SQL injection.
    *
    * @param tableName the name of the table to insert into
    * @param dataDict a map of column names to values to be inserted
    */   
    public void insertData(String tableName, Map<String, Object> dataDict) {
        if (dataDict.isEmpty()) {
            logger.warning("No data to insert into table: " + tableName);
            return;
        }

        List<String> columns = List.copyOf(dataDict.keySet()); // Get column names
        List<Object> values = List.copyOf(dataDict.values()); // Get column values

        String columnNames = String.join(", ", columns);
        String placeholders = String.join(", ", columns.stream().map(col -> "?").toArray(String[]::new));

        String query = String.format("""
            INSERT INTO %s (%s)
            VALUES (%s);
        """, tableName, columnNames, placeholders);

        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            for (int i = 0; i < values.size(); i++) {
                pstmt.setObject(i + 1, values.get(i)); // Set values dynamically
            }

            int rowsInserted = pstmt.executeUpdate();
            if (rowsInserted > 0) {
                logger.info("Successfully inserted " + rowsInserted + " row(s) into table: " + tableName);
            } else {
                logger.warning("No rows inserted into table: " + tableName);
            }
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error inserting data into table: " + tableName, e);
        }
    }

    /**
    * Creates a new table in the PostgreSQL database with the specified name and column definitions.
    * <p>
    * The table is created only if it does not already exist. An auto-incrementing {@code id} column
    * is added as the primary key by default. All column definitions are quoted to preserve casing and special names.
    *
    * @param tableName the name of the table to create
    * @param columns a list of {@link ColumnDefinition} objects specifying column names and types
    */
    public void createTable(String tableName, List<ColumnDefinition> columns) {
        StringBuilder columnDefinitions = new StringBuilder();

        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition col = columns.get(i);
            columnDefinitions.append("\"").append(col.name).append("\" ")
                             .append(col.type);
            if (i < columns.size() - 1) {
                columnDefinitions.append(", ");
            }
        }

        String query = String.format("""
            CREATE TABLE IF NOT EXISTS "%s" (
                id SERIAL PRIMARY KEY,
                %s
            );
        """, tableName, columnDefinitions);

        logger.info("Attempting to create table: " + tableName);
        logger.fine("Table creation query: " + query);

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(query);
            logger.info("Table created successfully: " + tableName);
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error creating table: " + tableName, e);
        }
    }

    /**
    * Deletes all tables from the current PostgreSQL database that match the configured table prefix.
    * <p>
    * This method searches for tables whose names begin with the current {@code tablePrefix}, and then
    * drops them using a transactional approach. If any error occurs during deletion, the transaction is rolled back.
    *
    * <p><b>Note:</b> Only tables in the {@code public} schema are considered.</p>
    */
    public void deleteTables() {
        String query = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name LIKE ?;
        """;

        String prefixPattern = tablePrefix + "%"; // D2ynamic prefix
        logger.fine("Prefix pattern for table deletion: " + prefixPattern); // Debugging output

        try {
            conn.setAutoCommit(false); // Disable auto-commit before executing queries
            List<String> tables = new ArrayList<>();

            try (PreparedStatement pstmt = conn.prepareStatement(query)) {
                pstmt.setString(1, prefixPattern);

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        tables.add(rs.getString("table_name"));
                    }
                }
            }

            if (tables.isEmpty()) {
                logger.info("No tables found for deletion with prefix: " + prefixPattern);
            } else {
                logger.info("Found tables to delete: " + tables);
                for (String tableName : tables) {
                    logger.info("Dropping table: " + tableName);
                    String dropQuery = "DROP TABLE IF EXISTS \"" + tableName + "\" CASCADE;";
                    try (Statement dropStmt = conn.createStatement()) {
                        dropStmt.execute(dropQuery);
                    }
                }
                logger.info("Successfully deleted " + tables.size() + " table(s).");
            }

            conn.commit(); // Commit changes
        } catch (SQLException e) {
            logger.log(Level.SEVERE, "Error deleting tables", e);
            try {
                conn.rollback(); // Rollback changes if an error occurs
                logger.warning("Transaction rolled back due to an error.");
            } catch (SQLException rollbackEx) {
                logger.log(Level.SEVERE, "Error during rollback", rollbackEx);
            }
        } finally {
            try {
                conn.setAutoCommit(true); // Re-enable auto-commit
            } catch (SQLException enableAutoCommitEx) {
                logger.log(Level.SEVERE, "Error re-enabling auto-commit", enableAutoCommitEx);
            }
        }
    }

    /**
    * Prints the current configuration of the dynamic table system.
    * <p>
    * Displays all configured column definitions along with the currently set dynamic column name.
    * Useful for debugging and verifying the internal setup of the {@code DynamicTables} instance.
    */
    public void status() {
        System.out.println("\nConfigured Columns:");
        for (ColumnDefinition column : columnList) {
            System.out.println(column);
        }
        System.out.println("\nDynamic Column:\n" + columnDynamic + "\n");
    }
   
    /**
    * Formats a table name by removing all non-alphanumeric and non-underscore characters,
    * then prepends the configured table prefix.
    * <p>
    * This method ensures that dynamically generated table names are safe and conform
    * to SQL identifier rules.
    *
    * @param inputColumn the input string used to generate the table name
    * @return a sanitized table name with the configured prefix
    */
    public String formatTableName(String inputColumn) {
        // Remove all characters except letters, numbers, and underscores
        String cleaned = inputColumn.replaceAll("[^a-zA-Z0-9_]", "");
        return tablePrefix + cleaned;
    }  
    
    /**
    * Inserts a row of data into a dynamically named table based on the configured columns.
    * <p>
    * This method accepts a variable number of arguments corresponding to the values for each
    * configured column. If the column designated as the dynamic column is encountered, its value
    * is used to generate the table name using {@link #formatTableName(String)}.
    * If the corresponding table does not exist, it is created using the current column definitions.
    *
    * @param args variable number of values representing one row of data, in the order of defined columns
    * @throws IllegalArgumentException if the number of arguments doesn't match the column list size
    */
    public void input(Object... args) {
        Map<String, Object> columnDict = new HashMap<>();
        tableNameDynamic = "";

        for (int columnNumber = 0; columnNumber < columnList.size(); columnNumber++) {
            ColumnDefinition columnDef = columnList.get(columnNumber);
            String columnName = columnDef.name;

            if (columnName.equals(columnDynamic)) {
            	tableNameDynamic = formatTableName((String) args[columnNumber]);

                columnDict.put(columnName, args[columnNumber]);
            } else {
                columnDict.put(columnName, args[columnNumber]);
            }
        }

        if (!tableNameDynamic.isEmpty()) {
            createTable(tableNameDynamic, columnList);
        }

        insertData(tableNameDynamic, columnDict);
    } 
    
    /**
    * Represents the definition of a database column including its name and SQL type.
    * <p>
    * This class is used to store column metadata for dynamically created tables.
    */
    public class ColumnDefinition {
        String name;
        String type;

        ColumnDefinition(String name, String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String toString() {
            return name + " " + type;
        }
    }

}



