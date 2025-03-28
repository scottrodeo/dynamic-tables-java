package rodeo.scott.dynamictables;

import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.sql.SQLException;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DynamicTablesTest {

    private DynamicTables tables;

    @BeforeAll
    void setupDatabase() throws SQLException {
        tables = new DynamicTables();
        tables.connectHC("mydb", "myuser", "losdjfaosidjf-sdfgjkd4DDSRs", "localhost"); // Replace with test DB credentials
        tables.setTablePrefix("dt1_");
        tables.deleteTables(); // Ensure a clean state before tests
        tables.setColumns("domain VARCHAR(100), keyword VARCHAR(100), language VARCHAR(100)");
        tables.setDynamicColumn("domain");
    }

    @Test
    void testShowVersion() {
        assertDoesNotThrow(() -> tables.showVersion());
    }

    /*

    @Test
    void testSetColumns() {
        tables.setColumns("id INT, name VARCHAR(255)");
        assertNotNull(tables.getColumns("test_table")); // Ensure columns are set
    }
*/
    @Test
    void testTableCreation() {
        tables.input("example.com", "test", "en");
        List<String> columns = tables.getColumns(tables.formatTableName("example.com"));
        assertFalse(columns.isEmpty(), "Table should have columns defined.");
    }

    @Test
    void testInsertAndRetrieveData() throws SQLException {
    	tables.deleteTables();
        tables.input("wikipedia.org", "cats", "en");
        tables.input("wikipedia.org", "dogs", "en");

        List<List<Object>> rows = tables.getTableRows(tables.formatTableName("wikipedia.org"));
        assertEquals(2, rows.size(), "Two rows should be inserted.");
    }

    @Test
    void testDeleteTables() {
        tables.deleteTables();
        
        // Retrieve only tables with the expected prefix
        String[] tablesList = assertDoesNotThrow(() -> tables.getTables());
        
        // Ensure none of the remaining tables start with the current prefix
        boolean hasPrefixedTables = false;
        for (String table : tablesList) {
            if (table.startsWith("dt1_")) {
                hasPrefixedTables = true;
                break;
            }
        }
        
        assertFalse(hasPrefixedTables, "All test tables with the prefix should be deleted.");
    }


    
    @Test
    void testSetTablePrefix() {
        tables.setTablePrefix("test_");
        assertEquals("test_example", tables.formatTableName("example"));
    }
    
    @Test
    void testSetColumns() {
        tables.setTablePrefix("dt1_");
        tables.setColumns("domain VARCHAR(100), keyword VARCHAR(100), language VARCHAR(100)");
        tables.input("wikipedia.org", "cats", "en");
        assertNotNull(tables.getColumns(tables.formatTableName("wikipedia.org"))); // Ensure columns are set
    }
    
    @AfterAll
    void tearDown() {
        tables.close();
    }
}

