package rodeo.scott.dynamictables;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ManualTest {
    public static void main(String[] args) {
    	
        // Initialize DynamicTable object
        DynamicTables tables = new DynamicTables();

        tables.showVersion();
        
        //tables.changeLogLevel("info");
        
        try {
            tables.connectHC("mydb", "myuser", "losdjfaosidjf-sdfgjkd4DDSRs", "localhost");
        	//tables.connectENVS();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        tables.setTablePrefix("dt1_");

        tables.deleteTables();
        
        tables.setColumns("domain VARCHAR(100), keyword VARCHAR(100), language VARCHAR(100)");
        
        tables.setDynamicColumn("domain");
        
        tables.input("wikipedia.org","cats","en");
        tables.input("wikipedia.org","dogs","en");

        tables.input("google.com","maps","en");
        tables.input("google.com","images","en");
        
        // formatted dynamic column name example
        System.out.println("---"+tables.formatTableName("wikipedia.org")+"---");        
        
        // select a table with the converted dynamic column name       
        tables.showTable(tables.formatTableName("wikipedia.org"));
  
        //System.out.println("---"+tables.formatTableName("google.com")+"---");    
        //tables.selectTable(tables.formatTableName("google.com"));
      
        tables.showTables();
        
        //tables.status();     
        //tables.showTables();
        //tables.showColumnsAll();
        //tables.showDB();
        
 
        try {

        	String query = "SELECT * FROM \"" + tables.formatTableName("wikipedia.org") + "\""; 
            
            try (Connection conn = tables.getConnection(); // Get the connection
                 PreparedStatement pstmt = conn.prepareStatement(query);
                 ResultSet rs = pstmt.executeQuery()) {

                int columnCount = rs.getMetaData().getColumnCount();

                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rs.getObject(i) + " ");
                    }
                    System.out.println();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        
        
        tables.close();   
        
        
    }
}
