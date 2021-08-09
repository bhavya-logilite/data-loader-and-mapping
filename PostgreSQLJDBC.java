package dlmp;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
public class PostgreSQLJDBC {
   public static void main( String args[] ) {
	   Connection c = null;
	   try {
	   Class.forName("org.postgresql.Driver");
       c = DriverManager
          .getConnection("jdbc:postgresql://localhost:5432/postgres",
          "postgres", "Ankur@803");
       c.setAutoCommit(false);
       System.out.println("Opened database successfully");
       Statement st = c.createStatement(); 
       String sql =" COPY customer_data FROM '/home/bhavya/extdrive/project/dataloder and mapper/customer_data.csv'DELIMITER ','CSV HEADER;";
       int m = st.executeUpdate(sql);
       if (m == 1)
           System.out.println("import fail : "+sql);
       else
           System.out.println("import success" + sql); 
       
       PreparedStatement ps=c.prepareStatement("select * from customer_data");  
       ResultSet rs=ps.executeQuery();  
       ResultSetMetaData rsmd=rs.getMetaData();   
       System.out.println("Total columns: "+rsmd.getColumnCount());    
            st.close();
            c.commit();
            c.close();
    	   
       } catch ( Exception e ) {
           System.err.println( e.getClass().getName()+": "+ e.getMessage() );
           System.exit(0);
        }
}
}
   
    