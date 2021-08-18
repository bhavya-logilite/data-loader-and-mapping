package dlmp;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

public class ReaderCsv {
	private static String url = "";
	private static String user = "";
	private static String password = "";
	static Properties props = null;
	static ReaderCsv readerCsv;
	static Scanner sc = new Scanner(System.in);
	static Statement st = null;
	static Connection con = null;
	ResultSet rs = null;
	static DatabaseMetaData dbm = null;
	String headers;
	String line;
	static int count = 0;
	static String file = null;
	static String fileName="customer";
	static int noOfCsvRecord = 0;
	BufferedReader br;
	public static void main(String[] args) throws IOException  {
		 
	     
		  readerCsv=new ReaderCsv();
		 readerCsv.setDatabaseProperties();
		 try {
				Class.forName("org.postgresql.Driver");
				con=DriverManager.getConnection(url,user,password);
		        st=con.createStatement();
		         dbm = con.getMetaData();
	       
		     } catch (ClassNotFoundException | SQLException e1) {
			 e1.printStackTrace();
		     }
			 file = "/home/bhavya/extdrive/project/dataloder and mapper/src/"+fileName+".csv";
		     readerCsv.readPropertiesFile();
		     readerCsv.readcsvfile();
		     System.out.println("database operation complete..");
		     System.out.println("Total records in csv file are:" +noOfCsvRecord);
		     System.out.println("No of fails record:"+(noOfCsvRecord-count));
		     System.out.println("No of record in database:"+count);
	}

private void readcsvfile() {
	String[] head=null;
		try  {
	        	BufferedReader br =new BufferedReader(new FileReader(file));
					try {
						headers = br.readLine();
					   head = headers.split(",");
						for (String heads : head) {
							System.out.println(heads+"");
							
						}
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					datainsertion(br,head);
				}
	            
	        catch(FileNotFoundException e) {
	        	System.out.println("File is not available in current directory");
			} 
	}
	private void datainsertion(BufferedReader br,String[] head) {
		try {
			while((line = br.readLine()) != null)
			{
				noOfCsvRecord++;
				if (line.trim().length() != 0) {
			    String[] record = line.split(",");
			    String query="insert into "+fileName+"("+props.getProperty("customer_id")+"," +props.getProperty("customer_fname")+","+props.getProperty("customer_lname")
			    +","+props.getProperty("customer_sales")+")"+ "values("+record[0]+",'"+record[1]+"','"+record[2]+"',"+record[3]+");";
			    ResultSet tables = dbm.getTables(null, null, "customer", null);
			    System.out.println("______________________");
			    if (!tables.next()) {
			    	 String createTableQuery = createTable(fileName);
			        	st.execute(createTableQuery);
			        	System.out.println("Table is created");
				 }
				 else {
					   System.out.println("table is already present");
				 }
			    st.execute(query);
			    System.out.println("Records store");
			    
			}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e){
			count=count+1;
          try {
          	datainsertion(br,head);
				br.readLine();
					
			} catch (IOException e1) {
				e1.printStackTrace();
				
			}
      }
		catch (NullPointerException e) {
			try {
				
				br.readLine();
			} catch (IOException e1) {
				e1.printStackTrace();
				System.out.println("IOException:-");
			}
		}
	}

private static String createTable(String fileName) {
	     String query="create table customer("+props.getProperty("customer_id")+" SERIAL PRIMARY KEY,"
	   		+ props.getProperty("customer_fname")+" char(1000),"
			+ props.getProperty("customer_lname")+" char(1000),"
	   		+ props.getProperty("customer_sales")+" int);";
   	   return query;
		
	}

public void setDatabaseProperties() {
		 url = "jdbc:postgresql://localhost:5432/postgres";
	     user = "postgres";
	     password = "Ankur@803";
	}
	public void readPropertiesFile() throws IOException {    
		  props = readPropertiesFile("/home/bhavya/extdrive/project/dataloder and mapper/src/customer.properties");
	     System.out.println("property files are readed");
	}
	
public static Properties readPropertiesFile(String fileName)  {
	      FileInputStream fis = null;
	    
	      try {
	         fis = new FileInputStream(fileName);
	         props = new Properties();
	         props.load(fis);
	         
	      } catch(FileNotFoundException fnfe) {
	         fnfe.printStackTrace();
	      } catch(IOException ioe) {
	         ioe.printStackTrace();
	      } finally {
	         try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace(); 
			}
	      }
	      return props;
	   }	
}