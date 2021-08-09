package dlmp;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;
public class PropertiesFile {
	public static void main(String[] args) {
			writePropertiesFile();

		}
		public static void writePropertiesFile() {
		Properties prop =new Properties();
		try {
			OutputStream output = new FileOutputStream("/home/bhavya/extdrive/project/dataloder and mapper/header.properties");  
			prop.setProperty("custom_id","id");
			prop.setProperty("custom_fname","fname");
			prop.setProperty("custom_lname","lname");
			prop.setProperty("custom_email","email");
			prop.setProperty("custom_mobile","mobile");
			prop.setProperty("custom_sales","sales");

			prop.store(output,"/home/bhavya/extdrive/assignment 2/customer_data.csv");
		}  
		catch (Exception e) {
			
			e.printStackTrace();
		}
		}
	
	}


