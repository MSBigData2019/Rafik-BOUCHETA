package Deploy;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;


public class Deploy {
	public static void main(String[] args) throws InterruptedException {
		
		String fileName = "/res/Slaves";
		String slave = null;
		
        try {
            // FileReader reads text files in the default encoding.
        	InputStream file = Deploy.class.getResourceAsStream(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(new InputStreamReader(file));

            while((slave = bufferedReader.readLine()) != null) {

            	TaskDeploy t = new TaskDeploy(slave);
            	t.start();
     
            }   
            // Always close files.
            bufferedReader.close();         
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                "Unable to open file '" + 
                fileName + "'");                
        }
        catch(IOException ex) {
            System.out.println(
                "Error reading file '" 
                + fileName + "'");                  
        }
	}
}
