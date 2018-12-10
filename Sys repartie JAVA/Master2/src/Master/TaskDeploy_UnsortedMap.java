package Master;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class TaskDeploy_UnsortedMap extends Thread {

	private Thread t = Thread.currentThread();
	private String threadName;
	private String slaveName;
	private String pathOfFile;
	private String targetPath;
	private String mode;

	private String inputPath;
	private List<String> keys = new ArrayList<String>() ;


	public String getThreadName() {
		return threadName;
	}


	public List<String> getKeys() {
		return keys;
	}


	public TaskDeploy_UnsortedMap(String slaveName, String fileToDeploy, String target, String input, String slaveMode) {
		this.slaveName = slaveName;
		this.pathOfFile = fileToDeploy;
		this.targetPath = target;
		this.mode = slaveMode ;
		this.inputPath = input;
		this.threadName = slaveName;
		
		System.out.println("Creating unsorted map thread " + threadName);
	}

	private static void inheritIO(final InputStream src, final PrintStream dest) {
	    new Thread(new Runnable() {
	        private Scanner sc;

			public void run() {
	            sc = new Scanner(src);
	            while (sc.hasNextLine()) {
	                dest.println(sc.nextLine());
	            }
	        }
	    }).start();
	}
	
	public void run() {
		
		try {

			//System.out.println("Thread: " + threadName);
			
			ProcessBuilder pb = new ProcessBuilder("ssh", "boucheta@" + slaveName + ".enst.fr", "mkdir", "-p", targetPath).inheritIO();
			ProcessBuilder pbCopy = new ProcessBuilder("scp", pathOfFile, "boucheta@" + slaveName + ".enst.fr"+ ":"+targetPath).inheritIO();
			ProcessBuilder pbJar = new ProcessBuilder().command("ssh", "boucheta@" + slaveName + ".enst.fr", "java", "-jar", "/tmp/boucheta/Slave.jar", 
													mode, inputPath);
			try {
				Process p1 = pb.start();	
				p1.waitFor();
				Process p2 = pbCopy.start();
				p2.waitFor();
				Process p3 = pbJar.start();
				p3.waitFor();
				// print result
                InputStream inputStream = p3.getInputStream();
                InputStream error = p3.getErrorStream();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream), 1);
                String line;
                if (error.available() > 0) {
            
                	inheritIO(p3.getErrorStream(), System.err);
                }
                else {
                    while ((line = bufferedReader.readLine()) != null) {
                    	keys.add(line);
                    }
                }
               
                inputStream.close();
                bufferedReader.close();

			} catch (Exception e) {
				e.printStackTrace();
			}

			Thread.sleep(50);

		} catch (InterruptedException e) {
			System.out.println("unsorted map Thread " + threadName + " interrupted.");
		}
		System.out.println("unsorted map Thread " + threadName + " finished.");

	}

	public void start() {
		//System.out.println("Starting " + threadName);
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}

}
