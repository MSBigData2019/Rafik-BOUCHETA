package Master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Scanner;

public class Task_SortedMap_reduce extends Thread {
	
	private Thread t = Thread.currentThread();
	private String threadName;
	private String slaveName;
	private String umsPath;
	private String words;
	private String smPath ;
	private HashMap<String, Integer> wordsCount = new HashMap<String, Integer>();

	
	public Task_SortedMap_reduce(String slaveName, String umsPath, String words, String smPath ) {
		threadName = slaveName;
		this.slaveName = slaveName;
		this.umsPath = umsPath;
		this.words = words;
		this.smPath = smPath;
		
		
		System.out.println("Creating SM reduce thread " + threadName);
	}
	
	
	public HashMap<String, Integer> getWordsCount() {
		return wordsCount;
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
//		System.out.println("Running " + threadName);
//		System.out.println("Thread: " + threadName);
		
		ProcessBuilder pbShuffle = new ProcessBuilder().command("ssh", "boucheta@" + slaveName + ".enst.fr", "java", "-jar",
															"/tmp/boucheta/Slave.jar",	"1", words, smPath, umsPath).inheritIO();
		ProcessBuilder pbReduce = new ProcessBuilder().command("ssh", "boucheta@" + slaveName + ".enst.fr", "java", "-jar",
															"/tmp/boucheta/Slave.jar",	"2", words, smPath, umsPath);

		try {
			Process p2 = pbShuffle.start();
			p2.waitFor();
			p2.destroy();
			
			Process p3 = pbReduce.start();
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
                	String[] splitLine = line.split(" ");
                	wordsCount.put(splitLine[0], Integer.parseInt(splitLine[1])) ;
                }
            }
            
			p3.destroy();
			
		} catch (InterruptedException e) {
			System.out.println("reduce Thread " + threadName + " interrupted.");
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Thread reduce" + threadName + " finished.");

	}

	public void start() {
		//System.out.println("Starting " + threadName);
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}

}
