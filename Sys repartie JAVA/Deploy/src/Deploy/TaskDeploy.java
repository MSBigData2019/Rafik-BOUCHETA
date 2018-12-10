package Deploy;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TaskDeploy extends Thread {

	private Thread t;
	private String threadName;

	TaskDeploy(String slaveName) {
		threadName = slaveName;
		System.out.println("Creating " + threadName);
	}

	public void run() {
		System.out.println("Running " + threadName);
		try {

			System.out.println("Thread: " + threadName);
			ProcessBuilder pb = new ProcessBuilder("ssh", "boucheta@" + threadName + ".enst.fr", "mkdir", "-p", "/tmp/boucheta")
					.inheritIO();
			try {
				Process p = pb.start();
				boolean b = p.waitFor(15, TimeUnit.SECONDS);
				if (!b) {
					System.out.println("time out");
				}

				ProcessBuilder pbCopy = new ProcessBuilder("scp", "/Users/rafman/Desktop/Slave.jar",
						"boucheta@" + threadName + ".enst.fr" + ":/tmp/boucheta").inheritIO();
				pbCopy.start();

			} catch (IOException e) {
				e.printStackTrace();
			}

			Thread.sleep(50);

		} catch (InterruptedException e) {
			System.out.println("Thread " + threadName + " interrupted.");
		}
		System.out.println("Thread " + threadName + " finished.");
	}

	public void start() {
		System.out.println("Starting " + threadName);
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}

}
