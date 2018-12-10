package Master;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Master2 {
	private final static int MAX_BLOCK_SIZE = 4000000;
	private static int countOfSplits = 0;
	private static int nb_splits = 0 ;
	private static String bigFile = "src/res/sante_publique";

	private static List<Thread> threadsMap = new ArrayList<Thread>();
	private static List<Thread> threadsReduce = new ArrayList<Thread>();
	
	private static List<TaskDeploy_UnsortedMap> tasksMap = new ArrayList<TaskDeploy_UnsortedMap>();
	private static List<Task_SortedMap_reduce> tasksReduce = new ArrayList<Task_SortedMap_reduce>();

	private static String fileS = null;
	//private static String bigFile = "src/res/sante_publique.txt";
	private static String target = "/tmp/boucheta/splits/";
	private static String mapspath = "/tmp/boucheta/maps/";
	
	private static HashMap<String, String> dictionaryUMSlave = new HashMap<String, String>();
	private static HashMap<String,  List<String>> dictionaryKeyUMs = new HashMap<String,  List<String>>();
	private static HashMap<String,  List<String>> dictionarySlaveKeys = new HashMap<String,  List<String>>();
	private static HashMap<String,  List<String>> dictionarySlaveUMx = new HashMap<String,  List<String>>();
	//HashMap<String,  List<String>> dictionarySlaveRMx = new HashMap<String,  List<String>>();
	
	private static HashMap<String, Integer> wordsCount = new HashMap<String, Integer>();

	private static BufferedReader bufferedReaderSlaves = null;
	private static BufferedReader bufferedReaderFiles = null;
	
	private static List<String> slaves = new ArrayList<String>() ;
	
	
	
	
	
	public static void main(String[] args) throws InterruptedException, IOException {
		
		Long startTime = System.currentTimeMillis();
		
		FileReader splitsFileReader = new FileReader("src/res/splits");
		FileReader slavesFileReader = new FileReader("src/res/Slaves");
		
		bufferedReaderSlaves = new BufferedReader(slavesFileReader);
		
		String slavetmp;
		while((slavetmp = bufferedReaderSlaves.readLine()) != null) {
			slaves.add(slavetmp);
		}
			
		
		
		bufferedReaderFiles = new BufferedReader(splitsFileReader);
		
		
		System.out.println("*************** split big file ***************\n");
		
		splitBigFile(bigFile, "src/res/splits");
		
		System.out.println("*************** start all procedure UM SM RM ***************\n");
		
		while (countOfSplits < nb_splits) {
			
			dictionaryKeyUMs = new HashMap<String,  List<String>>();
			dictionaryUMSlave = new HashMap<String, String>();
			//System.out.print("um slave " + dictionaryUMSlave);
			for (String slave : slaves) {
	        	//init dict slave list words to reduce
	        	dictionarySlaveKeys.put(slave, new ArrayList<String>());
	        	//init dict slave list Umx
	        	dictionarySlaveUMx.put(slave, new ArrayList<String>());
			}
			
			System.out.println("*************** start mapping ***************\n");
			unsortedMap();
	           
	        
	        System.out.println("*************** start shuffling and filtering ***************\n");
	        shuffle();
	        
	        
	        System.out.println("*************** Start Sorted Map and Reduce ***************\n");
	        reduce();
	        

	        tasksMap.clear();
	        threadsMap.clear();
	            

		}
        	

		
		bufferedReaderFiles.close(); 
		bufferedReaderSlaves.close();
        
		System.out.println("thread size " + threadsReduce.size());
		System.out.println("task size " + tasksReduce.size());
        System.out.println("*************** wait all thread finished ***************\n");
        
        for (int i = 0; i < threadsReduce.size(); i++)
        {
            try {
				((Thread)threadsReduce.get(i)).join(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
  
	        	for (String key: tasksReduce.get(i).getWordsCount().keySet()) {
	            	if(!wordsCount.containsKey(key)) {
	            		wordsCount.put(key, tasksReduce.get(i).getWordsCount().get(key));
	            	}
	            	else {
	            		int oldValue = wordsCount.get(key);
	            		wordsCount.put(key, oldValue + tasksReduce.get(i).getWordsCount().get(key));
	            	}
	        	}

        }

        threadsReduce.clear();
        tasksReduce.clear();
        
        printResult();
        
        long endTime   = System.currentTimeMillis();
        System.out.println(endTime - startTime);
	}
	
	
	
	public static void reduce() {
		int count = 0 ;
        for (Entry<String, List<String>> d : dictionarySlaveKeys.entrySet()) {
        	String slave1 = d.getKey();
        	String words = String.join(",", d.getValue()) ;
        	List<String> ums = dictionarySlaveUMx.get(slave1);
        	
        	if (ums.size() > 0) {
            	String umsString = mapspath + ums.get(0) ;
            	
            	for(String um : ums.subList(1, ums.size()) ) 
            	{umsString +=  "," + mapspath + um ; }
            	
            	//System.out.println(umsString);
            			
            	Task_SortedMap_reduce task = new Task_SortedMap_reduce(slave1, umsString, words, mapspath+"SM"+count);
            	Thread t = new Thread(task);
            	t.start();
            	threadsReduce.add(t);
            	tasksReduce.add(task) ;
        	}

        	
        	count++;
        }
        

        
	}
	
	public static void printResult() {
        System.out.println("*************** result the 5 most words ***************\n");
        


    	
        Comparator<Entry<String, Integer>> valueComparator = new Comparator<Entry<String,Integer>>() {
        	@Override 
        	public int compare(Entry<String, Integer> e1, Entry<String, Integer> e2) { 
        		Integer v1 = e1.getValue(); 
        		Integer v2 = e2.getValue(); 
        		return v1.compareTo(v2); 
        	} 
        };

        List<Entry<String, Integer>> listOfEntries = new ArrayList<Entry<String, Integer>>(wordsCount.entrySet()); 

        Collections.sort(listOfEntries, valueComparator);
        Collections.reverse(listOfEntries); // descendent
        	
        LinkedHashMap<String, Integer> listOfBest5 = new LinkedHashMap<String, Integer>(5);
        
    	Pattern pattern;
    	Matcher matcher;
        pattern = Pattern.compile("[a-z]+");
        
        for (Entry<String, Integer> entry : listOfEntries){ 
        	matcher = pattern.matcher(entry.getKey()) ;
        	if (matcher.find())
        		listOfBest5.put(entry.getKey(), entry.getValue());
        	
        	if (listOfBest5.size() > 5)
        		break;
        }
         //print
        Set<Entry<String, Integer>> entrySetSortedByValue = listOfBest5.entrySet(); 
        	
        for(Entry<String, Integer> mapping : entrySetSortedByValue){ 
        	System.out.println(mapping.getKey() + " ==> " + mapping.getValue()); 
        }
        
        
	}
	
	public static void shuffle() {
		// copy SMx for shuffling
        ExecutorService pool = Executors.newSingleThreadExecutor();
        
        for(Entry<String, List<String>> kum : dictionaryKeyUMs.entrySet()) {
        	
        	List<String> ums = kum.getValue() ;
        	String um0 = ums.get(0) ;
        	String key = kum.getKey() ;
        	String SlaveTargetTemp = dictionaryUMSlave.get(um0);
        	
        	for (String um : ums ) {
        		String slavekey1 = dictionaryUMSlave.get(um) ;
        		String slavekey2 = SlaveTargetTemp ;
        		if (dictionarySlaveKeys.containsKey(slavekey1) && dictionarySlaveKeys.containsKey(slavekey2) && dictionarySlaveKeys.get(slavekey1).size() < dictionarySlaveKeys.get(slavekey2).size()){
        			SlaveTargetTemp = dictionaryUMSlave.get(um);
        			um0 = um ;
            		//System.out.println(key + " -- " + SlaveTargetTemp);
            		//System.out.println(dictionarySlaveKeys);
        		}
        		
        	}

        	final String slaveTarget = SlaveTargetTemp ;
	   		 
        		if( !dictionarySlaveKeys.get(slaveTarget).contains(key)){

		       		dictionarySlaveKeys.get(slaveTarget).add(key);
		   		 }
        	
  
            	if(!dictionarySlaveUMx.get(slaveTarget).contains(um0)) {

            		dictionarySlaveUMx.get(slaveTarget).add(um0);
          
            	}
        	

        	
        	
        	for (String um : ums ) {
        		String slaveSource = dictionaryUMSlave.get(um) ;
        		
             	boolean b = dictionarySlaveUMx.get(slaveTarget).contains(um);
             	
             	if(!b) {
             		
             		dictionarySlaveUMx.get(slaveTarget).add(um);
             		
             		
        	        Runnable task = new Runnable() {
        	            public void run() {
                     
    	                    try
    	                    {
    	                    	//System.out.println("copy " + um + " from " + slaveSource + " to " + slaveTarget + " in thread :" + Thread.currentThread().getName());
    	                    	new ProcessBuilder("scp", "-3", "boucheta@" + slaveSource + ".enst.fr"+ ":" + mapspath + um,  "boucheta@" + slaveTarget + ".enst.fr"+ ":" + mapspath).inheritIO().start();
    	                      
    	                    }
    	                    catch (Throwable e)
    	                    {
    	                    	e.printStackTrace();
    	                    }
        	            }
        	        };
        	 
        	        pool.execute(task);            		
      
        		}

        	}
        	
        }
        pool.shutdown();
        while (!pool.isTerminated()) {
        	 
		}
       
        System.out.println("*************** shuffling finished ***************\n");
//        for(Entry<String, List<String>> d : dictionarySlaveUMx.entrySet())
//        { System.out.println(d.getKey() + " ==> " + d.getValue()); }
//        for(Entry<String, List<String>> d : dictionarySlaveKeys.entrySet())
//        { System.out.println(d.getKey() + " ==> " + d.getValue()); }
	}
	
	public static void unsortedMap() throws InterruptedException {

		List<String> indexOfFiles= new ArrayList<String> ();
		try {
			
			//while ((fileS = bufferedReaderFiles.readLine()) != null ) {
			for (String slave : slaves){
				if((fileS = bufferedReaderFiles.readLine()) != null) {
				//String slave = bufferedReaderSlaves.readLine() ;
	        	
				//if((fileS = bufferedReaderFiles.readLine()) != null ) {
					TaskDeploy_UnsortedMap task = new TaskDeploy_UnsortedMap(slave, "src/res/"+fileS, target, fileS, "0");
					Thread t = new Thread(task);
					t.start();
					threadsMap.add(t);
					tasksMap.add(task) ;
					dictionaryUMSlave.put("UM"+fileS.substring(1), slave );
					indexOfFiles.add(fileS.substring(1));
					
					countOfSplits++;
				}	
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

        // wait thread and create dict word list of UMs
        for (int i = 0; i < threadsMap.size(); i++)
        {
            ((Thread)threadsMap.get(i)).join(10000);
            
        	for (String key: tasksMap.get(i).getKeys()) {
        		
        		if(!dictionaryKeyUMs.containsKey(key)) {
        			List<String> l = new ArrayList<String>() ;
        			l.add("UM"+indexOfFiles.get(i));
        			dictionaryKeyUMs.put(key, l );
            	}
            	else {

            		dictionaryKeyUMs.get(key).add("UM"+indexOfFiles.get(i));
            	}
        	}

        }
        
        
        System.out.println("*************** all unsorted map thread finished ***************\n");
        
//        for(Entry<String, List<String>> d : dictionaryKeyUMs.entrySet())
//        { System.out.println(d.getKey() + " ==> " + d.getValue()); }
//        
//        for(Entry<String, String> d : dictionaryUMSlave.entrySet())
//        { System.out.println(d.getKey() + " ==> " + d.getValue()); }
        
	}
	
	public static void splitBigFile(String fileName, String saveFile) { 
		FileWriter fs;
		FileWriter fstring;
		try { 
			FileReader textFileReader = new FileReader(fileName);
			BufferedReader bufReader = new BufferedReader(textFileReader);
			
			fstring = new FileWriter(saveFile);
			
			char[] buffer = new char[MAX_BLOCK_SIZE]; 
			int numberOfCharsRead = bufReader.read(buffer); 
	
			while (numberOfCharsRead != -1) { 
				
				fs = new FileWriter("src/res/S"+nb_splits);
				fs.write(String.valueOf(buffer, 0, numberOfCharsRead)); 
				numberOfCharsRead = bufReader.read(buffer); 
				
				fs.close();
				
				fstring.write("s"+nb_splits+"\n");
				nb_splits ++;
			}
			fstring.close();
			bufReader.close();
	    
			
			} catch (IOException e) 
			{ // TODO Auto-generated catch block 
				e.printStackTrace();
			}
		}

}
