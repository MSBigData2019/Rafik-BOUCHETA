package Slave;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;


public class Slave {

    private static FileWriter fw;

	public static void main(String[] args) throws Exception {

    	HashMap<String, Integer> dictWords = new HashMap<String, Integer>();
    	
        ProcessBuilder pb = new ProcessBuilder("mkdir", "-p", "/tmp/boucheta/maps/").inheritIO();
        Process p = pb.start();
        p.waitFor();
        
        // Map process: Generate UMx files
        if (args[0].equals("0")) {
            String splitFile = args[1];
            
            	
            String splitFilePath = "/tmp/boucheta/splits/" + splitFile;

            String splitIndex = splitFile.substring(1);
            
            String mapFilePath = "/tmp/boucheta/maps/UM" + splitIndex ;

            fw = new FileWriter(mapFilePath);
            try (BufferedReader br = new BufferedReader(new FileReader(splitFilePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] words = line.replaceAll("[^a-zA-Z ]", "").toLowerCase().split(" ");
                    for (String word : words) {
                        fw.write(word + " 1\n");
                        
                        if(!dictWords.containsKey(word)) {
                        	dictWords.put(word, 1);
                    	}
                    	else {
                    		int oldValue = dictWords.get(word);
                    		dictWords.put(word, ++oldValue);
                    	}
                    }
                }
            } catch (Exception ex) {
                throw ex;
            }
            finally {
                fw.close();
            }
            
            for(Entry<String, Integer> word : dictWords.entrySet())
        	{ System.out.println(word.getKey() ); }


        }
        else if (args[0].equals("1")) {
        	String[] wordsToMap = args[1].split(",");
        	String smPath = args[2];
        	String[] umsPath = args[3].split(",");
        	//String um2Path = args[4];
        	
            fw = new FileWriter(smPath);
            for (String umPath : umsPath ) {
                try (BufferedReader br = new BufferedReader(new FileReader(umPath))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] words = line.split(" ");
                        if (Arrays.asList(wordsToMap).contains(words[0])) {
                        	fw.write(words[0] + " " + words[1] +  "\n");
                        }
                    }
                } catch (Exception ex) {
                    throw ex;
                }
            }

            fw.close();
        	
        }
        else if (args[0].equals("2")) {
        	String[] wordsToReduce = args[1].split(",");
        	String smPath = args[2];
        	//String rmPath = args[3];
        	HashMap<String, Integer> wordsCount = new HashMap<String, Integer>();
        	//fw = new FileWriter(rmPath);
        	
        	try (BufferedReader br = new BufferedReader(new FileReader(smPath))) {
                String line;
                while ((line = br.readLine()) != null) {
                	//int count = 0 ;
                    String[] words = line.split(" ");
                     
                    if (Arrays.asList(wordsToReduce).contains(words[0]) ) {
                    	//count += Integer.parseInt(words[1] ) ;
                    	if(!wordsCount.containsKey(words[0])) {
                    		wordsCount.put(words[0], 1);
                    	}
                    	else {
                    		int oldValue = wordsCount.get(words[0]);
                    		wordsCount.put(words[0], ++oldValue);
                    	}
                    }
                }
                //fw.write(words[0] + " " + count +  "\n");
            } catch (Exception ex) {
                throw ex;
            }
        	
        	//fw.close();
        	
            for(Entry<String, Integer> mapping : wordsCount.entrySet()){ 
            	System.out.println(mapping.getKey() + " " + mapping.getValue()); 
            } 
        }
        
        else {
            throw new Exception("Input arguments are not valid");
        }

    }

}