
/** 
 *  @ Utilties.java v1.0   16/05/2018 
 *  class which define general methods
 *  
 *  */ 
package com.tryg.poc.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
/** 
 * Description
* @author Joseph James 
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class Utilties {
	/**
	 * Method load the configuration parameters
	 * load the configuration and set to constant variables
	 */
	@SuppressWarnings("unused")
	public static void loadConfigProperties() {
		
		Properties prop = new Properties();
    	InputStream input = null;
    	try {
        
    		String filename = "config.properties";
    		input = new FileInputStream(filename);
    		if(input==null){
    	            System.out.println("Sorry, unable to find " + filename);
    		    return;
    		}

    		//load a properties file from class path, inside static method
    		prop.load(input);
    		if(prop.getProperty("KAFKASERVERS")!=null)
    			Constants.KafkaServers=prop.getProperty("KAFKASERVERS");
    		if(prop.getProperty("KAFKAPORT")!=null)
    			Constants.KafkaBootStrapPort=prop.getProperty("KAFKAPORT");
                  
    	} catch (IOException ex) {
    		ex.printStackTrace();
        } finally{
        	if(input!=null){
        		try {
				input.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
        	}
        }
	}

}
