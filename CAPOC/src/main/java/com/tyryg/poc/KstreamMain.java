/** 
 *  @ KstreamMain.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tyryg.poc;

import com.tryg.poc.data.operations.DataProcessor;
import com.tryg.poc.util.Utilties;
/** 
 * Description Main Class which initialize the application
* @author Joseph James 
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class KstreamMain {

	public static void main(String[] args) throws IllegalAccessException, InstantiationException {
		
		initialize();
	}
	/**
	 * initialize the application
	 */
	private static void initialize() {
		Utilties.loadConfigProperties();
		DataProcessor dataProcessor=new DataProcessor();
		
		try {
			dataProcessor.processData();
		} catch (IllegalAccessException | InstantiationException e) {
			e.printStackTrace();
		}
		
		
	}

}
