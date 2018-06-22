package com.tryg.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * UtilClass for PropertiesFile
 */
public class PropertiesFileUtil {

    private Properties properties = new Properties();

	public PropertiesFileUtil(String filePath) {
		try {
			properties.load(new FileInputStream(filePath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    public String getPropertyValueByKey(String key) {
    	
    	return properties.getProperty(key, null);
    	
    }



}