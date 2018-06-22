package com.tryg.util;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @author Jeevan George
 * @version $Revision$ 16/06/2018
 * UtilClass for Logger
 */
public class LoggerUtil {
	
	public static Logger logger = Logger.getLogger("MyLog");
	
	static {
    FileHandler fh;  

    try {  

        // This block configure the logger with handler and formatter  
        fh = new FileHandler("C:/Users/699487/Documents/temp/MyLogFile.log");  
        logger.addHandler(fh);
        SimpleFormatter formatter = new SimpleFormatter();  
        fh.setFormatter(formatter);  

        // the following statement is used to log any messages  
        logger.info("Logger initiated");  

    } catch (SecurityException e) {  
        e.printStackTrace();  
    } catch (IOException e) {  
        e.printStackTrace();  
    }  
	}

}
