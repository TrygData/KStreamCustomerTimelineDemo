/** 
 *  @ Policy.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;
/**
 * Description POJO Class which stores Policy data
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class Policy {

	public String POLICY;
	public String getPOLICY() {
		return POLICY;
	}
	public void setPOLICY(String pOLICY) {
		POLICY = pOLICY;
	}
	public double getPOLICYSTARTTIME() {
		return POLICYSTARTTIME;
	}
	public void setPOLICYSTARTTIME(double pOLICYSTARTTIME) {
		POLICYSTARTTIME = pOLICYSTARTTIME;
	}
	public int getPVAR1() {
		return PVAR1;
	}
	public void setPVAR1(int pVAR1) {
		PVAR1 = pVAR1;
	}
	public int getPVAR0() {
		return PVAR0;
	}
	public void setPVAR0(int pVAR0) {
		PVAR0 = pVAR0;
	}
	public double getPOLICYENDTIME() {
		return POLICYENDTIME;
	}
	public void setPOLICYENDTIME(double pOLICYENDTIME) {
		POLICYENDTIME = pOLICYENDTIME;
	}
	public double POLICYSTARTTIME;
	public int PVAR1;
	public int PVAR0;
	public double POLICYENDTIME;

}
