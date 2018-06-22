/** 
 *  @ Customer.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;
/** 
 * Description  POJO Class stores  Customer data
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class Customer {

	public String POLICY;
	public String getPOLICY() {
		return POLICY;
	}
	public void setPOLICY(String pOLICY) {
		POLICY = pOLICY;
	}
	public String getADDRESS() {
		return ADDRESS;
	}
	public void setADDRESS(String aDDRESS) {
		ADDRESS = aDDRESS;
	}
	public String getCUSTOMER() {
		return CUSTOMER;
	}
	public void setCUSTOMER(String cUSTOMER) {
		CUSTOMER = cUSTOMER;
	}
	public double getCUSTOMERTIME() {
		return CUSTOMERTIME;
	}
	public void setCUSTOMERTIME(double cUSTOMERTIME) {
		CUSTOMERTIME = cUSTOMERTIME;
	}
	public String ADDRESS;
	public String CUSTOMER;
	public double CUSTOMERTIME;

}
