/** 
 *  @ ClaimPayment.java v1.0   16/05/2018 
 *  
 *  */ 
package com.tryg.poc.data;

/** 
 * Description  POJO class for  ClaimPayment Data
* @author Joseph  
* @version 1.0
* @see 
* @updated by  
* @updated date 
* @Copy 
*/ 
public class ClaimPayment {

	public Double PAYMENT;
	public Double getPAYMENT() {
		return PAYMENT;
	}
	public void setPAYMENT(Double pAYMENT) {
		PAYMENT = pAYMENT;
	}
	public Double getPAYTIME() {
		return PAYTIME;
	}
	public void setPAYTIME(Double pAYTIME) {
		PAYTIME = pAYTIME;
	}
	public Integer getCLAIMCOUNTER() {
		return CLAIMCOUNTER;
	}
	public void setCLAIMCOUNTER(Integer cLAIMCOUNTER) {
		CLAIMCOUNTER = cLAIMCOUNTER;
	}
	public String getCLAIMNUMBER() {
		return CLAIMNUMBER;
	}
	public void setCLAIMNUMBER(String cLAIMNUMBER) {
		CLAIMNUMBER = cLAIMNUMBER;
	}
	public Double PAYTIME;
	public Integer CLAIMCOUNTER;
	public String CLAIMNUMBER;

}
