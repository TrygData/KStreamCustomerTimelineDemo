package kstream.demo3;

public class ClaimMessage {
    public Double CLAIMTIME;
    public String CLAIMNUMBER;
    public Double CLAIMREPORTTIME;
    public String CLAIMCOUNTER;


    public String toString() {
        return CLAIMNUMBER + "/" + CLAIMCOUNTER;
    }

}
