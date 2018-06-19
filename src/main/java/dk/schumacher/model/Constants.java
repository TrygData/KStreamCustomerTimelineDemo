package dk.schumacher.model;

public class Constants {

    public final static String CUSTOMER_SCHEMA = "{\"name\": \"Data\",\"type\": \"record\",\"fields\": [{\"name\": \"customername\",	\"type\": \"string\"}, {	\"name\": \"customerid\",	\"type\": \"int\"}, {\"name\": \"customeraddress\",\"type\": \"string\"}, {\"name\": \"customertime\",\"type\": \"long\"}]}";
    public final static String POLICY_SCHEMA = "{\"name\": \"Data\",\"type\": \"record\",\"fields\": [{\"name\": \"customerid\",	\"type\": \"int\"}, {	\"name\": \"policynumber\",	\"type\": \"string\"}, {\"name\": \"policytime\",\"type\": \"long\"}]}";
    public final static String WHOLE = "{\"type\":\"record\",\"name\":\"CustomerAndpolicyJoined\",\"fields\":[{\"name\":\"CustomerList\",\"type\":[\"null\",\"string\"]},{\"name\":\"policyList\",\"type\":[\"null\",\"string\"]}]}";

}
