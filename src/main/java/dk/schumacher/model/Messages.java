package dk.schumacher.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Gøran Schumacher (GS) / Schumacher Consulting Aps
 * @version $Revision$ 16/05/2018
 */
public class Messages {

    public static Serde<CustomerMessage> customerMessageSerde;
    public static Serde<CustomerList> customerListSerde;
    public static Serde<PolicyMessage> policyMessageSerde;
    public static Serde<PolicyList> policyListSerde;
    public static Serde<ClaimMessage> claimMessageSerde;
    public static Serde<ClaimList> claimListSerde;
    public static Serde<PaymentMessage> paymentMessageSerde;
    public static Serde<PaymentList> paymentListSerde;
    public static Serde<ClaimAndPayment> claimAndPaymentSerde;
    public static Serde<ClaimAndPayment2> claimAndPayment2Serde;
    public static Serde<CustomerView> customerViewSerde;

    /************************************************************************
     * INITIALIZATION
     ************************************************************************/

    static {
        Map<String, Object> serdeProps = new HashMap<String, Object>();

        customerMessageSerde = createSerde(CustomerMessage.class, serdeProps);
        customerListSerde = createSerde(CustomerList.class, serdeProps);
        policyMessageSerde = createSerde(PolicyMessage.class, serdeProps);
        policyListSerde = createSerde(PolicyList.class, serdeProps);
        claimMessageSerde = createSerde(ClaimMessage.class, serdeProps);
        claimListSerde = createSerde(ClaimList.class, serdeProps);
        paymentMessageSerde = createSerde(PaymentMessage.class, serdeProps);
        paymentListSerde = createSerde(PaymentList.class, serdeProps);

        /************************************************************************
         * NESTED
         ************************************************************************/

        claimAndPaymentSerde = createSerde(ClaimAndPayment.class, serdeProps);
        claimAndPayment2Serde = createSerde(ClaimAndPayment2.class, serdeProps);
        customerViewSerde = createSerde(CustomerView.class, serdeProps);
    }

    /************************************************************************
     * MESSAGES
     ************************************************************************/

    static public class CustomerMessage extends GenericJson  {
        public String ADDRESS;
        public String CUSTOMER;
        public Double CUSTOMERTIME;
        public String POLICY;
    }

    static public class PolicyMessage extends GenericJson  {
        public int PVAR1;
        public String POLICYENDTIME;
        public int POLICY;
        public String POLICYSTARTTIME;
        public int PVAR0;
    }

    static public class CustomerList extends ArrayList<CustomerMessage>{};

    static public class PolicyList extends ArrayList<PolicyMessage>{};

    @JsonIgnoreProperties({ "policy" })
    static public class ClaimMessage extends GenericJson  {
        public Double CLAIMTIME;
        public String CLAIMNUMBER;
        public Double CLAIMREPORTTIME;
        public String CLAIMCOUNTER;

        public int getPolicy() {
            return Integer.parseInt(CLAIMNUMBER.split("_")[0]);
        }
    }

    static public class ClaimList extends ArrayList<ClaimMessage>{};

    @JsonIgnoreProperties({ "policy" })
    static public class PaymentMessage extends GenericJson  {
        public Double PAYMENT;
        public Double PAYTIME;
        public Integer CLAIMCOUNTER;
        public String CLAIMNUMBER;

        public int getPolicy() {
            return Integer.parseInt(CLAIMNUMBER.split("_")[0]);
        }
    }

    static public class PaymentList extends ArrayList<PaymentMessage>{};

    static public class CustomerAndPolicy extends GenericJson  {
        public ArrayList<CustomerMessage> customerList = new ArrayList<>();
        public ArrayList<PolicyMessage> policyList = new ArrayList<>();

        public CustomerAndPolicy() {
        }

        public CustomerAndPolicy(ArrayList<CustomerMessage> customerList, ArrayList<PolicyMessage> policyList) {
            this.customerList = customerList;
            this.policyList = policyList;
        }

    }

    static public class ClaimAndPayment extends GenericJson  {
        public ArrayList<ClaimMessage> claimList = new ArrayList<>();
        public ArrayList<PaymentMessage> paymentList = new ArrayList<>();

        public ClaimAndPayment() {
        }

        public ClaimAndPayment(ArrayList<ClaimMessage> claimList, ArrayList<PaymentMessage> paymentList) {
            this.claimList = claimList;
            this.paymentList = paymentList;
        }
    }

    static public class ClaimAndPayment2 extends GenericJson  {
        public Map<String, ClaimAndPayment> claimAndPaymentMap = new HashMap<>();

        public ClaimAndPayment2() {
        }
    }

    static public class CustomerPolicyClaimPayment extends GenericJson  {
        public CustomerAndPolicy customerAndPolicy = new CustomerAndPolicy();
        public ClaimAndPayment claimAndPayment = new ClaimAndPayment();

        public CustomerPolicyClaimPayment(CustomerAndPolicy customerAndPolicy, ClaimAndPayment claimAndPayment) {
            this.customerAndPolicy = customerAndPolicy;
            this.claimAndPayment = claimAndPayment;
        }
    }

    /************************************************************************
     * GENERIC METHODS
     ************************************************************************/

    static public class CustomerView extends GenericJson {
        public int customerKey;
        public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
        public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();
        public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();
        public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

        public CustomerView(){}

        public CustomerView(int customerKey) {
            this.customerKey = customerKey;
        }
    }

    private static ObjectMapper mapper = new ObjectMapper();
    static public class GenericJson {
        public String toString() {
            try {
                return mapper.writeValueAsString(this);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

    }

    static public <T> Serde createSerde(T clazz, Map<String, Object> serdeProps) {
        Serializer<T> serializer = new JsonPOJOSerializer<T>();

        serdeProps.put("JsonPOJOClass", clazz);
        serializer.configure(serdeProps, false);

        final Deserializer<T> deserializer = new JsonPOJODeserializer<T>();
        serdeProps.put("JsonPOJOClass", clazz);
        deserializer.configure(serdeProps, false);
        return Serdes.serdeFrom(serializer, deserializer);

    }
}
