package dk.schumacher.avro;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

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

    static public class CustomerMessage {
        public String ADDRESS;
        public String CUSTOMER;
        public Double CUSTOMERTIME;
        public String POLICY;
    }

    static public class PolicyMessage {
        public int PVAR1;
        public String POLICYENDTIME;
        public int POLICY;
        public String POLICYSTARTTIME;
        public int PVAR0;
    }

    static public class CustomerList {
        public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
        String s = new String();

        public CustomerList() {
        }
    }

    static public class PolicyList {
        public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();

        public PolicyList() {
        }
    }

    @JsonIgnoreProperties({ "policy" })
    static public class ClaimMessage {
        public Double CLAIMTIME;
        public String CLAIMNUMBER;
        public Double CLAIMREPORTTIME;
        public String CLAIMCOUNTER;

        public int getPolicy() {
            return Integer.parseInt(CLAIMNUMBER.split("_")[0]);
        }
    }

    static public class ClaimList {
        public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();

        public ClaimList() {
        }
    }





    @JsonIgnoreProperties({ "policy" })
    static public class PaymentMessage {
        public Double PAYMENT;
        public Double PAYTIME;
        public Integer CLAIMCOUNTER;
        public String CLAIMNUMBER;

        public int getPolicy() {
            return Integer.parseInt(CLAIMNUMBER.split("_")[0]);
        }
    }

    static public class PaymentList {
        public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

        public PaymentList() {
        }
    }

    static public class CustomerAndPolicy {
        public CustomerList customerList = new CustomerList();
        public PolicyList policyList = new PolicyList();

        public CustomerAndPolicy() {
        }

        public CustomerAndPolicy(CustomerList customerList, PolicyList policyList) {
            this.customerList = customerList;
            this.policyList = policyList;
        }

    }

    static public class ClaimAndPayment {
        public ClaimList claimList = new ClaimList();
        public PaymentList paymentList = new PaymentList();

        public ClaimAndPayment() {
        }

        public ClaimAndPayment(ClaimList claimList, PaymentList paymentList) {
            this.claimList = claimList;
            this.paymentList = paymentList;
        }
    }

    static public class ClaimAndPayment2 {
        public Map<String, ClaimAndPayment> claimAndPaymentMap = new HashMap<>();

        public ClaimAndPayment2() {
        }

        public void add(final ClaimAndPayment claimAndPaymentMessage) {
            String key = claimAndPaymentMessage.claimList.claimRecords.get(0).CLAIMNUMBER
                    + claimAndPaymentMessage.claimList.claimRecords.get(0).CLAIMTIME.toString();
            if (claimAndPaymentMap.containsKey(key)) {
                remove(key);
            }
            claimAndPaymentMap.put(key, claimAndPaymentMessage);
        }

        private void remove(final String key) {
            claimAndPaymentMap.remove(key);
        }
    }

    static public class CustomerPolicyClaimPayment {
        public CustomerAndPolicy customerAndPolicy = new CustomerAndPolicy();
        public ClaimAndPayment claimAndPayment = new ClaimAndPayment();

        public CustomerPolicyClaimPayment() {
        }

        public CustomerPolicyClaimPayment(CustomerAndPolicy customerAndPolicy, ClaimAndPayment claimAndPayment) {
            this.customerAndPolicy = customerAndPolicy;
            this.claimAndPayment = claimAndPayment;
        }
    }

    static public class CustomerView {
        public int cutomerKey;
        public ArrayList<CustomerMessage> customerRecords = new ArrayList<>();
        public ArrayList<PolicyMessage> policyRecords = new ArrayList<>();
        public ArrayList<ClaimMessage> claimRecords = new ArrayList<>();
        public ArrayList<PaymentMessage> paymentRecords = new ArrayList<>();

        public CustomerView() {
        }
    }



    static {
        // define CustomerMessageSerde
        Map<String, Object> serdeProps = new HashMap<String, Object>();
        Serializer<CustomerMessage> customerMessageSerializer = new JsonPOJOSerializer<CustomerMessage>();

        serdeProps.put("JsonPOJOClass", CustomerMessage.class);
        customerMessageSerializer.configure(serdeProps, false);

        final Deserializer<CustomerMessage> customerMessageDeserializer = new JsonPOJODeserializer<CustomerMessage>();
        serdeProps.put("JsonPOJOClass", CustomerMessage.class);
        customerMessageDeserializer.configure(serdeProps, false);
        customerMessageSerde = Serdes.serdeFrom(customerMessageSerializer,
                customerMessageDeserializer);


        // define customerListSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<CustomerList> customerListSerializer = new JsonPOJOSerializer<CustomerList>();
        serdeProps.put("JsonPOJOClass", CustomerList.class);
        customerListSerializer.configure(serdeProps, false);

        final Deserializer<CustomerList> customerListDeserializer = new JsonPOJODeserializer<CustomerList>();
        serdeProps.put("JsonPOJOClass", CustomerList.class);
        customerListDeserializer.configure(serdeProps, false);
        customerListSerde = Serdes.serdeFrom(customerListSerializer,
                customerListDeserializer);

        // define policySerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<PolicyMessage> policyMessageSerializer = new JsonPOJOSerializer<PolicyMessage>();
        serdeProps.put("JsonPOJOClass", PolicyMessage.class);
        policyMessageSerializer.configure(serdeProps, false);

        final Deserializer<PolicyMessage> policyMessageDeserializer = new JsonPOJODeserializer<PolicyMessage>();
        serdeProps.put("JsonPOJOClass", PolicyMessage.class);
        policyMessageDeserializer.configure(serdeProps, false);
        policyMessageSerde = Serdes.serdeFrom(policyMessageSerializer,
                policyMessageDeserializer);


        // define policyListSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<PolicyList> policyListSerializer = new JsonPOJOSerializer<PolicyList>();
        serdeProps.put("JsonPOJOClass", PolicyList.class);
        policyListSerializer.configure(serdeProps, false);

        final Deserializer<PolicyList> policyListDeserializer = new JsonPOJODeserializer<PolicyList>();
        serdeProps.put("JsonPOJOClass", PolicyList.class);
        policyListDeserializer.configure(serdeProps, false);
        policyListSerde = Serdes.serdeFrom(policyListSerializer, policyListDeserializer);

        // define claimMessageSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<ClaimMessage> claimMessageSerializer = new JsonPOJOSerializer<ClaimMessage>();
        serdeProps.put("JsonPOJOClass", ClaimMessage.class);
        claimMessageSerializer.configure(serdeProps, false);

        final Deserializer<ClaimMessage> claimMessageDeserializer = new JsonPOJODeserializer<ClaimMessage>();
        serdeProps.put("JsonPOJOClass", ClaimMessage.class);
        claimMessageDeserializer.configure(serdeProps, false);
        claimMessageSerde = Serdes.serdeFrom(claimMessageSerializer,
                claimMessageDeserializer);

        // define claimListSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<ClaimList> claimListSerializer = new JsonPOJOSerializer<ClaimList>();
        serdeProps.put("JsonPOJOClass", ClaimList.class);
        claimListSerializer.configure(serdeProps, false);

        final Deserializer<ClaimList> claimListDeserializer = new JsonPOJODeserializer<ClaimList>();
        serdeProps.put("JsonPOJOClass", ClaimList.class);
        claimListDeserializer.configure(serdeProps, false);
        claimListSerde = Serdes.serdeFrom(claimListSerializer, claimListDeserializer);

        // define paymentSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<PaymentMessage> paymentMessageSerializer = new JsonPOJOSerializer<PaymentMessage>();
        serdeProps.put("JsonPOJOClass", PaymentMessage.class);
        paymentMessageSerializer.configure(serdeProps, false);

        final Deserializer<PaymentMessage> paymentMessageDeserializer = new JsonPOJODeserializer<PaymentMessage>();
        serdeProps.put("JsonPOJOClass", PaymentMessage.class);
        paymentMessageDeserializer.configure(serdeProps, false);
        paymentMessageSerde = Serdes.serdeFrom(paymentMessageSerializer,
                paymentMessageDeserializer);

        // define paymentListSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<PaymentList> paymentListSerializer = new JsonPOJOSerializer<PaymentList>();
        serdeProps.put("JsonPOJOClass", PaymentList.class);
        paymentListSerializer.configure(serdeProps, false);

        final Deserializer<PaymentList> paymentListDeserializer = new JsonPOJODeserializer<PaymentList>();
        serdeProps.put("JsonPOJOClass", PaymentList.class);
        paymentListDeserializer.configure(serdeProps, false);
        paymentListSerde = Serdes.serdeFrom(paymentListSerializer, paymentListDeserializer);


        /************************************************************************
         * NESTED
         ************************************************************************/

        // define claimAndPaymentSerde
        serdeProps = new HashMap<String, Object>();
        final Serializer<ClaimAndPayment> claimAndPaymentSerializer = new JsonPOJOSerializer<ClaimAndPayment>();
        serdeProps.put("JsonPOJOClass", ClaimAndPayment.class);
        claimAndPaymentSerializer.configure(serdeProps, false);

        final Deserializer<ClaimAndPayment> claimAndPaymentDeserializer = new JsonPOJODeserializer<ClaimAndPayment>();
        serdeProps.put("JsonPOJOClass", ClaimAndPayment.class);
        claimAndPaymentDeserializer.configure(serdeProps, false);
        claimAndPaymentSerde = Serdes.serdeFrom(claimAndPaymentSerializer,
                claimAndPaymentDeserializer);

        // define claimAndPayment2Serde
        serdeProps = new HashMap<String, Object>();
        final Serializer<ClaimAndPayment2> claimAndPayment2Serializer = new JsonPOJOSerializer<ClaimAndPayment2>();
        serdeProps.put("JsonPOJOClass", ClaimAndPayment2.class);
        claimAndPayment2Serializer.configure(serdeProps, false);

        final Deserializer<ClaimAndPayment2> claimAndPayment2Deserializer = new JsonPOJODeserializer<ClaimAndPayment2>();
        serdeProps.put("JsonPOJOClass", ClaimAndPayment2.class);
        claimAndPayment2Deserializer.configure(serdeProps, false);
        claimAndPayment2Serde = Serdes.serdeFrom(claimAndPayment2Serializer,
                claimAndPayment2Deserializer);

        // define customerViewserde
        serdeProps = new HashMap<String, Object>();
        final Serializer<CustomerView> customerViewSerializer = new JsonPOJOSerializer<CustomerView>();
        serdeProps.put("JsonPOJOClass", CustomerView.class);
        customerViewSerializer.configure(serdeProps, false);

        final Deserializer<CustomerView> customerViewDeserializer = new JsonPOJODeserializer<CustomerView>();
        serdeProps.put("JsonPOJOClass", CustomerView.class);
        customerViewDeserializer.configure(serdeProps, false);
        customerViewSerde = Serdes.serdeFrom(customerViewSerializer,
                customerViewDeserializer);
    }
}
