package dk.schumacher.cisco.model;

import dk.schumacher.model.Messages.JsonToString;
import org.apache.kafka.common.serialization.Serde;

import java.util.HashMap;
import java.util.Map;

import static dk.schumacher.model.Messages.createSerde;

/**
 * @author Gøran Schumacher (GS) / Schumacher Consulting Aps
 * @version $Revision$ 16/05/2018
 */
// SERDES ARE NOT NEEDED WHEN USING AVRO!!!!!!
//public class Messages {
//    public static Map<String, Object> serdeProps = new HashMap<String, Object>();
//
//    public static Serde<TERM_CALL_DETAIL> callMessageSerde = createSerde(TERM_CALL_DETAIL.class, serdeProps);
//    public static Serde<CALL_TYPE> callTypeMessageSerde = createSerde(CALL_TYPE.class, serdeProps);
//    public static Serde<AGENT_TEAM_MEMBER> agentTeamMemberMessageSerde = createSerde(AGENT_TEAM_MEMBER.class, serdeProps);
//    public static Serde<AGENT_TEAM> agentTeamMessageSerde = createSerde(AGENT_TEAM.class, serdeProps);
//    public static Serde<AGENT> agentMessageSerde = createSerde(AGENT.class, serdeProps);
//    public static Serde<PERSON> personMessageSerde = createSerde(PERSON.class, serdeProps);
//
//    /************************************************************************
//     * NESTED
//     ************************************************************************/
//
//
//    /************************************************************************
//     * MESSAGES
//     ************************************************************************/
//
//    static public class TERM_CALL_DETAIL extends JsonToString {
//        public String agentSkillTargetID;
//        public String callTypeId;
//        public Double skillTargetId;
//        public String agentTeamId;
//        public Double aNI;
//        public String digitsDialed;
//    }
//
//    static public class CALL_TYPE extends JsonToString {
//        public String callTypeId;
//        public String enterpriseName;
//    }
//
//    static public class AGENT_TEAM_MEMBER extends JsonToString {
//        public String skillTargetId;
//        public String agentTeamId;
//    }
//
//    static public class AGENT_TEAM extends JsonToString {
//        public String agentTeamId;
//        public String enterpriseName;
//    }
//
//    static public class AGENT extends JsonToString {
//        public String agentSkillTargetID;
//        public String pwersonId;
//    }
//
//    static public class PERSON extends JsonToString {
//        public String ersonId;
//        public String firstName;
//        public String lastName;
//        public String loginName;
//    }
//}
