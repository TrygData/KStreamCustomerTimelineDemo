package dk.schumacher.cisco.model;

import dk.schumacher.util.AvroRecordBuilder.Wrapper;
import dk.schumacher.util.AvroRecordBuilder.Field;

//public class Constants {
//
//    public final static String TERM_CALL_DETAIL = new Wrapper(
//            new Field("agentSkillTargetID", "int"),  // Agents Id
//            new Field("callTypeId", "int"),
//            //new Field("skillTargetId", "string"),
//            new Field("aNI", "string"),                 // Callers number
//            new Field("digitsDialed", "string")         // Called number
//    ).toString();
//
//    public final static String CALL_TYPE = new Wrapper(
//            new Field("callTypeId", "int"),
//            new Field("enterpriseName", "string")
//    ).toString();
//
//    public final static String AGENT_TEAM_MEMBER = new Wrapper(
//            new Field("skillTargetId", "string"),
//            new Field("agentTeamId", "string")
//    ).toString();
//
//    public final static String AGENT_TEAM = new Wrapper(
//            new Field("agentTeamId", "string"),
//            new Field("enterpriseName", "string")
//    ).toString();
//
//    public final static String AGENT = new Wrapper(
//            new Field("agentSkillTargetID", "string"),
//            new Field("personId", "string")
//    ).toString();
//
//    public final static String PERSON = new Wrapper(
//            new Field("personId", "string"),
//            new Field("firstName", "string"),
//            new Field("lastName", "string"),
//            new Field("loginName", "string")
//    ).toString();
//
//
//    // ================= RESULT RECORD ======================================
//    public final static String CISCO_WHOLE = new Wrapper(
//            new Field("agentSkillTargetID", "int"),  // Agents Id
//            new Field("personId", "string", true),    //X
//            new Field("firstName", "string", true),    //X
//            new Field("lastName", "string", true),    //X
//            new Field("loginName", "string", true),    //X
//            new Field("callTypeId", "int"),
//            new Field("skillTargetId", "string", true),
//            new Field("agentTeamId", "string", true),
//            new Field("enterpriseName", "string", true),    //X
//            new Field("aNI", "string"),                 // Callers number
//            new Field("digitsDialed", "string")         // Called number
//    ).toString();
//
//
//    public static void main(String[] args) {
//        String TERM_CALL_DETAIL = new Wrapper(
//                new Field("agentSkillTargetID", "string"),
//                new Field("callTypeId", "string"),
//                new Field("skillTargetId", "string"),
//                new Field("agentTeamId", "string")
//        ).toString();
//        System.out.println(TERM_CALL_DETAIL);
//    }
//}
