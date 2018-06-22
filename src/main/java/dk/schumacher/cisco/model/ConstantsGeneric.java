package dk.schumacher.cisco.model;

import dk.schumacher.util.AvroRecordBuilder;
import dk.schumacher.util.AvroRecordBuilder.Wrapper;

public class ConstantsGeneric {

    public final static Wrapper TERM_CALL_DETAIL = new Wrapper(
            new AvroRecordBuilder.FieldInt("agentSkillTargetID"),  // Agents Id
            new AvroRecordBuilder.FieldInt("callTypeId"),
            new AvroRecordBuilder.FieldString("aNI"),                 // Callers number
            new AvroRecordBuilder.FieldString("digitsDialed", true)         // Called number
    )
            .setTopicName("TermCallDetailX")
            .setTableName("");

    public final static Wrapper CALL_TYPE = new Wrapper(
            new AvroRecordBuilder.FieldInt("callTypeId"),
            new AvroRecordBuilder.FieldString("enterpriseName")
    )
            .setTopicName("CallType2")
            .setTableName("");

    public final static Wrapper CISCO_WHOLE = TERM_CALL_DETAIL.mergeSchema(CALL_TYPE).setTopicName("Whole2");

    public static void main(String[] args) {
        Wrapper TERM_CALL_DETAIL = new Wrapper(
                new AvroRecordBuilder.FieldString("agentSkillTargetID"),
                new AvroRecordBuilder.FieldString("callTypeId"),
                new AvroRecordBuilder.FieldString("skillTargetId"),
                new AvroRecordBuilder.FieldString("agentTeamId")
        );
        System.out.println(TERM_CALL_DETAIL.toString());
        TERM_CALL_DETAIL.getSchema();
        TERM_CALL_DETAIL.getGenericRecord();
    }
}
