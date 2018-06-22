package dk.schumacher.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import dk.schumacher.model.Messages;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.annotate.JsonIgnore;
import sun.tools.java.Type;

import java.util.*;

/**
 * @author Gøran Schumacher (GS) / Schumacher Consulting Aps
 * @version $Revision$ 14/06/2018
 */
public class AvroRecordBuilder {
    static public class Wrapper extends Messages.JsonToString implements Cloneable {
        public String name ="Data";
        public String type = "record";

        @JsonIgnore
        public String topicName = null;
        @JsonIgnore
        public Wrapper setTopicName(String topicName){
            this.topicName=topicName;
            return this;
        }

        @JsonIgnore
        public String tableName = null;
        @JsonIgnore
        public Wrapper setTableName(String tableName){
            this.tableName=tableName;
            return this;
        }

        @JsonIgnore
        Schema _schema = null;

        @JsonIgnore
        public Schema getSchema(){
            if (_schema == null)
                _schema = Schema.parse(this.toString());
            return _schema;
        }

        @JsonIgnore
        public GenericRecord getGenericRecord(){
            return new GenericData.Record(getSchema());
        }

        @JsonIgnore
        public Map<String, FieldAbstract> _fields = new HashMap<String, FieldAbstract>();

        public Wrapper(FieldAbstract... fields) {
            for (FieldAbstract field : fields) {
                this._fields.put(field.name, field);
            }
        }

        public static Wrapper createFromAvroSchema(Schema schema) {
            FieldAbstract[] fields = new FieldAbstract[schema.getFields().size()];
            for (int i = 0; i < schema.getFields().size(); i++) {
                fields[i] = FieldAbstract.createFromAvroSchema(schema.getFields().get(i));
            }
            return new Wrapper(fields);
        }

        @JsonProperty
        public Collection<FieldAbstract> getFields() {
            return _fields.values();
        }

        /**
         * Creates a new output schema ferom this and wrapper.
         * @param wrapper
         * @return
         */
        public Wrapper mergeSchema(Wrapper wrapper) {
            Wrapper clone = null;
            try {
                clone = (Wrapper)this.clone();
            for (FieldAbstract field : wrapper.getFields()) {
                clone._fields.put(field.name, field.clone());
            }
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            return clone;
        }

        public Wrapper removeFieldNamed(String fieldName) {
            Wrapper clone = null;
            try {
                clone = (Wrapper)this.clone();
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            clone._fields.remove(fieldName);
            return clone;
        }

        public Wrapper clone() throws CloneNotSupportedException {
            Wrapper clonedObj = (Wrapper)super.clone();
            clonedObj._fields = new HashMap<String, FieldAbstract>();
            for (FieldAbstract field : _fields.values())
                clonedObj._fields.put(field.name, field.clone());
            return clonedObj;
        }


        /**
         * Should be called from the to entry
         * @param from
         * @param to
         */
        public void copyFieldsTo(GenericRecord from, GenericRecord to) {
            for (FieldAbstract field : _fields.values()) {
                if(from.get(field.name) == null) {
                    System.out.println("Field " + field.name + " is null!!!");
                } else {
                    to.put(field.name, from.get(field.name));
                }
            }
        }

        public GenericRecord copyFields(GenericRecord from1, GenericRecord from2) {
            GenericRecord out = getGenericRecord();
            for (FieldAbstract field : _fields.values()) {
                if(from1.get(field.name) != null)
                    out.put(field.name, from1.get(field.name));
                if(from2 != null) {
                    if(from2.get(field.name) != null)
                        out.put(field.name, from2.get(field.name));
                }
            }
            return out;
        }
}

    public static abstract class FieldAbstract extends Messages.JsonToString  implements Cloneable {
        public String name ="fieldName";
        @JsonIgnore
        abstract protected String _getSimpleType();  // string / int
        @JsonIgnore
        public boolean _option = false;

        @JsonProperty
        public Object getType() {
            if (_option) {
                return new String[]{"null", _getSimpleType()};
            } else {
                return _getSimpleType();
            }
        }

        public FieldAbstract(String name) {
            this.name = name;
        }

        public FieldAbstract(String name, boolean option) {
            this.name = name;
            this._option=option;
        }

        public FieldAbstract clone() throws CloneNotSupportedException {
            FieldAbstract clonedObj = (FieldAbstract)super.clone();
            return clonedObj;
        }

        public static FieldAbstract createFromAvroSchema(Schema.Field field) {
            switch (findTypeFromField(field)) {
                case INT:
                    return new FieldInt(field.name(), field.schema().getType().getName().equals("union"));
                case STRING:
                    return new FieldString(field.name(), field.schema().getType().getName().equals("union"));
                default:
                    return null;
            }
        }

        private static Schema.Type findTypeFromField(Schema.Field field) {
            if(field.schema().getType().getName().equals("union")) {
                System.out.println("UNION");
                return field.schema().getTypes().get(1).getType();
            } else {
                return field.schema().getType();
            }
        }
    }

    public static class FieldString extends FieldAbstract {

        public FieldString(String name) {
            super(name);
        }

        public FieldString(String name, boolean option) {
            super(name, option);
        }

        @Override
        protected String _getSimpleType() {
            return "string";
        }
    }
    public static class FieldInt extends FieldAbstract {

        public FieldInt(String name) {
            super(name);
        }

        public FieldInt(String name, boolean option) {
            super(name, option);
        }

        @Override
        protected String _getSimpleType() {
            return "int";
        }
    }

    public static void main(String[] args) {

        Wrapper a = new Wrapper(
                new FieldInt("agentSkillTargetID"),
                new FieldInt("callTypeId"),
                new FieldString("aNI"),
                new FieldString("digitsDialed")
        );
        Wrapper b = new Wrapper(
                new FieldInt("callTypeId"),
                new FieldString("enterpriseName", true)
        );
        Wrapper c = a.mergeSchema(b);
        c.removeFieldNamed("callTypeId");
        System.out.println("Schema: " + c.getSchema());

        c.getSchema();
        c.getGenericRecord();

    }

}
