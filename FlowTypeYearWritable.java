package TDE;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowTypeYearWritable implements WritableComparable<FlowTypeYearWritable> {
    /**
     * Todo writable precisa ser um Java BEAN!
     * 1- Construtor vazio (OK)
     * 2- Gets e sets (OK)
     * 3- Comparação entre objetos (OK)
     * 4- Atributos privados (OK)
     */

    private String FlowType;
    private String Year;

    public FlowTypeYearWritable() {
    }

    public FlowTypeYearWritable(String FlowType, String Year) {
        this.FlowType= FlowType;
        this.Year = Year;
    }

    public String getFlowType() {
        return FlowType;
    }

    public void setFlowType(String flowType) {
        FlowType = flowType;
    }

    public String getYear() {
        return Year;
    }

    public void setYear(String year) {
        Year = year;
    }

    @Override
    public int compareTo(FlowTypeYearWritable o) {
        if(this.hashCode() > o.hashCode()){
            return +1;
        }else if(this.hashCode() < o.hashCode()){
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(FlowType); // MUITO CUIDADO COM A ORDEM!
        dataOutput.writeUTF(Year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        FlowType = dataInput.readUTF(); // DEVE SEGUIR A MESMA ORDEM DA ESCRITA!
        Year = dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowTypeYearWritable that = (FlowTypeYearWritable) o;
        return Objects.equals(FlowType, that.FlowType) && Objects.equals(Year, that.Year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(FlowType, Year);
    }

    @Override
    public String toString() {
        return Year + " | " + FlowType;
    }
}
