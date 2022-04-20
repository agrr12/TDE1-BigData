package TDE;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FlowTypeCommodityWritable implements WritableComparable<FlowTypeCommodityWritable> {

    String commodity;
    String flowType;

    public FlowTypeCommodityWritable(){}

    public FlowTypeCommodityWritable(String commodity, String flowType) {
        this.flowType = flowType;
        this.commodity = commodity;

    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getFlowType() {
        return flowType;
    }

    public void setFlowType(String flowType) {
        this.flowType = flowType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlowTypeCommodityWritable that = (FlowTypeCommodityWritable) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(flowType, that.flowType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, flowType);
    }

    @Override
    public int compareTo(FlowTypeCommodityWritable o) {
        if(this.hashCode() > o.hashCode()){
            return +1;
        }else if(this.hashCode() < o.hashCode()){
            return -1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(commodity); // MUITO CUIDADO COM A ORDEM!
        dataOutput.writeUTF(flowType); // MUITO CUIDADO COM A ORDEM!
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        flowType = dataInput.readUTF(); // DEVE SEGUIR A MESMA ORDEM DA ESCRITA!
    }

    @Override
    public String toString() {
        return flowType+";"+commodity;
    }
}
