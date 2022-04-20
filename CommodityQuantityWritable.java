package TDE;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityQuantityWritable implements WritableComparable<CommodityQuantityWritable> {

    String commodity;
    String quantity;

    public CommodityQuantityWritable(){}

    public CommodityQuantityWritable(String commodity, String flowType) {
        this.quantity = flowType;
        this.commodity = commodity;

    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommodityQuantityWritable that = (CommodityQuantityWritable) o;
        return Objects.equals(commodity, that.commodity) && Objects.equals(quantity, that.quantity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(commodity, quantity);
    }

    @Override
    public int compareTo(CommodityQuantityWritable o) {
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
        dataOutput.writeUTF(quantity); // MUITO CUIDADO COM A ORDEM!
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        commodity = dataInput.readUTF();
        quantity = dataInput.readUTF(); // DEVE SEGUIR A MESMA ORDEM DA ESCRITA!
    }

    @Override
    public String toString() {
        return quantity +";"+commodity;
    }
}
