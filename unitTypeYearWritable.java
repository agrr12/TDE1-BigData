package TDE;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class unitTypeYearWritable implements WritableComparable<unitTypeYearWritable> {

    private String UnitType;
    private String Year;

    public unitTypeYearWritable() {
    }

    public unitTypeYearWritable(String unitType, String year) {
        UnitType = unitType;
        Year = year;
    }

    public String getUnitType() {
        return UnitType;
    }

    public void setUnitType(String unitType) {
        UnitType = unitType;
    }

    public String getYear() {
        return Year;
    }

    public void setYear(String year) {
        Year = year;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        unitTypeYearWritable that = (unitTypeYearWritable) o;
        return Objects.equals(UnitType, that.UnitType) && Objects.equals(Year, that.Year);
    }

    @Override
    public int hashCode() {
        return Objects.hash(UnitType, Year);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(UnitType); // MUITO CUIDADO COM A ORDEM!
        dataOutput.writeUTF(Year); // MUITO CUIDADO COM A ORDEM!
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        UnitType = dataInput.readUTF();
        Year = dataInput.readUTF(); // DEVE SEGUIR A MESMA ORDEM DA ESCRITA!
    }

    @Override
    public int compareTo(unitTypeYearWritable o) {
        if(this.hashCode() > o.hashCode()){
            return +1;
        }else if(this.hashCode() < o.hashCode()){
            return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return UnitType+"|"+Year;

    }
}
