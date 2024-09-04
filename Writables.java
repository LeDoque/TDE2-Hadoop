package TDE2;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Writables implements Writable {

    private String country_or_Area;
    private long year;
    private long comm_code;
    private String commodity;
    private String flow;
    private long trade_usd;
    private long weight_kg;
    private String quantity_name;
    private long quantity;
    private String category;

    public Writables() {
    }

    public Writables(String country_or_Area, long year, long comm_code, String commodity, String flow, long trade_usd, long weight_kg, String quantity_name, long quantity, String category) {
        this.country_or_Area = country_or_Area;
        this.year = year;
        this.comm_code = comm_code;
        this.commodity = commodity;
        this.flow = flow;
        this.trade_usd = trade_usd;
        this.weight_kg = weight_kg;
        this.quantity_name = quantity_name;
        this.quantity = quantity;
        this.category = category;
    }

    public String getCountry_or_Area() {
        return country_or_Area;
    }

    public void setCountry_or_Area(String country_or_Area) {
        this.country_or_Area = country_or_Area;
    }

    public long getYear() {
        return year;
    }

    public void setYear(long year) {
        this.year = year;
    }

    public long getComm_code() {
        return comm_code;
    }

    public void setComm_code(long comm_code) {
        this.comm_code = comm_code;
    }

    public String getCommodity() {
        return commodity;
    }

    public void setCommodity(String commodity) {
        this.commodity = commodity;
    }

    public String getFlow() {
        return flow;
    }

    public void setFlow(String flow) {
        this.flow = flow;
    }

    public long getTrade_usd() {
        return trade_usd;
    }

    public void setTrade_usd(long trade_usd) {
        this.trade_usd = trade_usd;
    }

    public long getWeight_kg() {
        return weight_kg;
    }

    public void setWeight_kg(long weight_kg) {
        this.weight_kg = weight_kg;
    }

    public String getQuantity_name() {
        return quantity_name;
    }

    public void setQuantity_name(String quantity_name) {
        this.quantity_name = quantity_name;
    }

    public long getQuantity() {
        return quantity;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(country_or_Area);
        dataOutput.writeUTF(String.valueOf(year));
        dataOutput.writeUTF(String.valueOf(comm_code));
        dataOutput.writeUTF(commodity);
        dataOutput.writeUTF(flow);
        dataOutput.writeUTF(String.valueOf(trade_usd));
        dataOutput.writeUTF(String.valueOf(weight_kg));
        dataOutput.writeUTF(quantity_name);
        dataOutput.writeUTF(String.valueOf(quantity));
        dataOutput.writeUTF(category);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country_or_Area = dataInput.readUTF();
        year = Long.parseLong(dataInput.readUTF());
        comm_code = Long.parseLong(dataInput.readUTF());
        commodity = dataInput.readUTF();
        flow = dataInput.readUTF();
        trade_usd = Long.parseLong(dataInput.readUTF());
        weight_kg = Long.parseLong(dataInput.readUTF());
        quantity_name = dataInput.readUTF();
        quantity = Long.parseLong(dataInput.readUTF());
        category = dataInput.readUTF();
    }

    public class MyWritableComparable implements WritableComparable<MyWritableComparable> {

        private String country_or_Area;
        private long year;

        private long trade_usd;

        public MyWritableComparable() {
        }

        public MyWritableComparable(String country_or_Area, long year, long trade_usd) {
            this.country_or_Area = country_or_Area;
            this.year = year;
            this.trade_usd = trade_usd;
        }

        public String getCountry_or_Area() {
            return country_or_Area;
        }

        public void setCountry_or_Area(String country_or_Area) {
            this.country_or_Area = country_or_Area;
        }

        public long getYear() {
            return year;
        }

        public void setYear(long year) {
            this.year = year;
        }

        public long getTrade_usd() {
            return trade_usd;
        }

        public void setTrade_usd(long trade_usd) {
            this.trade_usd = trade_usd;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(country_or_Area);
            dataOutput.writeLong(year);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            country_or_Area = dataInput.readUTF();
            year = dataInput.readLong();
        }

        @Override
        public int compareTo(MyWritableComparable o) {
            if (this.hashCode() < o.hashCode()) {
                return -1;
            } else if (this.hashCode() > o.hashCode()) {
                return 1;
            }
            return 0;
        }

        @Override
        public String toString(){
            return country_or_Area + year;
        }

        @Override
        public boolean equals(Object o){
            if(this == o) return true;
            if(o == null || getClass() != o.getClass()) return false;
            MyWritableComparable that = (MyWritableComparable) o;
            return year == that.year && Objects.equals(country_or_Area, that.country_or_Area);
        }

        @Override
        public int hashCode(){
            return Objects.hash(country_or_Area, year);
        }
    }
}
