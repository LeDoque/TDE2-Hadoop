package TDE2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class WritableAnoPais implements WritableComparable<WritableAnoPais> {

    private String year;
    private String country_or_area;

    public WritableAnoPais() {
    }

    public WritableAnoPais(String year, String country_or_area) {
        this.year = year;
        this.country_or_area = country_or_area;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getCountry_or_area() {
        return country_or_area;
    }

    public void setCountry_or_area(String country_or_area) {
        this.country_or_area = country_or_area;
    }

    @Override
    public String toString() {
        return "WritableAnoPais{" +
                "year=" + year +
                ", country_or_area='" + country_or_area + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WritableAnoPais that = (WritableAnoPais) o;
        return year == that.year && Objects.equals(country_or_area, that.country_or_area);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, country_or_area);
    }

    @Override
    public int compareTo(WritableAnoPais o) {
        if (this.hashCode() < o.hashCode()) {
            return - 1;
        } else if (this.hashCode() > o.hashCode()) {
            return 1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(String.valueOf(country_or_area));
        dataOutput.writeUTF(String.valueOf(year));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        country_or_area = dataInput.readUTF();
        year = dataInput.readUTF();
    }
}