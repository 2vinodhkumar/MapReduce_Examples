import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class Employee implements WritableComparable<Employee> {

	private String empName;
	private Integer empID;
	
	public String getEmpName() {
		return empName;
	}

	public void setEmpName(String empName) {
		this.empName = empName;
	}

	public Integer getEmpID() {
		return empID;
	}

	public void setEmpID(Integer empID) {
		this.empID = empID;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		empName=WritableUtils.readString(in);
		empID=WritableUtils.readVInt(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(out, empName);
		WritableUtils.writeVLong(out, empID);
	}

	@Override
	public int compareTo(Employee o) {
		int cmp=empName.compareTo(o.getEmpName());
		if(cmp!=0)
		{
			return cmp;
		}
		
		// TODO Auto-generated method stub
		return empID.compareTo(o.getEmpID());
	}

	
}
