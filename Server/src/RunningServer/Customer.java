package RunningServer;

import java.io.*;
public class Customer implements Serializable
{
    private int custNumber;
    private String firstName;
    private String surname;
    private String phoneNum;
    private double credit;
    private boolean canRent;
    
    public Customer(){  
    }
    
    public Customer(int custNumber, String fName, String lName, String phone, double credAmt, boolean can){
        setCustNumber(custNumber);
        setName(fName);
        setSurname(lName);
        setPhoneNum(phone);
        setCredit(credAmt);
        setCanRent(can);
    }
    
    public void setCustNumber(int custNumber)  {
    	this.custNumber = custNumber;
    }
    
    public void setName(String sFName)
    {
        firstName = sFName;
    }
    
    public void setSurname(String sSName)
    {
        surname = sSName;
    }
    
    public void setPhoneNum(String sPhone)
    {
        phoneNum = sPhone;
    }
    
    public void setCredit(double sCredAmt)
    {
        credit = sCredAmt;
    }
    public void setCanRent(boolean can)
    {
        canRent = can;
    }
    
    public int getCustNumber()  {
    	return custNumber;
    }
    public boolean canRent()  {
    	return canRent;
    }
    
    public String getName()
    {
        return firstName;
    }
    
    public String getSurname()
    {
        return surname;
    }
    
    public String getPhoneNum()
    {
        return phoneNum;
    }
    
    public double getCredit()
    {
        return credit;
    }
    
    //this method can be edited to format strings differently
    @Override
    public String toString()
    {
        return String.format("%-8d%-10s%-10s%-10s     %.2f\t%-6b", getCustNumber(), getName(), getSurname(),
                getPhoneNum(), getCredit(), canRent);
    }      
}
