/**
 * Server.java
 * This is my version DVD Rentals Server Class
 * This Class connect the Database and send feedback to the client
 * @author Athenkosi Zono (218030185)
 * Date 7 November 2020
 */
package RunningServer;

import java.io.*;
import java.net.*;
import java.sql.*;
import java.util.*;
import javax.swing.JOptionPane;

public class Server {
    
    final String DATABASE_URL = "jdbc:derby://localhost:1527/RentalDB";
    final String PASSWORD = "nbuser";
    final String USERNAME = "nbuser";
    
    Statement stat = null;
    Connection conn = null;
    ResultSet res = null;
    PreparedStatement pStat = null;
    
    public static void main(String[] args) throws ClassNotFoundException {
        new Server().idle();
    }
    
    private void idle() throws ClassNotFoundException{
        ServerSocket ss = null;
        String msg;
        
        try {
            ss = new ServerSocket(7777);
        } 
        catch (IOException e) {
            System.out.println("Error:" + e.getMessage());
            System.exit(0);
        }
        try{
            while(true){
                try {
                    System.out.println("Server running...waiting for connection at port 7777");
                    Socket s = ss.accept();
                    ObjectInputStream in = new ObjectInputStream(s.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(new PrintStream(s.getOutputStream()));

                    msg = (String)in.readObject();

                    if(msg.equals("1")){
                        String x = addCustomer((Customer) in.readObject());
                        out.writeObject(x);
                    }
                    else if(msg.equals("2")){
                        String x = addDVD((DVD) in.readObject());
                        out.writeObject(x);
                    }
                    else if(msg.equals("3")){
                        String data[] = in.readObject().toString().split(",");
                        int cusNum = Integer.parseInt(data[0]);
                        int dvdNum = Integer.parseInt(data[1]);
                        String x = rentDvd(cusNum, dvdNum);
                        out.writeObject(x);
                    }
                    else if(msg.equals("4")){
                        String v = returnDvd((int)in.readObject());
                        out.writeObject(v);
                    }
                    else if(msg.equals("5")){
                        out.writeObject(getAllMovies());
                    }
                    else if(msg.equals("6")){
                        out.writeObject(getAllCustomers());
                    }
                    else if(msg.equals("7")){
                        ArrayList x = searchCustomer((String)in.readObject());
                        out.writeObject(x);
                    }
                    else if(msg.equals("8")){
                        switch((String)in.readObject()){
                            case "getAllRentals":
                                out.writeObject(getAllRentals());
                                break;
                            case "getOutstandingRentals":
                                out.writeObject(getOutstandingRentals());
                                break;
                            case "getDailyRentals":
                                ArrayList x = getDailyRentals((String)in.readObject());
                                out.writeObject(x);
                                break;
                        } 
                    }
                    else if(msg.equals("9")){
                        switch((String)in.readObject()){
                            case "CUSTOMER":
                                out.writeObject(deleteRecord((int)in.readObject(), "CUSTOMER"));
                                break;
                            case "DVD":
                                out.writeObject(deleteRecord((int)in.readObject(), "DVD"));
                                break;
                        }
                    }
                    else if(msg.equals("getCategory")){
                        out.writeObject(getMovieCategories());
                    }
                    else if(msg.equals("getCustomers")){
                        out.writeObject(getCustomers());
                    }
                    else if(msg.equals("getAvailMovies")){
                        out.writeObject(getAvailMovies((String)in.readObject()));
                    }
                    else if(msg.equals("exit")){
                        System.out.println("System exited...all connections closed");
                        System.exit(0);
                    }
                    s.close();
                    System.out.println("connection closed..");
                }
                catch (IOException e) {
                    System.out.println("Error: " + e.getMessage());
                }
            }
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage());
        }
        finally{
            if(ss != null){
                try {
                    ss.close();
                } 
                catch(IOException ex){
                    JOptionPane.showMessageDialog(null, "Couldn't close Server Socket\n"+ex.getMessage());
                }
            }  
        }
    }
    
    private String getTodayDate(){
        Calendar cal2 = new GregorianCalendar();
        String str = cal2.getTime().toString();
        String yr = str.substring(str.length()-4);
        String today = yr+"/"+(cal2.getTime().getMonth()+1)+"/"+cal2.getTime().getDate();
        return today;
    }
    
    private String rentDvd(int cusNum, int dvdNum){
        int ok = -1;
        double cred = 0.0;
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT CANRENT, CREDIT FROM CUSTOMER WHERE CUSTNUMBER = "+cusNum);
            while (res.next()) {
                if(!res.getBoolean("CANRENT"))
                    return "-3##0";
                else
                   cred = res.getDouble("CREDIT");
            }
            res = stat.executeQuery("SELECT * FROM DVD WHERE DVDNUMBER = "+dvdNum);
            while (res.next()) {
                cred = cred - res.getDouble("PRICE");
                if(cred < 0)
                    return "-2##0";
                else{
                    String sql = "UPDATE DVD SET AVAILABLEFORRENT = 'FALSE' WHERE DVDNUMBER = "+dvdNum;
                    pStat = conn.prepareStatement(sql);
                    ok = pStat.executeUpdate();
                }
                if(ok > 0){
                    ok = -1;
                    String sql = "UPDATE CUSTOMER SET CANRENT = 'FALSE', CREDIT = "+cred+" WHERE CUSTNUMBER = "+cusNum;
                    pStat = conn.prepareStatement(sql);
                    ok = pStat.executeUpdate();
                }
                if(ok > 0){
                    Rental rent = new Rental(generateUniqueKey("RENTAL", "RENTALNUMBER"), getTodayDate(), cusNum, dvdNum);
                    return addRental(rent)+"##"+rent.getRentalNumber();
                }
            }
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return "-1##0";
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return "-1##0";
        }
        return "-1##0";
    }
    
    private int generateUniqueKey(String table, String column){
        int last = 0;
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT "+column+" FROM "+table);
            while (res.next()) {
                last = res.getInt(column);
            }
            last ++;
            while(!isUnique(last, table, column)){
                last ++;
            }
            return last;
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getErrorCode(), "Warning", JOptionPane.ERROR_MESSAGE);
            return -1;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return -1;
        }
    }
    
    private String returnDvd(int rentalNum){
        String message = "-2,0";
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT * FROM RENTAL WHERE RENTALNUMBER = "+rentalNum);
            while (res.next()) {
                if(!res.getString("DATERETURNED").equalsIgnoreCase("NA"))
                    message = "-3,0";
                else{
                    Rental rent = new Rental(); 
                    rent.setRentalNumber(res.getInt("RENTALNUMBER"));
                    rent.setDateRented(res.getString("DATERENTED"));
                    rent.setCustNumber(res.getInt("CUSTNUMBER"));
                    rent.setdvdNumber(res.getInt("DVDNUMBER"));
                    rent.setDateReturned(getTodayDate());
                    if(updateRental(rentalNum, getTodayDate(), rent.getTotalPenaltyCost(), rent.getCustNumber(), rent.getDvdNumber()) > 0)
                        message = rent.numberOfDaysOverdue()+","+rent.getTotalPenaltyCost();
                    else
                        message = "-4,0";
                    }
            }    
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, "101>"+sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, "102>"+e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        finally {
            try {
                if(conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (stat != null)
                    stat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (res != null)
                    res.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        }
        System.out.println("Messege = "+message);
        return message;
    }
    
    private int updateRental(int rentalNum, String date, double penaltyCost, int custNum, int dvdNum){
        int ok = 0;
        double credit = 0.0;
        boolean canRent = false;
        try {
            String sql = "UPDATE RENTAL SET DATERETURNED = '"+date+"', TOTALPENALTYCOST = "+penaltyCost+" WHERE RENTALNUMBER = "+rentalNum;
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            pStat = conn.prepareStatement(sql);
            ok = pStat.executeUpdate();
            if(ok > 0){
                ok = 0;
                sql = "UPDATE DVD SET AVAILABLEFORRENT = 'TRUE' WHERE DVDNUMBER = "+dvdNum;
                pStat = conn.prepareStatement(sql);
                ok = pStat.executeUpdate();
            }
            if(ok > 0){
                ok = 0;
                sql ="SELECT CREDIT FROM CUSTOMER WHERE CUSTNUMBER ="+custNum;
                stat = conn.createStatement();
                res = stat.executeQuery(sql);
                while (res.next()) {
                    credit = res.getDouble("CREDIT");
                }
                credit = credit-penaltyCost;
                if(credit > 0)
                    canRent = true;
                sql = "UPDATE CUSTOMER SET CREDIT = "+credit+", CANRENT = '"+canRent+"' WHERE CUSTNUMBER = "+custNum;
                pStat = conn.prepareStatement(sql);
                ok = pStat.executeUpdate();
            }
        }
        catch(SQLException sqlException) {
            JOptionPane.showMessageDialog(null, "103>Error: Could not update. " + sqlException);
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, "104>"+e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        return ok;
    }
    
    private String addCustomer(Customer cus){
        cus.setCustNumber(generateUniqueKey("CUSTOMER", "CUSTNUMBER"));
        int ok;
        String sql = "INSERT INTO CUSTOMER VALUES (?, ?, ?, ?, ?, ?)";
        try{
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            pStat = conn.prepareStatement(sql);
            
            pStat.setInt(1, cus.getCustNumber());
            pStat.setString(2, cus.getName());
            pStat.setString(3, cus.getSurname());
            pStat.setString(4, cus.getPhoneNum());
            pStat.setDouble(5, cus.getCredit());
            pStat.setBoolean(6, cus.canRent());
            
            ok = pStat.executeUpdate();
            return ok+"##"+cus.getCustNumber();
        }
        catch(SQLException e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return "-10##0";
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return "-10##0";
        }
        finally {
            try {
                if(conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (pStat != null)
                    pStat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        }
    }
    
    private String addDVD(DVD dvd){
        dvd.setDvdNumber(generateUniqueKey("DVD", "DVDNUMBER"));
        int ok;
        String sql = "INSERT INTO DVD VALUES (?, ?, ?, ?, ?, ?)";
        try{
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            pStat = conn.prepareStatement(sql);
            
            pStat.setInt(1, dvd.getDvdNumber());
            pStat.setString(2, dvd.getTitle());
            pStat.setString(3, dvd.getCategory());
            pStat.setDouble(4, dvd.getPrice());
            pStat.setBoolean(5, dvd.isNewRelease());
            pStat.setBoolean(6, dvd.isAvailable());
            
            ok = pStat.executeUpdate();
            return ok+"##"+dvd.getDvdNumber();
        }
        catch(SQLException e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return "-10##0";
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return "-10##0";
        }
        finally {
            try {
                if(conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (pStat != null)
                    pStat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        }
    }
    
    private int addRental(Rental rent){
        int ok = 0;
        String sql = "INSERT INTO RENTAL VALUES (?, ?, ?, ?, ?, ?)";
        try{
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            pStat = conn.prepareStatement(sql);
            pStat.setInt(1, rent.getRentalNumber());
            pStat.setString(2, rent.getDateRented());
            pStat.setString(3, rent.getDateReturned());
            pStat.setInt(4, rent.getCustNumber());
            pStat.setInt(5, rent.getDvdNumber());
            pStat.setDouble(6, rent.getTotalPenaltyCost());
            
            ok = pStat.executeUpdate();
            return ok;
        }
        catch(SQLException e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return -1;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return -1;
        }
        finally {
            try {
                if(conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (pStat != null)
                    pStat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        } 
        
    }
    
    private int deleteRecord(int key, String table){
       // (-10 error)   (-1 ID not found) (-2 ID cannot be deleted at the moment) else deleted
        int ok;
        String sql;
        if(table.equals("CUSTOMER")){
            if(isUnique(key, table, "CUSTNUMBER"))
                return -1;
            try{
                conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
                stat = conn.createStatement();
                res = stat.executeQuery("SELECT CREDIT, CANRENT FROM CUSTOMER WHERE CUSTNUMBER = "+key);
                while (res.next()) {
                    if(res.getDouble("CREDIT") < 0 || !res.getBoolean("CANRENT"))
                        return -2;
                } 
            }
            catch(SQLException e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
                return -10;
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
                return -10;
            }
            sql = "DELETE FROM "+table+" WHERE CUSTNUMBER = "+key;
        }
        else{
            if(isUnique(key, table, "DVDNUMBER"))
                return -1;
            try{
                conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
                stat = conn.createStatement();
                res = stat.executeQuery("SELECT AVAILABLEFORRENT FROM DVD WHERE DVDNUMBER = "+key);
                while (res.next()) {
                    if(!res.getBoolean("AVAILABLEFORRENT"))
                        return -2;
                } 
            }
            catch(SQLException e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
                return -10;
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
                return -10;
            }
            sql = "DELETE FROM "+table+" WHERE DVDNUMBER = "+key;
        }
        
        try{
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            pStat = conn.prepareStatement(sql);
            ok = pStat.executeUpdate();
            return ok;
        }
        catch(SQLException e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return -10;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return -10;
        }
    }
    
    private ArrayList<String> getCustomers(){
        ArrayList<String> customers = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT * FROM CUSTOMER WHERE CANRENT = 'TRUE'");
            while (res.next()) {
                customers.add(res.getString("FIRSTNAME")+" "+res.getString("SURNAME")+" : "+res.getString("CUSTNUMBER"));
            }
            Collections.sort(customers);
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        finally {
            try {
                if (res != null)
                    res.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (stat != null)
                    stat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        } 
        return customers;
    }
    
    private ArrayList<String> getAllMovies(){
        ArrayList<String> movies = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT * FROM DVD ORDER BY CATEGORY, TITLE");
            int inc = 1;
            while (res.next()) {
                movies.add(inc+"##"+res.getInt("DVDNUMBER")+"##"+res.getString("TITLE")+"##"+res.getString("CATEGORY")+"##"+res.getDouble("PRICE")+"##"+res.getBoolean("NEWRELEASE")+"##"+res.getBoolean("AVAILABLEFORRENT"));
                inc++;
            } 
            return movies;
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return movies;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return movies;
        }
        finally {
            try {
                if (res != null)
                    res.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (stat != null)
                    stat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        } 
    }
    
    private ArrayList<String> getAllCustomers(){
        ArrayList<String> customers = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT * FROM CUSTOMER ORDER BY FIRSTNAME, SURNAME");
            int inc = 1;
            while (res.next()) {
                customers.add(inc+"##"+res.getInt("CUSTNUMBER")+"##"+res.getString("FIRSTNAME")+"##"+res.getString("SURNAME")+"##"+res.getString("PHONENUM")+"##"+res.getDouble("CREDIT")+"##"+res.getBoolean("CANRENT"));
                inc++;
            } 
            return customers;
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return customers;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return customers;
        }
        finally {
            try {
                if (res != null)
                    res.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (stat != null)
                    stat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        } 
    }

    private ArrayList<String> searchCustomer(String title){
        ArrayList<String> customers = new ArrayList<>();
        if(title.isEmpty())
            return customers;
        title = title.toUpperCase();
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT TITLE, CATEGORY, AVAILABLEFORRENT FROM DVD WHERE UPPER(TITLE) LIKE '"+title+"%'");
            int inc = 1;
            while (res.next()) {
                customers.add(inc+"##"+res.getString("TITLE")+"##"+res.getString("CATEGORY")+"##"+res.getBoolean("AVAILABLEFORRENT"));
                inc++;
            } 
            return customers;
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return customers;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return customers;
        }
        finally{
            try {
                if (res != null)
                    res.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (stat != null)
                    stat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        } 
    }
    
    private ArrayList<String> getAvailMovies(String category){
        ArrayList<String> availMovies = new ArrayList();
        if(category.equals("-----SELECT-----"))
            return availMovies;
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT * FROM DVD WHERE AVAILABLEFORRENT = 'TRUE' AND CATEGORY = '"+category+"'");
            while (res.next()) {
                availMovies.add(res.getString("DVDNUMBER")+"::"+res.getString("TITLE")+"::"+res.getString("PRICE"));
            }    
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        finally {
            try {
                if (res != null)
                    res.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (stat != null)
                    stat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        } 
        return availMovies;
    }
    
    private ArrayList<String> getMovieCategories(){
        ArrayList<String> categories = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT DISTINCT CATEGORY FROM DVD");
            while (res.next()) {
                categories.add(res.getString("CATEGORY"));
            }  
            Collections.sort(categories);
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
        }
        finally {
            try {
                if (res != null)
                    res.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (stat != null)
                    stat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        } 
        return categories;
    }
    
    private ArrayList<String> getAllRentals(){
        ArrayList<String> rents = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT * FROM RENTAL ORDER BY DATERENTED");
            int x = 1;
            while (res.next()) {
                rents.add(x+"##"+res.getInt("RENTALNUMBER")+"##"+res.getString("DATERENTED")+"##"+res.getString("DATERETURNED")+"##"+res.getString("CUSTNUMBER")+"##"+res.getString("DVDNUMBER")+"##"+res.getString("TOTALPENALTYCOST"));
                x++;
            }
            return rents;
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return rents;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return rents;
        }
    }
    
    private ArrayList<String> getOutstandingRentals(){
        ArrayList<String> outRents = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT * FROM RENTAL WHERE DATERETURNED = 'NA'");
            int x = 1;
            while (res.next()) {
                outRents.add(x+"##"+res.getInt("RENTALNUMBER")+"##"+res.getString("DATERENTED")+"##"+res.getString("CUSTNUMBER")+"##"+res.getString("DVDNUMBER"));
                x++;
            }
            return outRents;
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return outRents;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return outRents;
        }
    }
    
    private ArrayList<String> getDailyRentals(String date){
        ArrayList<String> rents = new ArrayList<>();
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT * FROM RENTAL WHERE DATERENTED = '"+date+"'");
            int x = 1;
            while (res.next()) {
                rents.add(x+"##"+res.getInt("RENTALNUMBER")+"##"+res.getString("DATERENTED")+"##"+res.getString("DATERETURNED")+"##"+res.getString("CUSTNUMBER")+"##"+res.getString("DVDNUMBER")+"##"+res.getString("TOTALPENALTYCOST"));
                x++;
            }
            return rents;
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return rents;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return rents;
        }
    }
    
    private boolean isUnique(int key, String table, String column){
        try {
            conn = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
            stat = conn.createStatement();
            res = stat.executeQuery("SELECT "+column+" FROM "+table);
            while (res.next()) {
                if(res.getInt(column) == key)
                    return false;
            }
        }
        catch(SQLException sqlException){
            JOptionPane.showMessageDialog(null, sqlException.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        catch(Exception e){
            JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            return false;
        }
        finally {
            try {
                if (res != null)
                    res.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (stat != null)
                    stat.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
            try {
                if (conn != null)
                    conn.close();
            }
            catch(Exception e){
                JOptionPane.showMessageDialog(null, e.getMessage(), "Warning", JOptionPane.ERROR_MESSAGE);
            }
        } 
        return true;
    }
     
}
