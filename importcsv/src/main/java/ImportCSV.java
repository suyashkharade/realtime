

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

public class ImportCSV {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String JDBC_DB_URL = "jdbc:mysql://localhost:3306/sample";
    static final String JDBC_USER = "suyash";
    static final String JDBC_PASS = "";
    private static GenericObjectPool gPool = null;

    public DataSource setUpPool() throws Exception {
        Class.forName(JDBC_DRIVER);
        // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!
        gPool = new GenericObjectPool();
        gPool.setMaxActive(5);
        // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!
        ConnectionFactory cf = new DriverManagerConnectionFactory(JDBC_DB_URL, JDBC_USER, JDBC_PASS);
        // Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory to Add Object Pooling Functionality!
        PoolableConnectionFactory pcf = new PoolableConnectionFactory(cf, gPool, null, null, false, true);
        return new PoolingDataSource(gPool);
    }


    private static void insert(Connection conn,Long id,String event,String entityType,
                String entityId,String targetEntityType,
                String targetEntityId,String properties,String eventTime) throws ClassNotFoundException,SQLException {
        String insertQuery="insert into pio_event_1(id,event,entityType,entityId,targetEntityType,targetEntityId,properties,eventTime,eventTimeZone,creationTimeZone)" +
                " values(?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstmt = conn.prepareStatement(insertQuery);
        pstmt.setString(1,Long.toString(id));
        pstmt.setString(2,event);
        pstmt.setString(3,entityType);
        pstmt.setString(4,entityId);
        pstmt.setString(5,targetEntityType);
        pstmt.setString(6,targetEntityId);
        pstmt.setString(7,properties);
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(Long.parseLong(eventTime)*1000);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String dateStr = sdf.format(cal.getTime());
        pstmt.setTimestamp(8, Timestamp.valueOf(dateStr));
        pstmt.setString(9, "UTC");
        pstmt.setString(10, "UTC");
        pstmt.execute();
    }
    public static void main(String[] args) throws Exception {
        //Connection Pool
        ImportCSV importCSV = new ImportCSV();
        DataSource dataSource = importCSV.setUpPool();

        BufferedReader br= new BufferedReader(new FileReader(args[0]));
        BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]));
        String line = br.readLine();
        long cnt=0;
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = dataSource.getConnection();
//        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/predictionio","root", "password");
        while (line!=null) {
            String result="";
            String[] cols = line.split("\t");
//            result+= UUID.randomUUID()+"\t";
//            result+=cols[0]+"\t";
//            result+="user"+"\t";
//            result+=cols[2]+"\t";
//            result+="hotel"+"\t";
//            result+=cols[5]+"\t";
//            result+="{}"+"\t";
//            result+=cols[1]+"\t";
            if(cols.length>5)
                insert(conn,++cnt,cols[0],"user",cols[2],"hotel",cols[5],"{}",cols[1]);
            line = br.readLine();
        }
        conn.close();
        br.close();
    }
}
