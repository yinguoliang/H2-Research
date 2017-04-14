package my.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class BaseTest {
    static Properties prop = new Properties();
    static String url = "jdbc:h2:tcp://localhost:9092/test9";
    static Random random = new Random();
    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        String[] values = new String[]{"1","1111","1111111"};
        Connection conn = DriverManager.getConnection("jdbc:h2:d:/h2/basetest.db", "sa", "");
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS my_table");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS my_table(id varchar(34),name varchar(50),addr varchar(200))");
        for(int i=0;i<100;i++){
//            if(i%1000==0){
//                System.out.println(">>>>>>>>>>>");
//            }
//            Thread.sleep(200);
            String sql = String.format("INSERT INTO my_table(id,name,addr) VALUES('%s','%s','%s')"
                    ,i,"AAAA"+i,"BBBB"+values[random.nextInt(10000)%values.length] );
            stmt.executeUpdate(sql);
        }
        System.out.println("................delete.................");
        for(int i=0;i<1000;i++){
            String sql = String.format("delete from my_table where id = %s"
                    , random.nextInt(100000));
            stmt.executeUpdate(sql);
        }
        ResultSet rs = stmt.executeQuery("SELECT * FROM my_table");
        rs.next();
        System.out.println(rs.getString(1)+","+rs.getString(2)+","+rs.getString(3));

        stmt.close();
        conn.close();
    }
}
