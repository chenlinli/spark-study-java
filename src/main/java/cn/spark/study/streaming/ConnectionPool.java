package cn.spark.study.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedList;

public class ConnectionPool {
    private static LinkedList<Connection> connectionQueue;

    //加载驱动
    static{
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
    //获取连接,多线程
    public synchronized static Connection getConnection(){
        try{
            if(connectionQueue==null){
                //创建10个链接
                connectionQueue = new LinkedList<>();
                for(int i =0;i<10;i++){
                    Connection con = DriverManager.getConnection(
                            "jdbc:mysql://spark1:3306/testdb"
                    ,"","");
                    connectionQueue.offer(con);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return connectionQueue.poll();

    }

    /**
     * 归还连接
     * @param con
     */
    public static void returnConnection(Connection con){
        connectionQueue.offer(con);
    }

}
