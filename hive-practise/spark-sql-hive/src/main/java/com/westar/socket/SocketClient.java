package com.westar.socket;

import org.apache.spark.sql.execution.SortPrefixUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class SocketClient {
    // 搭建客户端
    public static void main(String[] args) throws IOException {
        try{
            Socket socket = new Socket("127.0.0.1",5209);
            System.out.println("客户端启动成功");

            PrintWriter write = new PrintWriter(socket.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            write.print("ping");
            write.flush();
            String line = in.readLine();
            System.out.println("server: " + line);

            in.close();
            write.close();
            socket.close();
        }catch (Exception e){
            System.out.println("can not listen to:" + e);// 出错，打印出错信息
        }

    }
}
