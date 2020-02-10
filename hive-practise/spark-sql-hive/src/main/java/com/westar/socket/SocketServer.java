package com.westar.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServer {
    public static void main(String[] args) {

        try{
            ServerSocket server = null;
            server = new ServerSocket(5209);
            //b)指定绑定的端口，并监听此端口。
            System.out.println("服务器启动成功");
            //创建一个ServerSocket在端口5209监听客户请求

            while (true){
                Socket socket = null;
                //2、调用accept()方法开始监听，等待客户端的连接
                //使用accept()阻塞等待客户请求，有客户
                //请求到来则产生一个Socket对象，并继续执行
                socket = server.accept();

                handle(socket);
            }

        }catch (IOException e){
            System.out.println("没有启动监听：" + e);
            //出错，打印出错信息
        }catch (Exception e){
            System.out.println("Error." + e);
        }
    }

    private static void handle(Socket socket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        PrintWriter writer = new PrintWriter(socket.getOutputStream());
        String line = in.readLine();

        if (line.equals("add")) {
            System.out.println("client add");
            writer.println("server add");
        } else if (line.equals("ping")) {
            System.out.println("client ping");
            writer.println("server ping");
        } else {
            System.out.println("client not support operation");
            writer.println("not support operation");
        }

        //向客户端输出该字符串
        writer.flush();
    }
}
