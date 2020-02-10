package com.westar.thrift;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import tutorial.Calculator;

public class ThriftJavaServer {
    public static CalculatorHandler handler;

    public static Calculator.Processor processor;

    public static void main(String[] args) {
        try{
            handler = new CalculatorHandler();
            processor = new Calculator.Processor(handler);

            Runnable simple = new Runnable() {
                @Override
                public void run() {
                    service(processor);
                }


            };
            new Thread(simple).start();

        }catch (Exception x){
            x.printStackTrace();
        }
    }

    private static void service(Calculator.Processor processor) {
        try{
            TServerTransport serverTransport = new TServerSocket(9090);
            TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));
            System.out.println("Starting the service server...");
            server.serve();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
