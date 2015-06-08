package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by Nikhil on 4/23/15.
 */
public class ServerTask implements Runnable {

    ServerSocket acceptor;

    @Override
    public void run() {
        try {
            acceptor = new ServerSocket(10000);

            while(true){
                Log.d(ServerTask.class.getSimpleName(), "Server Task loop");
                Socket sock = acceptor.accept();
                new Thread(new ReceiverTask(sock)).start();
            }
        }catch(IOException ioe){
            Log.e(ServerTask.class.getSimpleName(), "Server Socket IO Exception");
        }
    }
}