package edu.buffalo.cse.cse486586.simpledynamo;

import android.util.Log;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Created by Nikhil on 4/23/15.
 */
public final class Sockets {

    public Socket socks;
    public ObjectOutputStream outputStream;
    public OutputStream oStream;
    public ObjectInputStream inputStream;
    public InputStream iStream;
    public LoadCarrier loadCarrier;
    public static Boolean pingOn = true;
    public static Boolean pingOff = false;

    public Sockets(int node, LoadCarrier loadCarrier, Boolean pingOn){

        this.makeSocket(node);
        this.loadCarrier = loadCarrier;
        Log.d(Sockets.class.getSimpleName(), "Socket Creation done");
        Log.d(Sockets.class.getSimpleName(), loadCarrier.toString());

        if(pingOn)
            this.ping(node);
        else
            this.write(loadCarrier);
    }

    public void ping(final int node){

        if(socks.isConnected())
            this.write(this.loadCarrier);
        else {
            Log.d(Sockets.class.getSimpleName(), "Socket Disconnected, Trying again");
            new Sockets((node) % 5, loadCarrier, Sockets.pingOn);
        }

        try {
            iStream = socks.getInputStream();
            inputStream = new ObjectInputStream(iStream);

            Object ip = inputStream.readObject();

            if(Integer.parseInt(ip.toString()) == 1) {
                //update threadIntegerHashMap here
                Log.d(Sockets.class.getSimpleName(), "Ack Received");
            }
            else
                Log.d(Sockets.class.getSimpleName(), "Unexpected Object Received");

        } catch (EOFException|SocketTimeoutException ex){
            Log.e(Sockets.class.getSimpleName(), "In Ping thread EOF Exception");

            loadCarrier.timeToLive--;
            SimpleDynamoProvider.gates++;

            if(loadCarrier.timeToLive > 1) {

                new Sockets((node + 1) % 5, loadCarrier, Sockets.pingOff);
            }
            if(loadCarrier.timeToLive == 1 && loadCarrier.messageType.equals("59")) {

                loadCarrier.timeToLive = 2;
                new Sockets(TiredOfClasses.myPosition, loadCarrier, Sockets.pingOff);
            }
        } /*catch (SocketTimeoutException ste){
            Log.e(Sockets.class.getSimpleName(), "Ping timed out: Sending to next node");

            loadCarrier.timeToLive--;

            if(loadCarrier.timeToLive > 1) {

                new Sockets((node + 1) % 5, loadCarrier, Sockets.pingOff);
            }
            if(loadCarrier.timeToLive == 1 && loadCarrier.messageType.equals("59")) {

                loadCarrier.timeToLive = 2;
                new Sockets(TiredOfClasses.myPosition, loadCarrier, Sockets.pingOff);
            }
        }*/ catch (IOException e) {
            Log.e(Sockets.class.getSimpleName(), "In Ping Thread IO Exception " + e.toString());
        } catch(Exception ex){}
    }

    public void makeSocket(int node){

        try {
            socks = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    2 * Integer.parseInt(TiredOfClasses.ports[node]));
            Log.d(Sockets.class.getSimpleName(), "Sending to: " + socks.toString());
            socks.setSoTimeout(2000);
        }
        catch(UnknownHostException uhe){
            Log.e(Sockets.class.getSimpleName(), "Connecting to Unknown Host");
        }
        catch(IOException ioe){
            Log.e(Sockets.class.getSimpleName(), "IO Exception");
        }
    }

    public void write(LoadCarrier lc){
        try {

            Log.d(Sockets.class.getSimpleName(), "Writing: " + lc.temp + "to " + socks.toString() + " with TTL: " + lc.timeToLive);

            oStream = socks.getOutputStream();
            outputStream = new ObjectOutputStream(oStream);
            outputStream.flush();
            outputStream.writeObject(lc);
            Log.d(Sockets.class.getSimpleName(), "Packet Sent");

        }catch(IOException ioe){
            Log.e(Sockets.class.getSimpleName(), "IO Exception in write");
        }
    }

    public int getTimeToLive(){
        return loadCarrier.timeToLive;
    }

    public void destroySocket(){
        try{
            outputStream.close();
            inputStream.close();
            socks.close();
        } catch (IOException e) {
            Log.d(Sockets.class.getSimpleName(), "Error in closing connection");
        }
    }
}