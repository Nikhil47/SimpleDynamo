package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.graphics.Matrix;
import android.util.Log;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashMap;

/**
 * Created by Nikhil on 4/23/15.
 */
public class ReceiverTask implements Runnable {

    private Socket sock;
    private ObjectInputStream inputStream;
    private ObjectOutputStream outputStream;
    private Object ip;
    private static int lockCount = 0;

    public ReceiverTask(Socket sock){
        this.sock = sock;
    }

    @Override
    public void run() {
        try {

            outputStream = new ObjectOutputStream(sock.getOutputStream());
            outputStream.flush();
            inputStream = new ObjectInputStream(sock.getInputStream());

            ip = inputStream.readObject();

            switch (Integer.parseInt(ip.toString())) {

                //This case is for sending the heartbeat
                case 1: {

                    //outputStream.writeObject(new LoadCarrier("alive", 1, 0));

                    LoadCarrier loadCarrier = (LoadCarrier) ip;

                    if(loadCarrier.uuid != null){

                        if(SimpleDynamoProvider.threadUUIDHashMap.get(loadCarrier.uuid) != null) {
                            Log.d(ReceiverTask.class.getSimpleName(), "Wake Up: " + loadCarrier.uuid);
                            LockFace lock = SimpleDynamoProvider.threadUUIDHashMap.get(loadCarrier.uuid);
                            lock.openLock();
                            SimpleDynamoProvider.threadUUIDHashMap.put(loadCarrier.uuid, lock);

                            synchronized (lock) {
                                lock.notify();
                            }
                        }
                        else
                            Log.e(ReceiverTask.class.getSimpleName(), "Duplicate Ack for same record");

                        Log.d(ReceiverTask.class.getSimpleName(), "Case 1 interrupt: Delete/Insert Success");

                        return;
                    }

                    Log.d(ReceiverTask.class.getSimpleName(), "Case 1 timeToLive: " + loadCarrier.timeToLive);

                    return;
                }

                case 11: {

                    LoadCarrier loadCarrier = ((LoadCarrier) ip);

                    TiredOfClasses.mapToCursor(loadCarrier.map, loadCarrier.uuid);
                    Log.d(ReceiverTask.class.getSimpleName(), "Wake up call by: " + loadCarrier.owner);

                    return;
                }

                //Data Packet has been received
                case 56: {

                    try {
                        outputStream.writeObject(new LoadCarrier("alive", 1, null));
                    }catch(SocketException se){
                        Log.d(ReceiverTask.class.getSimpleName(), "Ack not sent for 56");
                    }

                    int timeToLive = 1; //If error revert this to 0

                    LoadCarrier lc = ((LoadCarrier) ip);
                    lc.timeToLive--;

                    if(lc.whoToSendBack == -1) {
                        Log.d(ReceiverTask.class.getSimpleName(), "Recovery Data Received:");
                        if(!SimpleDynamoProvider.threadUUIDHashMap.get(SimpleDynamoActivity.uuid).getLock()){
                            lockCount++;
                            Log.d(ReceiverTask.class.getSimpleName(), "lockCount value: " + lockCount);
                        }
                        if(lockCount == 3) {
                            SimpleDynamoProvider.threadUUIDHashMap.get(SimpleDynamoActivity.uuid).openLock();
                            Log.d(ReceiverTask.class.getSimpleName(), "Transactions unlocked");
                        }
                    }

                    if(lc.timeToLive >= 1) {
                        TiredOfClasses.insert(lc.map);

                        Log.d(ReceiverTask.class.getSimpleName(), lc.temp + "packet timeToLive: " + lc.timeToLive + " going to: " +
                                (TiredOfClasses.myPosition + 1) % 5);

                        if (lc.timeToLive > 1)
                            timeToLive = new Sockets((TiredOfClasses.myPosition + 1) % 5, lc, Sockets.pingOn).getTimeToLive();

                        if (lc.whoToSendBack != -1 && lc.timeToLive == 1)
                            new Sockets(lc.whoToSendBack, new LoadCarrier("alive", timeToLive, lc.uuid), Sockets.pingOff);
                    }

                    return;
                }

                case 57: {

                    //Send my own id so that sender knows where to send
                    //when I receive a request here I forward

                    //In case of recovery result,
                    // 1. For first query keep TTL as 3 and not 4
                    // 2. For second and third query keep TTL as 4
                    // If TTL expires here then that node should service the request
                    // Request should be forwarded with pingOn

                    try {
                        outputStream.writeObject(new LoadCarrier("alive", 1, null));
                    }catch(SocketException se){
                        Log.d(ReceiverTask.class.getSimpleName(), "Ack not sent for 57");
                    }

                    LoadCarrier lc = (LoadCarrier) ip;
                    lc.timeToLive--;

                    // Insert result from query() into 56 type packet
                    if(lc.timeToLive == 1) {

                        String[] selectionArgs = {"NULL"};
                        String selection = lc.owner;
                        //Cursor cursor = TiredOfClasses.simpleDynamoProvider.query(TiredOfClasses.uri,
                        //        null, selection, selectionArgs, null);
                        Cursor cursor = new TiredOfClasses().unlockedQuery(selection);

                        Log.d(ReceiverTask.class.getSimpleName(), "Sending to: " + TiredOfClasses.ports[lc.whoToSendBack] + " Owner of keys: " + lc.owner);
                        HashMap map = new TiredOfClasses().queryCursorResolver(cursor);
                        LoadCarrier loadCarrier = new LoadCarrier(map, "insert", -1, null);
                        loadCarrier.timeToLive = 2;

                        new Sockets(lc.whoToSendBack, loadCarrier, Sockets.pingOff);
                    }
                    else
                        new Sockets((TiredOfClasses.myPosition + 1) % 5, lc, Sockets.pingOn);

                    return;
                }

                case 58: {

                    try {
                        outputStream.writeObject(new LoadCarrier("alive", 1, null));
                    }catch(SocketException se){
                        Log.d(ReceiverTask.class.getSimpleName(), "Ack not sent for 58");
                    }

                    LoadCarrier loadCarrier = ((LoadCarrier) ip);
                    loadCarrier.timeToLive--;

                    if(TiredOfClasses.myPosition == loadCarrier.whoToSendBack){

                        TiredOfClasses.mapToCursor(loadCarrier.queryMap, loadCarrier.uuid);
                        Log.d("Debug:", "Generating interrupt");
                        return;
                    }

                    HashMap<String, String> mergeMap = new HashMap<>();
                    mergeMap.putAll(loadCarrier.queryMap);
                    String selection = "\"@\"";
                    Cursor cursor = TiredOfClasses.simpleDynamoProvider.query(TiredOfClasses.uri,
                            null, selection, null, null);
                    HashMap map = new TiredOfClasses().queryCursorResolver(cursor);

                    mergeMap.putAll(map);

                    loadCarrier.queryMap = mergeMap;
                    new Sockets((TiredOfClasses.myPosition + 1) % 5, loadCarrier, Sockets.pingOn);

                    return;
                }

                case 59: {

                    try {
                        outputStream.writeObject(new LoadCarrier("alive", 1, null));
                    }catch(SocketException se){
                        Log.d(ReceiverTask.class.getSimpleName(), "Ack not sent for 59");
                    }

                    LoadCarrier lc = (LoadCarrier) ip;
                    lc.timeToLive--;
                    Log.d(ReceiverTask.class.getSimpleName(), "Received query req for: " + lc.queryKey + "by" + TiredOfClasses.ports[lc.whoToSendBack]);

                    // Insert result from query() into 56 type packet
                    if (lc.timeToLive == 1) {

                        String[] selectionArgs = {"NULL"};
                        String selection = lc.queryKey;
                        Cursor cursor = TiredOfClasses.simpleDynamoProvider.query(TiredOfClasses.uri,
                                null, selection, selectionArgs, null);

                        HashMap map = new TiredOfClasses().queryCursorResolver(cursor);
                        Log.d(ReceiverTask.class.getSimpleName(), "Case 59: Map Size" + map.size());

                        LoadCarrier loadCarrier = new LoadCarrier(map, "wakeup", lc.uuid);
                        loadCarrier.owner = TiredOfClasses.ports[TiredOfClasses.myPosition()];

                        new Sockets(lc.whoToSendBack, loadCarrier, Sockets.pingOff);
                    }
                    else
                        new Sockets((TiredOfClasses.myPosition + 1) % 5, lc, Sockets.pingOn);

                    return;
                }

                case 60: {

                    try {
                        outputStream.writeObject(new LoadCarrier("alive", 1, null));
                    }catch(SocketException se){
                        Log.d(ReceiverTask.class.getSimpleName(), "Ack not sent for 60");
                    }

                    int timeToLive = 1;
                    LoadCarrier loadCarrier = ((LoadCarrier) ip);
                    loadCarrier.timeToLive--;

                    if (loadCarrier.queryKey.equals("\"@\"") && loadCarrier.timeToLive == 1) {

                        LockFace lock = SimpleDynamoProvider.threadUUIDHashMap.get(loadCarrier.uuid);
                        lock.openLock();
                        SimpleDynamoProvider.threadUUIDHashMap.put(loadCarrier.uuid, lock);
                        synchronized (lock) {
                            lock.notify();
                        }
                        Log.d(ReceiverTask.class.getSimpleName(), "Generating interrupt on Delete Thread");

                        TiredOfClasses.simpleDynamoProvider.delete(TiredOfClasses.uri, loadCarrier.queryKey, null);
                        new Sockets((TiredOfClasses.myPosition + 1) % 5, loadCarrier, Sockets.pingOff);
                    }
                    else{

                        String[] selectionArgs = {"NULL"};
                        TiredOfClasses.simpleDynamoProvider.delete(TiredOfClasses.uri, loadCarrier.queryKey, selectionArgs);
                        Log.d(ReceiverTask.class.getSimpleName(), "Deleted " + loadCarrier.queryKey + " at: " + TiredOfClasses.ports[TiredOfClasses.myPosition]);

                        if(loadCarrier.timeToLive > 1) {
                            Log.d(ReceiverTask.class.getSimpleName(), "Sending out for deletion: " + loadCarrier.queryKey);
                            timeToLive = new Sockets((TiredOfClasses.myPosition + 1) % 5, loadCarrier, Sockets.pingOn).getTimeToLive();
                        }

                        if(loadCarrier.timeToLive == 1)
                            new Sockets(loadCarrier.whoToSendBack, new LoadCarrier("alive", timeToLive, loadCarrier.uuid), Sockets.pingOff);
                    }

                    return;
                }
            }
        } catch(SocketTimeoutException ste){
            Log.e(ReceiverTask.class.getSimpleName(), "Socket TimeOut Exception while reading stream");
        } catch(EOFException eofe){
            Log.e(ReceiverTask.class.getSimpleName(), "EOF Exception while reading stream");
        } catch (IOException ioe) {
            Log.e(ReceiverTask.class.getSimpleName(), "IO Exception " + ioe.getMessage());
            ioe.printStackTrace();
        } catch (ClassNotFoundException cnfe) {
            Log.e(ReceiverTask.class.getSimpleName(), "USO received");
        }
    }
}
