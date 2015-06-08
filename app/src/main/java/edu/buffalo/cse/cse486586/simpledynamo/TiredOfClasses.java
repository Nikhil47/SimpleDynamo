package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Nikhil on 4/23/15.
 */
public final class TiredOfClasses {

    //Ports arranged in the increasing order of the SHA1 hashed outputs
    public final static String[] ports = {"5562", "5556", "5554", "5558", "5560"};
    public static Context context;
    public static SimpleDynamoProvider simpleDynamoProvider;
    public static Uri uri;
    public static int myPosition;
    public static String myID;
    public static SQLiteDatabase database;

    public TiredOfClasses(){}

    /*
    This method will take the key as an input and return an index for the port[].
    Using this the current AVD will contact the appropriate node.
     */
    public static int coordinatorForKey(String key){

        for(int i = 0;i < 5;i++){
            if(genHash(key).compareTo(genHash(ports[i])) < 0)
                return i;
        }
        return 0;
    }

    public static int myPosition(){
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        for(int i = 0;i < 5;i++){
            if(portStr.equals(ports[i]))
                return i;
        }
        return -1;
    }

    public static void insert(final HashMap mp) {

        new Thread(){
            public void run() {
                if(mp != null) {
                    HashMap map = mp;
                    Iterator it = map.entrySet().iterator();

                    while (it.hasNext()) {
                        Map.Entry pair = (Map.Entry) it.next();
                        //System.out.println(pair.getKey() + " = " + pair.getValue());

                        ContentValues contentValues = new ContentValues();
                        contentValues.put(DatabaseHelper.KEY, (String) pair.getKey());
                        contentValues.put(DatabaseHelper.VALUE, (String) pair.getValue());
                        String owner = TiredOfClasses.ports[TiredOfClasses.coordinatorForKey((String) pair.getKey())];
                        contentValues.put(DatabaseHelper.OWNER, owner);

Log.d(TiredOfClasses.class.getSimpleName(), "Map Size: " + map.size());
Log.d(TiredOfClasses.class.getSimpleName(), "Inserting key: " + pair.getKey() + " Owner: " + owner + " At: " + TiredOfClasses.myID);

                        if (contentValues != null)
                            TiredOfClasses.simpleDynamoProvider.insert(uri, contentValues);
                        //it.remove(); // avoids a ConcurrentModificationException
                    }
                }
            }
        }.start();
    }

    public HashMap queryCursorResolver(Cursor cursor){

        final HashMap<String, String> map = new HashMap<>();

        if(cursor.getCount() != 0) {
            cursor.moveToFirst();
            do {
                map.put(cursor.getString(0), cursor.getString(1));
                Log.d("TOCursorToMap", cursor.getCount() + " " + cursor.getString(0) + " " + cursor.getString(1));
            }while (cursor.moveToNext());
            cursor.close();

            return map;
        }
        else
            return null;
    }

    public Cursor unlockedQuery(String selection){

        selection = DatabaseHelper.OWNER + " = '" + selection + "'";

        SQLiteDatabase sqd = database;
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables(DatabaseHelper.TABLE);

        //selection = DatabaseHelper.OWNER + " = '" + selection + "'";
        Cursor cursor = sqLiteQueryBuilder.query(sqd, null, selection, null, null, null, null);

        return cursor;
    }

    public static void mapToCursor(final HashMap map, final String uuid){

        new Thread(){
            public void run(){
                String[] columns = {"key", "value"};
                MatrixCursor matrixCursor = new MatrixCursor(columns);
                Iterator it = map.entrySet().iterator();

                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();

                    String[] cols = {(String) pair.getKey(), (String) pair.getValue()};
                    Log.d(TiredOfClasses.class.getSimpleName(), "mapToCursor: " + pair.getKey() + " " + pair.getValue());
                    matrixCursor.addRow(cols);

                    it.remove(); // avoids a ConcurrentModificationException
                }
                matrixCursor.moveToFirst();
                SimpleDynamoProvider.mergedThreadCursorMap.put(uuid, matrixCursor);

                Log.d(ReceiverTask.class.getSimpleName(), "Generating interrupt");
                LockFace lock = SimpleDynamoProvider.threadUUIDHashMap.get(uuid);
                lock.openLock();
                SimpleDynamoProvider.threadUUIDHashMap.put(uuid, lock);
                synchronized (lock) {
                    lock.notify();
                }
            }
        }.start();
    }

    public static Uri buildUri(String scheme, String authority) {

        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public static synchronized Boolean isInPorts(String input){

        for(int i = 0;i < 5;i++)
            if(input.equals(ports[i]))
                return true;

        return false;
    }

    public static String genHash(String input) {

        String hash = new String();
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            hash = formatter.toString();
        }catch(NoSuchAlgorithmException nsae){
            Log.e(TiredOfClasses.class.getSimpleName(), "No Such Algorithm Exception");
        }
        return hash;
    }

    public int getRecoveryCount(){
        SQLiteDatabase sqLiteDatabase = database;
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables("recovery");

        Cursor cursor = sqLiteQueryBuilder.query(sqLiteDatabase, null, null, null, null, null, null);
        Log.d(TiredOfClasses.class.getSimpleName(), "Recovery col count " + cursor.getCount());
        cursor.moveToFirst();
        return cursor.getInt(0);
    }

    public void setRecoveryCount(){
        SQLiteDatabase sqd = database;
        sqd.execSQL("update recovery set count = 1;");

        return;
    }
}