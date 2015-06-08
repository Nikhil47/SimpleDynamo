package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    public static DatabaseHelper databaseHelper;
    public static Map<String, LockFace> threadUUIDHashMap;
    public static Map<String, MatrixCursor> mergedThreadCursorMap;
    public static int gates = 0;
    public Object query, insert, delete;

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

        String uuid = String.valueOf(UUID.randomUUID());
        SQLiteDatabase sqLiteDatabase = databaseHelper.getDB();
        Log.d(SimpleDynamoProvider.class.getSimpleName(), "Delete called for: " + selection);

        while(!threadUUIDHashMap.get(SimpleDynamoActivity.uuid).getLock())
            continue;

        if(selection.equals("\"*\"")){

            synchronized (delete) {
                sqLiteDatabase.delete(DatabaseHelper.TABLE, null, null);
            }

            LockFace lock = new LockFace();
            threadUUIDHashMap.put(uuid, lock);
            LoadCarrier lc = new LoadCarrier("delete", -1, "\"@\"", uuid);
            lc.timeToLive = 6;

            new Sockets((TiredOfClasses.myPosition + 1) % 5, lc, Sockets.pingOff);

            synchronized (lock) {
                try {
                    while (!threadUUIDHashMap.get(uuid).getLock()) {
                        lock.wait();
                    }
                } catch (InterruptedException ie) {
                    Log.d("Debug:", "Delete * Interrupted");
                }
            }
            threadUUIDHashMap.remove(uuid);
        }

        else if(selection.equals("\"@\"")){
            synchronized (delete) {
                sqLiteDatabase.delete(DatabaseHelper.TABLE, null, null);
            }
        }

        else {

            if (selectionArgs == null) {

                int index = TiredOfClasses.coordinatorForKey(selection);
                String owner = TiredOfClasses.ports[index];
                String self = TiredOfClasses.ports[TiredOfClasses.myPosition];

                LockFace lock = new LockFace();
                threadUUIDHashMap.put(uuid, lock);
                if (owner.equals(self)) {
                    new Sockets(index,
                            new LoadCarrier("delete", TiredOfClasses.myPosition, selection, uuid), Sockets.pingOff);
                    //threadIntegerHashMap.put(Thread.currentThread().getId(), 4);
                }
                else {
                    new Sockets(index,
                            new LoadCarrier("delete", TiredOfClasses.myPosition, selection, uuid), Sockets.pingOn);
                    //threadIntegerHashMap.put(Thread.currentThread().getId(), 4);
                }

                synchronized (lock) {
                    try {
                        while (!threadUUIDHashMap.get(uuid).getLock()) {
                            lock.wait();
                        }
                    } catch (InterruptedException ie) {
                        Log.d("Debug:", "Delete Interrupted");
                    }
                }
                threadUUIDHashMap.remove(uuid);
            }

            else{
                selection = DatabaseHelper.KEY + " = '" + selection + "'";
                synchronized (delete) {
                    sqLiteDatabase.delete(DatabaseHelper.TABLE, selection, null);
                }
            }
        }

		return 1;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

        String uuid = String.valueOf(UUID.randomUUID());

        int index = TiredOfClasses.coordinatorForKey(values.getAsString(DatabaseHelper.KEY));
        String owner = TiredOfClasses.ports[index];
        String self = TiredOfClasses.ports[TiredOfClasses.myPosition];

        while(!threadUUIDHashMap.get(SimpleDynamoActivity.uuid).getLock())
            continue;

        if(values.size() == 2) {
            HashMap<String, String> map = new HashMap<String, String>();

            map.put(values.getAsString(DatabaseHelper.KEY),
                    values.getAsString(DatabaseHelper.VALUE));

            LockFace lock = new LockFace();
            threadUUIDHashMap.put(uuid, lock);
            if(owner.equals(self)) {
                Log.d(SimpleDynamoProvider.class.getSimpleName(), "Sending Key: " + values.getAsString("key") + " to " + TiredOfClasses.ports[index]);
                LoadCarrier lc = new LoadCarrier(map, "insert", TiredOfClasses.myPosition, uuid);
                lc.temp = values.getAsString("key");
                new Sockets(index, lc, Sockets.pingOff);
                //threadIntegerHashMap.put(Thread.currentThread().getId(), 4);
            }
            else {
                Log.d(SimpleDynamoProvider.class.getSimpleName(), "Sending Key: " + values.getAsString("key") + " to " + TiredOfClasses.ports[index]);
                LoadCarrier lc = new LoadCarrier(map, "insert", TiredOfClasses.myPosition, uuid);
                lc.temp = values.getAsString("key");
                new Sockets(index, lc, Sockets.pingOn);
                //threadIntegerHashMap.put(Thread.currentThread().getId(), 4);
            }

            //Put the thread to sleep here.
            Log.d(SimpleDynamoProvider.class.getSimpleName(), "Sleep loop: " + uuid);
            synchronized (lock) {
                try {
                    while (!threadUUIDHashMap.get(uuid).getLock()) {
                        lock.wait();
                        Log.d(SimpleDynamoProvider.class.getSimpleName(), "Status: " + threadUUIDHashMap.get(uuid));
                    }
                } catch (InterruptedException ie) {
                    Log.d("Debug:", "Insert Interrupted");
                }
            }
            threadUUIDHashMap.remove(uuid);
            Log.d(SimpleDynamoProvider.class.getSimpleName(), "It works");
        }

        else{

            Log.d(SimpleDynamoProvider.class.getSimpleName(), "Inserting key: " + values.getAsString(DatabaseHelper.KEY) + " in " + TiredOfClasses.myID);

            /*String statement = "insert into " + DatabaseHelper.TABLE +
                    " (" + DatabaseHelper.KEY + ", " +
                           DatabaseHelper.VALUE + ", " +
                           DatabaseHelper.OWNER + ")  values('" +
                    values.getAsString(DatabaseHelper.KEY) + "', '" +
                    values.getAsString(DatabaseHelper.VALUE) + "', '" +
                    values.getAsString(DatabaseHelper.OWNER) + "') on duplicate key update " +
                    DatabaseHelper.VALUE + " = '" + values.getAsString(DatabaseHelper.VALUE) + "', " +
                    DatabaseHelper.OWNER + " = '" + values.getAsString(DatabaseHelper.OWNER)+ "';";
            */
            String statement = "replace into " + DatabaseHelper.TABLE + " (" +
                    DatabaseHelper.KEY + ", " +
                    DatabaseHelper.VALUE + ", " +
                    DatabaseHelper.OWNER + ")  values('" +
                    values.getAsString(DatabaseHelper.KEY) + "', '" +
                    values.getAsString(DatabaseHelper.VALUE) + "', '" +
                    values.getAsString(DatabaseHelper.OWNER) + "');";

            SQLiteDatabase sqd = databaseHelper.getDB();
            synchronized (insert) {
                sqd.execSQL(statement);
            }
            Log.d(SimpleDynamoProvider.class.getSimpleName(), "Insert function called");
        }

		return null;
	}

	@Override
	public boolean onCreate() {

        new Thread(new ServerTask()).start();

        TiredOfClasses.context = getContext();
        TiredOfClasses.simpleDynamoProvider = this;

        TiredOfClasses.myPosition = TiredOfClasses.myPosition();
        TiredOfClasses.myID = TiredOfClasses.ports[TiredOfClasses.myPosition];

        TiredOfClasses.uri = TiredOfClasses.buildUri("content",
                "edu.buffalo.cse.cse486586.simpledynamo.provider");

        //databaseHelper = new DatabaseHelper(getContext());
        threadUUIDHashMap = Collections.synchronizedMap(new HashMap<String, LockFace>());
        mergedThreadCursorMap = Collections.synchronizedMap(new HashMap<String, MatrixCursor>());

        query = new Object();
        insert = new Object();
        delete = new Object();

        //new Thread(new OnStartQueryTask()).start();

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

        SQLiteDatabase sqLiteDatabase;
        Cursor cursor;
        String uuid = String.valueOf(UUID.randomUUID());

        while(!threadUUIDHashMap.get(SimpleDynamoActivity.uuid).getLock())
            continue;

        sqLiteDatabase = databaseHelper.getDB();
        SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
        sqLiteQueryBuilder.setTables(DatabaseHelper.TABLE);

        Log.d(SimpleDynamoProvider.class.getSimpleName(), "Reference" + this.toString());
        Log.d(SimpleDynamoProvider.class.getSimpleName(), "Query called for: " + selection);

        if(selection.equals("\"*\"")){
            //selection = "select * from " + databaseHelper.TABLE + ";";
            //selection = DatabaseHelper.OWNER + " = '" + TiredOfClasses.ports[TiredOfClasses.myPosition] + "'";
            String[] localProjection = {DatabaseHelper.KEY, DatabaseHelper.VALUE};

            synchronized (query) {
                cursor = sqLiteQueryBuilder.query(sqLiteDatabase, localProjection, null, null, null, null, null);
            }
            Log.d(SimpleDynamoProvider.class.getSimpleName(), "* Cursor Size: " + cursor.getCount());

            LockFace lock = new LockFace();
            threadUUIDHashMap.put(uuid, lock);
            LoadCarrier lc = new LoadCarrier("*query", TiredOfClasses.myPosition, 6, uuid);
            lc.queryMap = new TiredOfClasses().queryCursorResolver(cursor);

            new Sockets((TiredOfClasses.myPosition + 1) % 5, lc, Sockets.pingOn);

            synchronized (lock) {
                try {
                    while (!threadUUIDHashMap.get(uuid).getLock())
                        lock.wait();
                } catch (InterruptedException ie) {
                    Log.d("Debug:", "Interrupted");
                }
            }

            cursor = mergedThreadCursorMap.get(uuid);
            mergedThreadCursorMap.remove(uuid);
            threadUUIDHashMap.remove(uuid);
            Log.d(SimpleDynamoProvider.class.getSimpleName(), "Size of * Cursor: " + cursor.getCount());

            return cursor;
        }

        else if(selection.equals("\"@\"")){
            //selection = DatabaseHelper.OWNER + " = '" + TiredOfClasses.ports[TiredOfClasses.myPosition] + "'";
            String[] localProjection = {DatabaseHelper.KEY, DatabaseHelper.VALUE};
            synchronized (query) {
                cursor = sqLiteQueryBuilder.query(sqLiteDatabase, localProjection, null, null, null, null, null);
            }
            Log.d(SimpleDynamoProvider.class.getSimpleName(), "Returning cursor for @: " + cursor.getCount());
            return cursor;
        }

        else if(TiredOfClasses.isInPorts(selection)){
            selection = DatabaseHelper.OWNER + " = '" + selection + "'";
            synchronized (query) {
                cursor = sqLiteQueryBuilder.query(sqLiteDatabase, null, selection, null, null, null, null);
            }
            return cursor;
        }

        else{
            if(selectionArgs == null){
                int index = TiredOfClasses.coordinatorForKey(selection);
                String owner = TiredOfClasses.ports[index];
                String self = TiredOfClasses.ports[TiredOfClasses.myPosition];

                LockFace lock = new LockFace();
                threadUUIDHashMap.put(uuid, lock);

                if(owner.equals(self))
                    new Sockets(index,
                            new LoadCarrier("query", TiredOfClasses.myPosition, selection, uuid), Sockets.pingOff);
                else
                    new Sockets(index,
                            new LoadCarrier("query", TiredOfClasses.myPosition, selection, uuid), Sockets.pingOn);

                synchronized (lock) {
                    try {
                        while (!threadUUIDHashMap.get(uuid).getLock())
                            lock.wait();
                    } catch (InterruptedException ie) {
                        Log.d("Debug:", "Interrupted");
                    }
                }

                cursor = mergedThreadCursorMap.get(uuid);
                if(cursor != null) {
                    cursor.moveToFirst();
                    Log.d(SimpleDynamoProvider.class.getSimpleName(), "Returning Cursor: " + cursor.getString(0) + " " + cursor.getString(1) + "for" + selection);
                }
                else
                    Log.d(SimpleDynamoProvider.class.getSimpleName(), "Null Cursor for " + selection);

                mergedThreadCursorMap.remove(uuid);
                threadUUIDHashMap.remove(uuid);
                return cursor;
            }

            else if(selectionArgs[0].equals("NULL")) {
                String[] localProjection = {DatabaseHelper.KEY, DatabaseHelper.VALUE};
                selection = DatabaseHelper.KEY + " = '" + selection + "'";
                synchronized (query) {
                    cursor = sqLiteQueryBuilder.query(sqLiteDatabase, localProjection, selection, null, null, null, null);
                }

                if(cursor.getCount() == 0)
                    Log.e(SimpleDynamoProvider.class.getSimpleName(), "Empty Cursor");
                else
                    Log.d(SimpleDynamoProvider.class.getSimpleName(), "Non-Empty Cursor");

                return cursor;
            }
            else{
                int index = TiredOfClasses.coordinatorForKey(selection);
                String owner = TiredOfClasses.ports[index];
                String self = TiredOfClasses.ports[TiredOfClasses.myPosition];

                LockFace lock = new LockFace();
                threadUUIDHashMap.put(uuid, lock);
                if(owner.equals(self))
                    new Sockets(index,
                            new LoadCarrier("query", TiredOfClasses.myPosition, selection, uuid), Sockets.pingOff);
                else
                    new Sockets(index,
                            new LoadCarrier("query", TiredOfClasses.myPosition, selection, uuid), Sockets.pingOn);

                synchronized (lock) {
                    try {
                        while (!threadUUIDHashMap.get(uuid).getLock())
                            lock.wait();
                    } catch (InterruptedException ie) {
                        Log.d("Debug:", "Interrupted");
                    }
                }

                cursor = mergedThreadCursorMap.get(uuid);
                if(cursor != null) {
                    cursor.moveToFirst();
                    Log.d(SimpleDynamoProvider.class.getSimpleName(), "Returning Cursor: " + cursor.getString(0) + " " + cursor.getString(1) + "for" + selection);
                }
                else
                    Log.d(SimpleDynamoProvider.class.getSimpleName(), "Null Cursor for" + selection);

                mergedThreadCursorMap.remove(uuid);
                threadUUIDHashMap.remove(uuid);
                return cursor;
            }
        }
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
}