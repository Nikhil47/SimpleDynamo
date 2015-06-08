package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by Nikhil on 4/23/15.
 */
public class DatabaseHelper extends SQLiteOpenHelper {
    public static final String TABLE = "dynamo";

    public static final String KEY = "key";
    public static final String VALUE = "value";
    public static final String OWNER = "owner";

    private static final String DBNAME = "dynamo.db";
    private static final int DATABASE_VERSION = 2;

    private static final String CREATE_DATABASE =
            "create table " + TABLE + " (" +
            KEY + " text primary key ASC, " +
            VALUE + " text not null, " +
            OWNER + " text not null);";

    private static final String RECOVERY_COUNT =
            "create table recovery (count smallint default 0);";

    public DatabaseHelper(Context ctx)
    {
        super(ctx, DBNAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(CREATE_DATABASE);
        db.execSQL(RECOVERY_COUNT);
        db.execSQL("insert into recovery (count) values (0)");
        return;
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        //db.execSQL("drop table if exists " + TABLE);
        return;
    }

    public void stop(SQLiteDatabase db){
        db.execSQL("drop table if exists " + TABLE);
    }

    public SQLiteDatabase getDB()
    {
        return this.getWritableDatabase();
    }
}