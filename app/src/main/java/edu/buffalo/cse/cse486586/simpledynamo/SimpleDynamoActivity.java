package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.sqlite.SQLiteDatabase;
import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.util.UUID;

public class SimpleDynamoActivity extends Activity {

    public static String uuid = String.valueOf(UUID.randomUUID());

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

        SimpleDynamoProvider.databaseHelper = new DatabaseHelper(getBaseContext());
        TiredOfClasses.database = SimpleDynamoProvider.databaseHelper.getDB();

        int count = new TiredOfClasses().getRecoveryCount();
        LockFace lock = new LockFace();

        if(count == 1) {
            lock.setLock();
            SimpleDynamoProvider.threadUUIDHashMap.put(uuid, lock);
            new Thread(new OnStartQueryTask()).start();
        }
        else{
            lock.openLock();
            SimpleDynamoProvider.threadUUIDHashMap.put(uuid, lock);
            new TiredOfClasses().setRecoveryCount();
        }
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();

        DatabaseHelper dbh = new DatabaseHelper(getBaseContext());
        dbh.stop(dbh.getDB());

	    Log.v("Test", "onStop()");
	}

}
