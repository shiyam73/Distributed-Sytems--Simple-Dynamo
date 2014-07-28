package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class myDatabase  extends SQLiteOpenHelper{

	public static final String DB_NAME = "Shiyam_dynamo.db";
	public static int DB_VERSION= 1;
	public static final String TABLE_NAME= "SimpleDht";
	public static final String KEY_FIELD= "key";
	public static final String VALUE_FIELD = "value";
	public static final String FIELD_VERSION = "versionId";
	public static final String CREATE_TABLE= "CREATE TABLE " + TABLE_NAME + "(" + KEY_FIELD + " TEXT PRIMARY KEY, " + VALUE_FIELD + " TEXT ,"+ FIELD_VERSION + " INTEGER);";
	public static final String TAG= "Shiyam";
	
	public myDatabase(Context context) {
		super(context, DB_NAME, null, DB_VERSION);
		System.out.println("Inside mydatabase constructor");
		//context.deleteDatabase(DB_NAME);
	}

	@Override
	public void onCreate(SQLiteDatabase arg0) {
		arg0.execSQL(CREATE_TABLE);
		System.out.println("table created");
		Log.v(TAG, "Table created");
		}

	@Override
	public void onUpgrade(SQLiteDatabase arg0, int arg1, int arg2) {
		Log.v(TAG, "OnUpgrade");
		 arg0.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
		 onCreate(arg0);
	}

	/*public static void insertPair(String key, String value, ContentResolver c) {
		ContentValues keyValuespair= new ContentValues();
		keyValuespair.put(myDatabase.KEY_FIELD, key);
		keyValuespair.put(myDatabase.VALUE_FIELD, value);
		c.insert(SimpleDhtProvider.CONTENT_URI, keyValuespair);
	}*/
	
}
