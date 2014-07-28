package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoActivity.ClientTask;
import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.SharedPreferences.Editor;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Handler;
import android.preference.PreferenceManager;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private myDatabase database;
	private SQLiteDatabase sqlDb;
	private SQLiteDatabase sqlDb1;
	private SQLiteDatabase sqlDb2;
	private static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	private SortedMap<String, String> chord;
	private static final String BASE_PATH = myDatabase.TABLE_NAME;
	//public static final Uri CONTENT_URI = Uri.parse("content://"+ AUTHORITY + "/" + BASE_PATH);
	public static final Uri CONTENT_URI = Uri.parse("content://"+ AUTHORITY);
	public static final String TAG= "Shiyam";
	private static String node_id = null;
	private static Neighbours receivedNodes;
	private ArrayList<String> forJoin;
	//private static String[] nodes = {"5554","5556","5558"};
	private static String[] nodes = {"5554","5556","5558","5560","5562"};
	public static Map<String , String> chord_map_port;
	public static int versionCommon = 0;
	private ExecutorService serverExecutor= Executors.newSingleThreadExecutor();
	public static ArrayBlockingQueue<Integer> blockInsert = new ArrayBlockingQueue<Integer>(1);
	public static ArrayBlockingQueue<Integer> blockRecovery = new ArrayBlockingQueue<Integer>(1);
	private static final String DUMMY_BASE_PATH = "dummy";
	public static final Uri DUP_CONTENT_URI = Uri.parse("content://"+ AUTHORITY + "/" + DUMMY_BASE_PATH);
	private boolean query_reply_flag = false;
	private boolean global_reply_flag = false;
	private Map<String,String> cursorMap = null;
	private Map<String,String> dumpCursorMap = new HashMap<String,String>();
	private ArrayList<Map<String,String>> resultantDumpCursorList = new ArrayList<Map<String,String>>(); 
	private Cursor replyCursor;
	private Cursor dumpCursor;
	private String deleteGlobalId = null;
	private static int failure = 0;
	SharedPreferences sharedPreferences;
	private static int queryCount = 0;
	private static int queryLock = 0;
	 private boolean isLocked = false;
	
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		sqlDb = database.getWritableDatabase();
    	
    	//int a = sqlDb.delete(myDatabase.TABLE_NAME,selection ,selectionArgs);
    	System.out.println("Inside delete()");
    	int a = sqlDb.delete(myDatabase.TABLE_NAME,myDatabase.KEY_FIELD+"=?",selectionArgs);
    	
    	if(selection != null)
    	{
    		if(selection.equals("@"))
    		{
    			a = sqlDb.delete(myDatabase.TABLE_NAME,null,null);
    		}
    		else
    		{
    			a = sqlDb.delete(myDatabase.TABLE_NAME,myDatabase.KEY_FIELD+"=?",new String[]{selection});
    		}
    	}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
	//	System.out.println("Inside insert()");
		
		try {
			lock();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		int tempVer = 0;
    	long rowId=0;
    	String hashKey,hashNode,hashPred;
    	//sqlDb = database.getWritableDatabase();    	
    	
    	if(!values.containsKey("versionId"))
    	{
    		tempVer = versionCommon +2;
    		values.put(myDatabase.FIELD_VERSION, tempVer);
    	}
		else 
		{
			tempVer = (int)values.get("versionId");
		}
    	
    	String key = (String) values.get(myDatabase.KEY_FIELD);
		String value = (String)values.get(myDatabase.VALUE_FIELD);
		String insertionNode = getInsertionNode(key);
		Cursor resultCursor = queryInsert(CONTENT_URI, null, key, null, "for_ins");
		//System.out.println(resultCursor.getCount());
		//String tempUri = uri.toString()+"/" + BASE_PATH;
		//uri = Uri.parse(tempUri);
		//System.out.println(uri+" "+CONTENT_URI);
	//	System.out.println("KEY "+key);
		if(uri.equals(CONTENT_URI)) 
		{
			//System.out.println("Inside if "+key);
			if(insertionNode.equals(node_id)) 
			{	
				if(checkingForCursor(resultCursor,tempVer))
				{
					System.out.println("inserting in to present node "+key);
					rowId= sqlDb.replace(myDatabase.TABLE_NAME, myDatabase.VALUE_FIELD, values);
					//Log.d(TAG, "New version inserted");
					int index = getIndex(node_id);
					replication(node_id,key,value,tempVer,index);
					
				}
			} 
			else
			{
				//System.out.println("forwarding the key "+key);
				Message msg = new Message(node_id,"insert_replication",key,value,tempVer);
				Message soc = new Message(chord_map_port.get(insertionNode));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, soc);
				
					blockInsert.clear();
				//	ins_suc = block_ins.poll(1200, TimeUnit.MILLISECONDS) != null;
				
					if(!insertionBlock()) 
					{
					Log.e(TAG, "Timeout "+insertionNode);
						int index = getIndex(insertionNode);
						replication(node_id,key,value,tempVer,index);
					}
					blockInsert.clear();
			}
		} 
		else
		{
		//	System.out.println("Inside else "+key);
			//System.out.println(uri+" "+CONTENT_URI);
			if(checkingForCursor(resultCursor,tempVer)) 
			{
				System.out.println("replica insertion "+key);
				rowId= sqlDb.replace(myDatabase.TABLE_NAME, myDatabase.VALUE_FIELD, values);
				//Log.d(TAG, "New replica version inserted");
			}
		}
    	
		if (rowId > 0) {
			Uri newUri = ContentUris.withAppendedId(CONTENT_URI, rowId);
			getContext().getContentResolver().notifyChange(newUri, null);
			versionCommon = Math.max(versionCommon, tempVer);
			return newUri;
		} else {
			//Log.e(TAG, "Insert to db failed");
		}
		unlock();
		return null;
	}
	
	public void replication(String nodeId,String key,String value,int tempVer,int index)
	{
		Message msg1 = null;
		Message msg2 = null;
		Message soc1 = null;
		Message soc2 = null;
		
		if(index != -1)
		{
			if(index == forJoin.size()-1)
			{
				msg1 = new Message(nodeId,"replication",key,value,tempVer);
				msg2 = new Message(nodeId,"replication",key,value,tempVer);
				soc1 = new Message(chord_map_port.get(forJoin.get(0)));
				soc2 = new Message(chord_map_port.get(forJoin.get(1)));
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, soc1);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, soc2);
			}
			else
			{
				msg1 = new Message(nodeId,"replication",key,value,tempVer);
				msg2 = new Message(nodeId,"replication",key,value,tempVer);
				
				if((index+1) == forJoin.size()-1)
				{
					soc1 = new Message(chord_map_port.get(forJoin.get(index+1)));
					soc2 = new Message(chord_map_port.get(forJoin.get(0)));
				}
				/*else if((index+2) == forJoin.size()-1)
				{
					soc1 = new Message(chord_map_port.get(forJoin.get(index+1)));
					soc2 = new Message(chord_map_port.get(forJoin.get(index+2)));
				}*/
				else
				{
					soc1 = new Message(chord_map_port.get(forJoin.get(index+1)));
					soc2 = new Message(chord_map_port.get(forJoin.get(index+2)));
				}
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1, soc1);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg2, soc2);
			}
		//	System.out.println("Replication() "+forJoin.get(index)+" "+soc1.socket+" "+soc2.socket);
		}
	}
	
	public boolean insertionBlock()
	{
		boolean success = false;
		try {
			success = blockInsert.poll(2000, TimeUnit.MILLISECONDS) != null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return success;
	}
	
	public boolean recoveryBlock()
	{
		boolean success = false;
		try {
			success = blockRecovery.poll(2000, TimeUnit.MILLISECONDS) != null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return success;
	}

	public int getIndex(String node)
	{
		for(int i=0;i<forJoin.size();i++)
		{
			if(node.equals(forJoin.get(i)))
				return i;
		}
		return -1;
	}
	public boolean checkingForCursor(Cursor cursor,int version)
	{
		//System.out.println(cursor.getCount() == 0 || (cursor.moveToFirst() && version > cursor.getInt(cursor.getColumnIndex(myDatabase.FIELD_VERSION))));
		if(cursor == null)
			return true;
		else
			return (cursor.getCount() == 0 || (cursor.moveToFirst() && version > cursor.getInt(cursor.getColumnIndex(myDatabase.FIELD_VERSION))));
	}
	
	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		database = new myDatabase(getContext());
		sqlDb = database.getWritableDatabase();
		
		sharedPreferences = PreferenceManager.getDefaultSharedPreferences(getContext());
		chord = new TreeMap<String, String>();
		forJoin = new ArrayList<String>();
		chord_map_port = new HashMap<String, String>();
		ExecutorService serverExecutor= Executors.newSingleThreadExecutor();
		serverExecutor.execute(new Server());
		failure = sharedPreferences.getInt("failure",0);
		if(failure == 0)
		{
			failure++;
			System.out.println("Inside oncreate else");
			Editor editor = sharedPreferences.edit();
			editor.putInt("failure",failure);
			editor.commit();
		}
		else
		{
			//System.out.println("Recovered from a stop");
			//SimpleDynamoActivity.stopped = 1;
			for(int i=0;i<nodes.length;i++)
			{
				String node = nodes[i];
				if(!node.equals(node_id))
				{
					Message msg = new Message("join", node_id,SimpleDynamoActivity.stopped);
					Message socket = new Message((Integer.parseInt(node)*2)+"");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, socket);
				}
			}
		}
		
		node_id = getPortString();
		//Log.v("SDHTP OnCreate()",node_id);
		constructChord();
		System.out.println("SDHTP OnCreate()"+node_id+" "+failure);
		receivedNodes = new Neighbours(node_id, node_id, node_id);
		return true;
	}

	private String getPortString()
    {
    	TelephonyManager telephone = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
    	String portStr = telephone.getLine1Number().substring(telephone.getLine1Number().length() - 4);	
    	final String port = String.valueOf((Integer.parseInt(portStr)));
    	return port;
    }
	
	public void constructChord()
	{
		//System.out.println("Inside construct chord()");
		for(int i=0;i<nodes.length;i++)
		{
			String hash;
	    	try 
	    	{
					hash = genHash(nodes[i]);
					chord.put(hash,nodes[i]);
			} 
	    	catch (NoSuchAlgorithmException e) 
	    	{
					// TODO Auto-generated catch block
					e.printStackTrace();
			}
		}
		
		for(Entry<String,String> entry : chord.entrySet())
		{
			String node = entry.getValue();
			chord_map_port.put(node,(Integer.parseInt(node)*2)+"");
			forJoin.add(node);
		}
		
		/*for(int i=0;i<forJoin.size();i++)
			System.out.print(forJoin.get(i)+" -> ");
		System.out.println();*/
	}
	
	public String getInsertionNode(String key)
	{
		String result = null;
		try 
		{
			String hashKey = genHash(key);
			for(int i=0;i<forJoin.size();i++)
			{
				String Node = forJoin.get(i);
				String hashNode = genHash(forJoin.get(i));
				String hashPred = genHash(getPrev(Node));
				if(hashKey.compareTo(hashNode) <= 0 && hashKey.compareTo(hashPred) > 0)
				{
					result = Node;
					break;
				} 
				else if(Node.equals(forJoin.get(0)) && (hashKey.compareTo(hashNode) <= 0 || hashKey.compareTo(hashPred) > 0))
				{
						result = Node;
						break;
				}
			}
		} 
		catch (NoSuchAlgorithmException e) 
		{
			Log.e(TAG, "Hash Fail");
		}
		return result;
	}
	
	public String getPrev(String node)
	{
		String predecessor = null;
		int index = getIndex(node);
		//System.out.println("Inside getprev() "+node+" "+index);
		if(index == 0)
		{
			predecessor = forJoin.get(forJoin.size()-1);
		}
		else
		{
			predecessor = forJoin.get(index-1);
		}
		//System.out.println("Predecessor "+predecessor);
		return predecessor;
	}
	
	public String getSucc(String node)
	{
		String successor = null;
		int index = getIndex(node);
		//System.out.println("Inside getprev() "+node+" "+index);
		if(index == forJoin.size()-1)
		{
			successor = forJoin.get(0);
		}
		else
		{
			successor = forJoin.get(index+1);
		}
		//System.out.println("Predecessor "+predecessor);
		return successor;
	}
	
	public void recoveryKeyValue(Map<String, String[]> recoveryMap) {
		Log.d(TAG, "Recovery method");
		for(Map.Entry<String, String[]> entry: recoveryMap.entrySet()) {
			ContentValues _cv = new ContentValues();
			String k = entry.getKey();
			String v[]= entry.getValue();
			_cv.put(myDatabase.KEY_FIELD, k);
			_cv.put(myDatabase.VALUE_FIELD, v[0]);
			_cv.put(myDatabase.FIELD_VERSION, Integer.parseInt(v[1]));
			insert(DUP_CONTENT_URI,_cv);
    	}
	}
	
	public Cursor queryInsert(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder)
	{
		//System.out.println("Inside query Insert()");
		//sqlDb1 = database.getWritableDatabase();  
		Cursor cursor = null;
			cursor = sqlDb.query(myDatabase.TABLE_NAME, // a. table
					null, // b. column names
					myDatabase.KEY_FIELD+"=?", // c. selections 
	                new String[]{selection},
					null, // e. group by
					null, // f. having
					null, // g. order by
					null); // h. limit
			return cursor;
	}
	
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		//System.out.println("Query Lock "+queryLock+" for query "+selection);
		
		try {
			lock();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Cursor cursor = null;
    	if(selection != null)
    	{
    		if(selection.equals("@"))
    		{
    			
    			cursor = sqlDb.query(myDatabase.TABLE_NAME, // a. table
    					new String[]{myDatabase.KEY_FIELD,myDatabase.VALUE_FIELD}, // b. column names
    					null, // c. selections 
    					null, // d. selections args
    					null, // e. group by
    					null, // f. having
    					null, // g. order by
    					null); // h. limit
    			System.out.println(selection+" "+cursor.getCount());
    			return cursor;
    		}
    		else if(selection.equals("*"))
    		{
    			cursor = sqlDb.query(myDatabase.TABLE_NAME, // a. table
    					new String[]{myDatabase.KEY_FIELD,myDatabase.VALUE_FIELD}, // b. column names
    					null, // c. selections 
    					null, // d. selections args
    					null, // e. group by
    					null, // f. having
    					null, // g. order by
    					null); // h. limit
    			dumpCursorMap = cursorToHash(cursor);
    			cursor =  dumpQuery(dumpCursorMap);
    		}
    		else
    		{
    			
    			cursor =  singleQuery(selection,projection);
				if(cursor != null && cursor.getCount() > 0)
				{
					System.out.println("result for "+selection);
					displayCursor(cursor);
					return cursor;
				}
				while(cursor == null)
				{
					//new Task().run();
					System.out.println("Inside while loop");
					cursor =  singleQuery(selection,projection);
				}
    		}
    	}
    	else
    	{
    		//System.out.println("Inside query :: else");
    		//cursor= sqlDb2.rawQuery("select * from "+myDatabase.TABLE_NAME, null);
    		cursor = sqlDb2.query(myDatabase.TABLE_NAME, // a. table
					null, // b. column names
					null, // c. selections 
					null, // d. selections args
					null, // e. group by
					null, // f. having
					null, // g. order by
					null); // h. limit
    	}
		//System.out.println("Cursor count "+cursor.getCount());

		System.out.println("result for "+selection);
		displayCursor(cursor);
		unlock();
		return cursor;
	}

	public synchronized void lock()
			  throws InterruptedException{
			    while(isLocked){
			     // wait();
			    }
			    isLocked = true;
			  }
	public synchronized void unlock(){
	    isLocked = false;
	    notify();
	  }
	
	public Map<String,String> cursorToHash(Cursor cursor)
    {
    	Map<String,String> temp = new HashMap<String,String>();
    	if (cursor.moveToFirst()) 
		{
			while (!cursor.isAfterLast()) 
			{
				int keyIndex = cursor.getColumnIndex("key");
				int valueIndex = cursor.getColumnIndex("value");
				String returnKey = cursor.getString(keyIndex);
				String returnValue = cursor.getString(valueIndex);
				temp.put(returnKey, returnValue);
				cursor.moveToNext();
			}
		}
    	return temp;
    }
	
	public Map<String,String[]> cursorToHashRecovery(Cursor cursor)
	{
		Map<String,String[]> temp = new HashMap<String,String[]>();
		if(cursor!= null && cursor.moveToFirst()) 
		{
			while(!cursor.isAfterLast())
			{
				int keyIndex = cursor.getColumnIndex("key");
    	        int valueIndex = cursor.getColumnIndex("value");
    	    	String returnKey = cursor.getString(keyIndex);
    	        String returnValue = cursor.getString(valueIndex);
    	        int version = cursor.getInt(cursor.getColumnIndex(myDatabase.FIELD_VERSION));
    	        String arr[] = {returnValue,Integer.toString(version)};
    	        temp.put(returnKey, arr);
    	        cursor.moveToNext();
			}
			return temp;
		}
		return null;
	}
	
	public Cursor dumpQuery(Map<String,String> temp)
    {
    	Cursor cursor = null;
    	Message msg = new Message("global_dump",node_id,temp);
    	resultantDumpCursorList.add(temp);
    	for(String node : nodes)
    	{
    		if(!node.equals(node_id))
    		{
    			Message socket = new Message(chord_map_port.get(node));
    			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,socket);
    		}
    	}
		waitForDumpReply();
		dumpCursor = mapListToCursor(resultantDumpCursorList);
		if(dumpCursor != null)
			cursor = dumpCursor;
		return cursor;
    }
	
	 public void waitForDumpReply()
	 {
		long a = System.currentTimeMillis();  
		 while(true)
		 {  
		      long  b =  System.currentTimeMillis();  
		        if(b-a == 10000) break;   // Elapsed 10s  
		 } 
	 }
	
	 public Cursor mapListToCursor(ArrayList<Map<String,String>> temp)
	 {
		 MatrixCursor mc = new MatrixCursor(new String[]{myDatabase.KEY_FIELD,myDatabase.VALUE_FIELD});
		 Cursor c = null;
		 for(int i=0;i<temp.size();i++)	
		 {
			 for(Entry<String,String> entry : temp.get(i).entrySet())
			 {
				 mc.newRow().add(entry.getKey()).add(entry.getValue());
			 }
		 }
		 c = mc;
		 return c;
	 }
	 
	  public Cursor singleQuery(String key,String[] projection)
	    {
	    	//System.out.println("KEY "+key);
	    	String hashKey,hashNode,hashPred;
	    	Cursor cursor = null;
	    		
	    	String queryNode = getInsertionNode(key);
	    	System.out.println(queryNode+" "+node_id+" "+key);
	    		//if(queryNode.equals(node_id))
	    		//{
	    			cursor = sqlDb.query(myDatabase.TABLE_NAME, // a. table
	    					new String[]{myDatabase.KEY_FIELD,myDatabase.VALUE_FIELD}, // b. column names
	    					myDatabase.KEY_FIELD+"=?", // c. selections 
	    	                new String[]{key},
	    					null, // e. group by
	    					null, // f. having
	    					null, // g. order by
	    					null); // h. limit
	    			
	    			if((cursor == null) || (cursor.getCount() == 0))
	    			{
	    				System.out.println("Inside singleQuery :: 3");
		    			Message msg = new Message("query_for",node_id,key);
		    			Message socket = new Message(chord_map_port.get(queryNode));
		    			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,socket);
		    			waitForReply(key);
		    			if(replyCursor != null)
		    			{
		    				displayCursor(replyCursor);
		    				return replyCursor;
		    				//cursor = replyCursor;
		    			}
	    			}
	    		//}
	    	//	else
	    		//{
	    			/*System.out.println("Inside singleQuery :: 3");
	    			Message msg = new Message("query_for",node_id,key);
	    			Message socket = new Message(chord_map_port.get(queryNode));
	    			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg,socket);
	    			waitForReply();
	    			if(replyCursor != null)
	    				cursor = replyCursor;*/
	    		//}
	    			if((cursor == null) || (cursor.getCount() == 0))
	    				cursor = null;
	    	return cursor;
	    }
	
	  public void displayCursor(Cursor resultCursor)
	  {
		  
		  if(resultCursor != null)
	    	{
			  System.out.println("Cursor Size "+resultCursor.getCount());
	    		if (resultCursor.getCount()!=0 && resultCursor.moveToFirst()) {
	    			while (!resultCursor.isAfterLast()) {
	    				int keyIndex = resultCursor.getColumnIndex("key");
	    				int valueIndex = resultCursor.getColumnIndex("value");
	    				String returnKey = resultCursor.getString(keyIndex);
	    				String returnValue = resultCursor.getString(valueIndex);
	    				System.out.println("REPLY "+returnKey+" "+returnValue);
	    				resultCursor.moveToNext();
	    			}
	    		}
	    	}
	  }
	  
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public void waitForReply(String key)
    {
		System.out.println("Query loop "+key);
		System.out.println("Inside query loop");
		while(true)
    	{
			
    		if(queryCount != 0)
    		{
    			queryCount = 0;
    			break;
    		}
    	}
    }
	
	/******************* SERVER CODE ******************/
    class Receiver implements Runnable {

    	static final String TAG = "Shiyam Receiver";
    	Socket sock= null;
    	Message msg;

    	Receiver (Message s) 
    	{
    		this.msg= s;
    	}

    	public void run() 
    	{
    		//Log.i(TAG, "recvd msg: "+ msg.type+" received in "+node_id+" from "+msg.node_id);
    		System.out.println("recvd msg: "+ msg.type+" received in "+node_id+" from "+msg.node_id);
    		if (msg.type.equals("join")) 
    		{
    			
    	//		System.out.println("Received Join Request");
    			blockRecovery.clear();
    			if(msg.stop > 0)
    			{
    			//	System.out.println("Inside Join Recovery Condition 1 from "+msg.node_id);
    				String succ = getSucc(node_id);
    				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Message("request_recovery",node_id), new Message(chord_map_port.get(succ)));
    				if(!recoveryBlock())
    				{
    		//			System.out.println("Inside Join Recovery Condition 2 from "+msg.node_id+" "+succ+" failed so sending to its "+getSucc(succ));
    					succ = getSucc(succ);
    					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Message("request_recovery",node_id), new Message(chord_map_port.get(succ)));
    				}
    			}
    			blockRecovery.clear();
    		}
    		else if(msg.type.equals("request_recovery"))
    		{
    			blockRecovery.offer(2);
    			Cursor c= getContext().getContentResolver().query(CONTENT_URI, null, null, null, null);
    			Map<String,String[]> send = cursorToHashRecovery(c);
    		//	System.out.println("Sending request for recovery "+msg.node_id);
    			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Message("recovery",node_id,send,"dummy"), new Message(chord_map_port.get(msg.node_id)));
    		}
    		else if(msg.type.equals("recovery"))
    		{
    		//	System.out.println("Received recovery from "+msg.node_id);
    			recoveryKeyValue(msg.recoveryMap);
    		}
    		else if(msg.type.equals("insert_replication")) 
    		{
    			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, new Message("ack",msg.key,msg.value,msg.version), new Message(chord_map_port.get(msg.node_id)));
    			ContentValues values = new ContentValues();
    			values.put(myDatabase.KEY_FIELD, msg.key);
    			values.put(myDatabase.VALUE_FIELD, msg.value);
    			values.put(myDatabase.FIELD_VERSION, msg.version);
    			getContext().getContentResolver().insert(CONTENT_URI, values);
    		} 
    		else if(msg.type.equals("ack"))
    		{
    			blockInsert.offer(msg.version);
    		} 
    		else if(msg.type.equals("query_for"))
    		{
    			Map<String,String> cursorMap =  new HashMap<String, String>();
    			System.out.println("Query from "+msg.node_id+" "+msg.selection);
    			Cursor cursor = getContext().getContentResolver().query(CONTENT_URI, null, msg.selection, null,null);
    			
    			if(cursor != null)
    			{
    				cursorMap = cursorToHash(cursor);
    				Message reply = new Message("query_reply",cursorMap,node_id);
    				Message socket = new Message(chord_map_port.get(msg.node_id));
    				System.out.println("Cursor not null "+msg.selection);
    				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, reply, socket);
    				
    			}
    		}
    		else if(msg.type.equals("query_reply"))
    		{
    			cursorMap = msg.map;
    			MatrixCursor mc = new MatrixCursor(new String[]{myDatabase.KEY_FIELD,myDatabase.VALUE_FIELD});
    			System.out.println("Query reply from "+msg.sender);
    			for(Entry<String,String> entry : cursorMap.entrySet())
    			{
    				System.out.println("query reply "+entry.getKey()+" "+entry.getValue());
    				mc.newRow().add(entry.getKey()).add(entry.getValue());
    			}
    			replyCursor = mc;
    			queryCount = 1;
    			cursorMap.clear();
    			//mc.newRow().add(selection).add(val);
    		}
    		else if(msg.type.equals("replication")) 
    		{
    			ContentValues values = new ContentValues();
    			values.put(myDatabase.KEY_FIELD, msg.key);
    			values.put(myDatabase.VALUE_FIELD, msg.value);
    			values.put(myDatabase.FIELD_VERSION, msg.version);
    			getContext().getContentResolver().insert(DUP_CONTENT_URI, values);
    		}
    		else if(msg.type.equals("global_dump"))
    		{
    	//		System.out.println("Inside global dump else if");
    			/*if(msg.node_id.equals(node_id))
    			{
    		//		System.out.println("Inside global dump else if ( if");
    				global_reply_flag = true;
        			dumpCursorMap = msg.map;
        			MatrixCursor mc = new MatrixCursor(new String[]{myDatabase.KEY_FIELD,myDatabase.VALUE_FIELD});
        			for(Entry<String,String> entry : dumpCursorMap.entrySet())
        			{
        				mc.newRow().add(entry.getKey()).add(entry.getValue());
        			}
        			dumpCursor = mc;
        			dumpCursorMap.clear();
        			//cursorMap.clear();
    			}
    			else if(msg.type.equals("global_dump"))
    			{*/
    	//			System.out.println("Inside receiver global dump else MSG ID-> "+msg.node_id+" receiver_-> "+node_id);
    				Cursor cursor = getContext().getContentResolver().query(CONTENT_URI, null, null, null,null);
    			//	dumpCursorMap = msg.map;
    				if(cursor != null)
    				{
    					if (cursor.moveToFirst()) 
    					{
    						while (!cursor.isAfterLast()) 
    						{
    							int keyIndex = cursor.getColumnIndex("key");
    							int valueIndex = cursor.getColumnIndex("value");
    							String returnKey = cursor.getString(keyIndex);
    							String returnValue = cursor.getString(valueIndex);
    							dumpCursorMap.put(returnKey, returnValue);
    							cursor.moveToNext();
    						}
    					}
    				}
    				/*for(Entry<String,String> entry : dumpCursorMap.entrySet())
    				{
    					System.out.println(entry.getKey()+" "+entry.getValue());
    				}*/
    				Message msg1 = new Message("global_dump_reply",msg.node_id,dumpCursorMap);
    				Message socket = new Message(chord_map_port.get(msg.node_id));
    				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg1,socket);
    		//	}
    			
    			
    			//mc.newRow().add(selection).add(val);
    		}
    		else if(msg.type.equals("global_dump_reply"))
			{
				resultantDumpCursorList.add(msg.map);
			}
    	}
    }
	
	 class Server implements Runnable {

	    	static final String TAG = "Shiyam Server";
	    	static final int recvPort= 10000;
	    	//ExecutorService receiverExecutor= Executors.newFixedThreadPool();

	    	public void run() {
	    		ObjectInputStream input =null;
	    		ServerSocket serverSocket= null;
	    		Socket server= null;

	    		try {
	    			serverSocket= new ServerSocket(recvPort);
	    		} catch (IOException e) {
	    			Log.e(TAG, ""+e.getMessage());
	    		}

	    		while(true) {
	    			try {
	    				server= serverSocket.accept();
	    				input =new ObjectInputStream(server.getInputStream());
	    				Message obj;
	    				try {
	    					obj = (Message) input.readObject();
	    					Executors.newSingleThreadExecutor().execute(new Receiver(obj)); //replace where to send this object
	    				} catch (ClassNotFoundException e) {
	    					Log.e(TAG, e.getMessage());
	    				}
	    			} 

	    			catch (IOException e) {
	    				Log.e(TAG, ""+e.getMessage());
	    				e.printStackTrace();
	    			}
	    			finally {
	    				if (input!= null)
	    					try {
	    						input.close();
	    					} catch (IOException e) {
	    						Log.e(TAG, ""+e.getMessage());
	    					}
	    				if(server!=null)
	    					try {
	    						server.close();
	    					} catch (IOException e) {
	    						Log.e(TAG, ""+e.getMessage());
	    					}	
	    			}
	    		}
	    	}    
	    
	    }
	    
	/**************************************************/
	 
	 /************* CLIENT *************/
	    
	    public class ClientTask extends AsyncTask<Message, Void, Void> 
	    {

	        @Override
	        protected Void doInBackground(Message... msgs) {
	            try {
	                String remotePort = msgs[1].socket;

	                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
	                        Integer.parseInt(remotePort));
	                
	                Message msgToSend = msgs[0];
	                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
	                out.writeObject(msgToSend);
	                out.flush();
	                out.close();
	                
	                
	                 // TODO: Fill in your client code that sends out a message.
	                 
	                
	                socket.close();
	            } catch (UnknownHostException e) {
	            	receivedNodes = new Neighbours(node_id, node_id, node_id);
	                //Log.e("Client", "ClientTask UnknownHostException");
	            } catch (IOException e) {
	                //Log.e("Client", "ClientTask socket IOException");
	            	receivedNodes = new Neighbours(node_id, node_id, node_id);
	            }

	            return null;
	        }

			public void executeOnExecutor(Executor serialExecutor, Message m,
					String string) {
				// TODO Auto-generated method stub
				
			}
	    }
	    
	    /**********************************/
	    
	    class Task implements Runnable 
	    {
	    	@Override
	    	public void run() 
	    	{
	    		try 
	    		{
	    			Thread.sleep(300);
	    		} catch (InterruptedException e) 
	    		{
	    			e.printStackTrace();
	    		}

	    	}
	    }

		   

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
