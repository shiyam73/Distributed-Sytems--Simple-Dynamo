package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.Executor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Handler;
import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	private static String node_id;
	String TAG= "Shiyam";
	private static String[] nodes = {"11108","11112","11116"};
	private static String[] nodes_i = {"5554","5556","5558"};
	private static final String AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";
	private static final String BASE_PATH = myDatabase.TABLE_NAME;
	public static final Uri CONTENT_URI = Uri.parse("content://"+ AUTHORITY + "/" + BASE_PATH);
	public static int version = 0;
	private Handler uiHandle= new Handler();
	static int stopped = 0;
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
    
		node_id = get_portStr();
		
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
       
        /*if(stopped > 0)
        {
        	Log.e("Asking for recovery","Stopped");
        	for(int i=0;i<nodes.length;i++)
        	{
        		String node = nodes[i];
        		if(!nodes_i[i].equals(node_id))
        		{
        			Message msg = new Message("join", node_id,stopped);
        			Message socket = new Message(node);
        			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, socket);
        		}
        	}
        }*/
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public String get_portStr() {
    	TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
    	String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
    	return portStr;
    }
	
	private void insertValues(String j) {
		int version1 = ++version;
		for(int i=0 ; i<20 ; i++) {
			try {
				//obj.insertRequest(Integer.toString(i),j+Integer.toString(i),version);
				ContentValues _cv = new ContentValues();
				_cv.put(myDatabase.KEY_FIELD, Integer.toString(i));
				_cv.put(myDatabase.VALUE_FIELD, Integer.toString(i));
				_cv.put(myDatabase.FIELD_VERSION, version1);
				getContentResolver().insert(CONTENT_URI, _cv);
				Thread.sleep(1200);
			} catch (InterruptedException e) {
				Log.e(TAG, "Put Sleep fail");
			}
		}
    }

	public void Put1(View view) {
		insertValues("Put1");
		
	}
	
	public void LDump(View view) {
    	Cursor resultCursor = getContentResolver().query(CONTENT_URI, null, "@", null,null);
    	updateTextView("Partition Empty" , true);
    	if(resultCursor != null)
    	{
    		if (resultCursor.getCount()!=0 && resultCursor.moveToFirst()) {
    			while (!resultCursor.isAfterLast()) {
    				int keyIndex = resultCursor.getColumnIndex("key");
    				int valueIndex = resultCursor.getColumnIndex("value");
    				String returnKey = resultCursor.getString(keyIndex);
    				String returnValue = resultCursor.getString(valueIndex);
    				updateTextView(returnKey+" "+returnValue,false);
    				resultCursor.moveToNext();
    			}
    		}
    	}
    	else {
    		updateTextView("Partition Empty",false);
    	}
    }
	
	 public void updateTextView(String message, final boolean set) {
	    	final String msg= message;
	    	uiHandle.post(new Runnable() {
	    		public void run() {
	    			TextView textView = (TextView)findViewById(R.id.textView1);
	    			textView.setMovementMethod(new ScrollingMovementMethod());
	    	    	if (!set)
	    	    		textView.append(msg+"\n");
	    	    	else
	    	    		textView.setText(" ");
	       		}
	    	});
	    }
	
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
            	//receivedNodes = new Neighbours(node_id, node_id, node_id);
                Log.e("Client", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("Client", "ClientTask socket IOException");
            	//receivedNodes = new Neighbours(node_id, node_id, node_id);
            }

            return null;
        }

		public void executeOnExecutor(Executor serialExecutor, Message m,
				String string) {
			// TODO Auto-generated method stub
			
		}
    }
    
    /**********************************/

}
