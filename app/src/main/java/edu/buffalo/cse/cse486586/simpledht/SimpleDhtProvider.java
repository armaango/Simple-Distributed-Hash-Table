package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.List;



public class SimpleDhtProvider extends ContentProvider {
    //static final int[] portList = {11108, 11112, 11116, 11120, 11124};
    
    static final String FIRST_NODE="5554";
    String myPort;
    boolean firstNode;
    boolean singleNode;
    static final int SERVER_PORT = 10000;
    private List<String> nodesJoinedList = new ArrayList<String>();
    Context context;
    boolean infinteLoop=true;
    boolean deleteFlag=false;
    String queryResultinClientTask=null;
    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        //implement this
        if(singleNode==false) {
            deleteFlag = true;
            if (!myPort.equals(FIRST_NODE)) {
                boolean tempFlag = false;
                String keyToDelete = selection;
                Log.v(TAG, "key to be deleted is : " + keyToDelete);
                context = getContext();
                try {
                    tempFlag = context.deleteFile(keyToDelete);
                    if (tempFlag == true) {
                        deleteFlag = true;
                    }
                } catch (Exception e) {
                    Log.e(TAG, "delete: Error occurred in delete");
                }
            } else if (myPort.equals(FIRST_NODE)) {
                boolean tempFlag = false;
                String keyToDelete = FIRST_NODE + "-" + selection;
                Log.v(TAG, "key to be deleted is : " + keyToDelete);
                context = getContext();
                try {
                    tempFlag = context.deleteFile(keyToDelete);
                    if (tempFlag == true) {
                        deleteFlag = true;
                        Log.d(TAG, "delete: delete flag set to tru for 5554");
                    }
                } catch (Exception e) {
                    Log.e(TAG, "delete: Error occurred in delete");
                }
            }
        }
       else if(singleNode==true){

            String keyToDelete = selection;
            Log.v(TAG, "key to be deleted is : " + keyToDelete);
            context = getContext();
            try {
               context.deleteFile(keyToDelete);
            } catch (Exception e) {
                Log.e(TAG, "delete: Error occurred in delete");
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

        Log.d(TAG, "insert: Insert query received from grader");
        if (singleNode==true)
        {
    // Case of only one node in whole chord so store all data locally
            String file_name=values.get("key").toString();
            String data = values.get("value").toString();
            context=getContext();
            try {
                FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
                fos.write(data.getBytes());
                fos.close();

            }
            catch(IOException e)
            {
                Log.v("Failed to insert",values.toString());
            }
        }
        else if (singleNode==false)
        {
            //case where there are multiple nodes but request is received on 5554 so calculate where to insert and then send message to corresponding node
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort,"InsertMessage", (String) values.get("key"), (String) values.get("value"));
        }
        return uri;
    }

    @Override
    public boolean onCreate() {

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort=portStr;
        try {ServerSocket serverSocket = new ServerSocket(SERVER_PORT);//creating the server on 10000
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);//sending to accept connections on the serversocket
        } catch (IOException e) {
            Log.v(TAG, "Problem occurred while creating server socket");
            return false;}
        if(!myPort.equals(FIRST_NODE)){

            Log.v(TAG, "Case where node is not 5554");
            Log.v(TAG, "My port number is :" + myPort);
            firstNode=true;
            //Execute commands to send join message to 5554
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,myPort,"JoinMessage" );
        }else {
            Log.v(TAG, "Case where first node started is 5554");
            nodesJoinedList.add(myPort);
            firstNode=true;
            singleNode = true;

        }
        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor matrixCursor=new MatrixCursor(new String[]{"key","value"});

        String queryParameter=selection;

        if(singleNode==true)
        {
            Log.v(TAG,"Query case of single node");
            //Case where only 1 node so @ and * mean same thing
            if (queryParameter.equals("*") || queryParameter.equals("@"))
            {
                //case of one node so dump all local stored values
                context=getContext();
                String fileList[]=context.fileList();
                for(int i=0;i<fileList.length;i++)
                {
                    String file_name=fileList[i];
                    try {
                        FileInputStream fis = context.openFileInput(file_name);
                        InputStreamReader inputStreamReader = new InputStreamReader(fis);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        StringBuilder sb = new StringBuilder();
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            sb.append(line);
                        }
                        Log.e("data read",sb.toString());
                        String[]row={file_name,sb.toString()};

                        matrixCursor.addRow(row);

                        fis.close();
                        //return matrixCursor;
                    }
                    catch(IOException e)
                    {
                        Log.v("Failed to query", selection);
                    }

                }
                int countReturned=matrixCursor.getCount();
                if(countReturned>0)
                {
                    Log.v(TAG, "query for one node only for * or @: search successful ");
                }

            }
            else
            {
                String file_name=selection;
                context=getContext();
        try {
            FileInputStream fis = context.openFileInput(file_name);
            InputStreamReader inputStreamReader = new InputStreamReader(fis);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                sb.append(line);
            }
            Log.e("data read",sb.toString());
            String[]row={selection,sb.toString()};

            matrixCursor.addRow(row);

            fis.close();
        }
        catch(IOException e)
        {
            Log.v("Failed to query", selection);
        }
            }
        }// single node case ends here

        else if(singleNode==false) {
            Log.v(TAG, "Query case of chord");
            //case where there are many nodes in the chord
            if (queryParameter.equals("*")) {
                //think of a strategy here to query from all dumps

                if (deleteFlag==false)
                {

                    Log.v(TAG, "Query for * for multiple nodes");
                if (myPort.equals(FIRST_NODE)) {
                    context = getContext();
                    String fileList[] = context.fileList();

                    for (int i = 0; i < fileList.length; i++) {
                        String tempFileName = fileList[i];
                        String splitFileNames[] = tempFileName.split("-");

                        String file_name = fileList[i];
                        try {
                            FileInputStream fis = context.openFileInput(file_name);
                            InputStreamReader inputStreamReader = new InputStreamReader(fis);
                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                            StringBuilder sb = new StringBuilder();
                            String line;
                            while ((line = bufferedReader.readLine()) != null) {
                                sb.append(line);
                            }
                            Log.e("data read", sb.toString());
                            String[] row = {splitFileNames[1], sb.toString()};

                            matrixCursor.addRow(row);

                            fis.close();
                            //return matrixCursor;
                        } catch (IOException e) {
                            Log.v("Failed to query", selection);
                        }
                    }
                } else if (!myPort.equals(FIRST_NODE)) {
                    //case of * query where request is not received on 5554 and has to be sent to server task
                    //send the request to 5554
                    infinteLoop = true;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, "QueryMessage", queryParameter);
                    //here request of * is sent to client task who will send it to 5554

                    while (infinteLoop) {
                        //do nothing keep waiting
                    }
                    String dataToprocess = queryResultinClientTask;
                    //Now process data to matrix cursor object

                    String[] splitlevelone = dataToprocess.split("#");
                    if (splitlevelone[0] != null) {
                        Log.v(TAG, "after splitlevel one data is :" + splitlevelone[0]);
                    }
                    if (splitlevelone[1] != null && splitlevelone[1].length() > 0 && splitlevelone[1].charAt(splitlevelone[1].length() - 1) == '?') {
                        splitlevelone[1] = splitlevelone[1].substring(0, splitlevelone[1].length() - 1);
                    }
                    Log.v(TAG, "after splitlevel ONE TRIMMING one data is :" + splitlevelone[1]);

                    String[] splitleveltwo = splitlevelone[1].split("\\?");

                    Log.v(TAG, "after splitlevel two first data is :" + splitleveltwo[0]);

                    for (int i = 0; i < splitleveltwo.length; i++) {
                        String[] splitlevelthree = splitleveltwo[i].split("-");
                        String[] row = {splitlevelthree[0], splitlevelthree[1]};

                        matrixCursor.addRow(row);

                    }
                }
            }
        }
            else if(queryParameter.equals("@")) {
                Log.v(TAG, "Inside query of @  ");
                if (!myPort.equals(FIRST_NODE)) {
                    Log.v(TAG, "query:inside query of @ for any node other than first node ");
                    context = getContext();
                    String fileList[] = context.fileList();
                    for (int i = 0; i < fileList.length; i++) {
                        String file_name = fileList[i];
                        try {
                            FileInputStream fis = context.openFileInput(file_name);
                            InputStreamReader inputStreamReader = new InputStreamReader(fis);
                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                            StringBuilder sb = new StringBuilder();
                            String line;
                            while ((line = bufferedReader.readLine()) != null) {
                                sb.append(line);
                            }
                            Log.v("data read", sb.toString());
                            String[] row = {file_name, sb.toString()};

                            matrixCursor.addRow(row);

                            fis.close();
                            //return matrixCursor;
                        } catch (IOException e) {
                            Log.v("Failed to query", selection);
                        }

                    }
                    int countReturned = matrixCursor.getCount();
                    if (countReturned > 0) {
                        Log.v(TAG, "query for @: search successful ");
                    }
                }
                else if(myPort.equals(FIRST_NODE)){
                    Log.v(TAG, "query: Inside @ query for first node ");
                    context = getContext();
                    String fileList[] = context.fileList();

                    for (int i = 0; i < fileList.length; i++) {
                        String tempFileName=fileList[i];
                        String splitFileNames []=tempFileName.split("-");
                        if (splitFileNames[0].equals(FIRST_NODE))
                        {
                            String file_name = fileList[i];
                            try {
                                FileInputStream fis = context.openFileInput(file_name);
                                InputStreamReader inputStreamReader = new InputStreamReader(fis);
                                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                                StringBuilder sb = new StringBuilder();
                                String line;
                                while ((line = bufferedReader.readLine()) != null) {
                                    sb.append(line);
                                }
                                Log.e("data read", sb.toString());
                                String[] row = {splitFileNames[1], sb.toString()};

                                matrixCursor.addRow(row);

                                fis.close();
                                //return matrixCursor;
                            } catch (IOException e) {
                                Log.v("Failed to query", selection);
                            }
                        }


                    }
                    int countReturned = matrixCursor.getCount();
                    if (countReturned > 0) {
                        Log.v(TAG, "query for @: search successful ");
                    }
                }
            }

            else {
                //case where any single key value is queried for
                //step 1, check if I have the data present locally

                Log.v(TAG, "query: In query for single value ");
                String file_name = queryParameter;

                context = getContext();
                String fileList[] = context.fileList();
                boolean hasLocal = false;
                if (myPort.equals(FIRST_NODE)){
                    Log.v(TAG, "query: In query for single value for first node ");
                    for (int i = 0; i < fileList.length; i++) {
                        String temp []=fileList[i].split("-");
                        Log.v(TAG, "query: temp value equals : "+ temp[1]);
                        if (temp[1].equals(file_name)) {
                            hasLocal = true;
                            file_name=fileList[i];
                            break;
                        }
                    }
            }
                else
                {
                    Log.v(TAG, "query: In query for single value for any other node");
                    for(int i=0;i<fileList.length;i++)
                    {
                        if(fileList[i].equals(file_name))
                        {
                            hasLocal=true;
                            break;
                        }
                    }
                }
                if(hasLocal==true) {
                    try {
                        FileInputStream fis = context.openFileInput(file_name);
                        InputStreamReader inputStreamReader = new InputStreamReader(fis);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        StringBuilder sb = new StringBuilder();
                        String line;
                        while ((line = bufferedReader.readLine()) != null) {
                            sb.append(line);
                        }
                        Log.e("data read", sb.toString());
                        String[] row = {selection, sb.toString()};

                        matrixCursor.addRow(row);

                        fis.close();
                    } catch (IOException e) {
                        Log.v("Failed to query", selection);
                    }
                }

                //2 cases possible, query received on 5554 or any other node- handle on client side
                //check how to get data returned from the called function
                else if(hasLocal==false) {//case where locally key value is not present
                    infinteLoop=true;
                    new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort, "QueryMessage", queryParameter);
                    while(infinteLoop)
                    {
                        //do nothing keep waiting
                    }
                    String dataToprocess=queryResultinClientTask;
                    //now process data here to convert to matrix cursor rows

                    String [] splitlevelone=dataToprocess.split("#");
                    if(splitlevelone[0]!=null){
                        Log.v(TAG,"after splitlevel one data is :"+splitlevelone[0]);}
                    String [] splitleveltwo=splitlevelone[1].split("-");
                     String[] row = {splitleveltwo[0], splitleveltwo[1]};

                        matrixCursor.addRow(row);
                }

            }
        }
        return matrixCursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.v(TAG,"Inside Doinbackground for Server task of port number : "+myPort);
            ServerSocket serverSocket=sockets[0];

            try
            {
                while(true)
                {
                    Socket server=serverSocket.accept();
                    DataInputStream dataInputStream = new DataInputStream(server.getInputStream());
                    String messageFromClient = dataInputStream.readUTF();
                    String messageToClient=null;

                    if(messageFromClient==null)
                    {
                        Log.v(TAG,"No data received from client");
                    }
                    else
                    {
                        Log.v(TAG,"Message on : " +myPort+" received and message is : "+messageFromClient);
                        String [] tokenisedData = messageFromClient.split("&");
                        String operationToPerform=tokenisedData[0];
                        if (operationToPerform.equals("Join"))
                        {
                            String portToJoin=tokenisedData[1];
                            nodesJoinedList.add(portToJoin);

                            singleNode=false;
                            messageToClient="Node has joined the list";
                            Log.v(TAG, "doInBackground: Server : new node added ");

                        }
                        else if(operationToPerform.equals("Insert"))
                        {
                            Log.v(TAG, "Server task do in background Insert message received at port: "+myPort);
                            if(myPort.equals(FIRST_NODE))
                            {
                                //case where Insert message comes to 5554 from some other node- Here either 5554 will store it in its own local storage or send to concerned avd where to store
                                //Step 1 - Compare where to store

                                balanceChord();//using this for iterative method
                                String destinationPort=null;
                                String keytoInsert=tokenisedData[1];
                                String valuetoInsert=tokenisedData[2];

                                //Now check where the key should reside
                                try {
                                    boolean foundDestinationFlag=false;
                                    for (int i=0;i<nodesJoinedList.size();i++)
                                    {
                                        String porttoCheck=nodesJoinedList.get(i);
                                        int comparisonVal=genHash(keytoInsert).compareTo(genHash(porttoCheck));
                                        if(comparisonVal<=0)
                                        {
                                            destinationPort=porttoCheck;
                                            foundDestinationFlag=true;
                                            break;
                                        }
                                    }
                                    if (foundDestinationFlag==false) {
                                        destinationPort = nodesJoinedList.get(0);
                                    }
                                    Log.v(TAG,"destination port for key : "+keytoInsert+" is :" + destinationPort);
                                }
                                catch (NoSuchAlgorithmException e)
                                {
                                    Log.e(TAG, "doInBackground: Error exception occurred in genhash comparison" );
                                }

                                //Case 1 where 5554 stores it in its own storage
                                if (destinationPort.equals(FIRST_NODE))
                                {
                                    String tempFileName=destinationPort+"-"+keytoInsert;

                                    //String file_name=keytoInsert;
                                    String file_name=tempFileName;
                                    String data = valuetoInsert;
                                    context=getContext();
                                    try {
                                        FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
                                        fos.write(data.getBytes());
                                        fos.close();

                                    }
                                    catch(IOException e)
                                    {
                                        Log.v(TAG,"Failed to insert");
                                    }
                                }
                                else
                                {
                                    //case where it needs to send data to another port


                                    //Implementing workaround
                                    String tempFileName=destinationPort+"-"+keytoInsert;

                                    //String file_name=keytoInsert;
                                    String file_name=tempFileName;
                                    String data = valuetoInsert;
                                    context=getContext();
                                    try {
                                        FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
                                        fos.write(data.getBytes());
                                        fos.close();
                                        Log.v(TAG, "doInBackground: in workaround file name is "+ file_name);

                                    }
                                    catch(IOException e)
                                    {
                                        Log.v(TAG,"Failed to insert");
                                    }


                                    //workaround insert ends here//

                                    try {
                                        Socket specialsocketinserver = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destinationPort)*2);
                                        DataOutputStream specialdataOutputStreaminserver = new DataOutputStream(specialsocketinserver.getOutputStream());
                                        //messageFromClient+="\n";
                                        specialdataOutputStreaminserver.writeUTF(messageFromClient);
                                        Log.v(TAG, " Special Message sent from 5554 to port "+destinationPort+" and message is : " + messageFromClient);

                                    }
                                    catch (UnknownHostException e)
                                    {
                                        Log.e(TAG, "doInBackground: Unknown host exception occurred" );
                                    }
                                    catch (IOException e)
                                    {
                                        Log.e(TAG, "doInBackground: IO exception occurred 1" );
                                        e.printStackTrace();

                                    }
                                }
                            }
                            else
                            {
                                //case where Insert message is sent to some other node by 5554 so store on local storage of that device

                                String file_name=tokenisedData[1];
                                String data = tokenisedData[2];
                                context=getContext();
                                try {
                                    FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
                                    fos.write(data.getBytes());
                                    fos.close();

                                }
                                catch(IOException e)
                                {
                                    Log.v(TAG,"Failed to insert");
                                }

                            }

                        }

                        else if(operationToPerform.equals("Query"))
                        {
                                //case where 5554 has received a query request from another node
                                //2 cases possible, single query received or * query received
                                //handle * case later, yet to make logic
                                //handle single query case first and return data
                            String queryOperator=tokenisedData[1];
                            String queryOriginPort=tokenisedData[2];
                            Log.d(TAG, "doInBackground: query origin port is :"+queryOriginPort);
                            if (queryOperator.equals("*"))
                            {
                                //think of this logic
                                //copy same logic as before for *


                                context = getContext();
                                String fileList[] = context.fileList();
                                StringBuilder dataToReturn=new StringBuilder("QueryOutput#");
                                Log.v(TAG, "doInBackground: area of doubt 1");

                                for (int i = 0; i < fileList.length; i++) {
                                    String tempFileName = fileList[i];
                                    String splitFileNames[] = tempFileName.split("-");

                                        String file_name = fileList[i];
                                        try {
                                            FileInputStream fis = context.openFileInput(file_name);
                                            InputStreamReader inputStreamReader = new InputStreamReader(fis);
                                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                                            StringBuilder sb = new StringBuilder();
                                            String line;
                                            while ((line = bufferedReader.readLine()) != null) {
                                                sb.append(line);
                                            }
                                            Log.e("data read", sb.toString());

                                            //write logic to put all this data to a string

                                            //first create a row
                                            dataToReturn.append(splitFileNames[1]).append("-").append(sb.toString()).append("?");


                                            fis.close();

                                        } catch (IOException e) {
                                            Log.v(TAG,"Failed to query in * of any node other than 5554");
                                        }
                                    }
                                messageToClient=dataToReturn.toString();

                            }
                            else
                            {
                                //single key value is received
                                //calculate where this key is located
                                context = getContext();
                                String fileList[] = context.fileList();
                                StringBuilder dataToReturn=new StringBuilder("QueryOutput#");
                                Log.v(TAG, "doInBackground: area of doubt 2");
                                Log.v(TAG, "doInBackground: area of doubt 2 : queryOperator is "+queryOperator);


                                for (int i = 0; i < fileList.length; i++) {
                                    String tempFileName=fileList[i];
                                    String splitFileNames []=tempFileName.split("-");
                                    Log.d(TAG, "doInBackground: port number is : "+splitFileNames[0]+" and file name is : "+splitFileNames[1]);
                                    if (splitFileNames[1].equals(queryOperator))
                                    {
                                        Log.d(TAG, "doInBackground: Entering if of doubt area");
                                        String file_name = fileList[i];
                                        try {
                                            FileInputStream fis = context.openFileInput(file_name);
                                            InputStreamReader inputStreamReader = new InputStreamReader(fis);
                                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                                            StringBuilder sb = new StringBuilder();
                                            String line;
                                            while ((line = bufferedReader.readLine()) != null) {
                                                sb.append(line);
                                            }
                                            Log.e("data read", sb.toString());
                                            dataToReturn.append(splitFileNames[1]).append("-").append(sb.toString());
                                            //String[] row = {splitFileNames[1], sb.toString()};

                                            //matrixCursor.addRow(row);

                                            fis.close();
                                            //return matrixCursor;
                                        } catch (IOException e) {
                                            Log.v(TAG,"Failed to query");
                                        }
                                    }
                                }
                                messageToClient=dataToReturn.toString();

                            }
                        }
                        try {
                            if(messageToClient!=null) {
                                DataOutputStream dataOutputStream = new DataOutputStream(server.getOutputStream());
                                Log.v(TAG, "doInBackground: Writing data to output stream:  " + messageToClient);
                                dataOutputStream.writeUTF(messageToClient);
                                Log.v(TAG, "doInBackground: data written to client");
                            }
                            //server.getOutputStream().write(messageToClient.getBytes());
                        } catch (IOException e) {
                            Log.e(TAG, " IO Exception Occurred. ");
                        }
                    }

                }
            }
            catch(IOException e)
            {
                Log.e(TAG,"IO Exception occurred in servertask"); e.printStackTrace();
            }







            return null;
        }
    }

    private class ClientTask extends AsyncTask<String, String, Void> {
        @Override
        protected Void doInBackground(String... messages) {
            Log.v(TAG, "Inside Doinbackground for client task of port number : " + myPort);

            String operationToPerform = messages[1];
            StringBuilder dataToSend = new StringBuilder();
            boolean dontsendFlag = false;

            if (operationToPerform.equals("JoinMessage")) {
                Log.v(TAG, "doInBackground:Inside join message creation ");
                dataToSend.append("Join").append("&").append(myPort);
            } else if (operationToPerform.equals("InsertMessage")) {
                Log.d(TAG, "doInBackground: Inside insert at client");
                String portRequestRecvd = messages[0];
                String keytoInsert = messages[2];
                String valuetoInsert = messages[3];
                if (portRequestRecvd.equals(FIRST_NODE)) {
                    Log.d(TAG, "doInBackground:Client task Insert function when request comes to first node ");
                    dontsendFlag = true;
                    balanceChord();
                    //Calculate where to insert message
                    //Step 1, sort the nodes in chord based on hash values

                    String destinationPort = null;

                    //Now check where the key should reside
                    try {
                        boolean foundDestinationFlag=false;
                        for (int i=0;i<nodesJoinedList.size();i++)
                        {
                            String porttoCheck=nodesJoinedList.get(i);
                            int comparisonVal=genHash(keytoInsert).compareTo(genHash(porttoCheck));
                            if(comparisonVal<=0)
                            {
                                destinationPort=porttoCheck;
                                foundDestinationFlag=true;
                                break;
                            }
                        }
                        if (foundDestinationFlag==false) {
                            destinationPort = nodesJoinedList.get(0);
                        }
                        Log.v(TAG, "destination port for key : " + keytoInsert + " is :" + destinationPort);

                    } catch (NoSuchAlgorithmException e) {
                        Log.e(TAG, "doInBackground: Error exception occurred in genhash comparison");
                    }
                    //dataToSend.append("Insert").append("&").append(keytoInsert).append(valuetoInsert);
                    if (destinationPort.equals(FIRST_NODE)) {

                        String tempFileName = destinationPort + "-" + keytoInsert;

                        //String file_name=keytoInsert;
                        String file_name = tempFileName;
                        String data = valuetoInsert;
                        context = getContext();
                        try {
                            FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
                            fos.write(data.getBytes());
                            fos.close();

                        } catch (IOException e) {
                            Log.v(TAG, "Failed to insert");
                        }
                    } else {
                        //Case where destination node is not the same as req recvd port and it is also not 5554
                        //Implementing workaround
                        String tempFileName = destinationPort + "-" + keytoInsert;

                        //String file_name=keytoInsert;
                        String file_name = tempFileName;
                        String data = valuetoInsert;
                        context = getContext();
                        try {
                            FileOutputStream fos = context.openFileOutput(file_name, Context.MODE_PRIVATE);
                            fos.write(data.getBytes());
                            fos.close();
                            Log.v(TAG, "doInBackground: in workaround file name is " + file_name);

                        } catch (IOException e) {
                            Log.v(TAG, "Failed to insert");
                        }


                        //workaround insert ends here//


                        dataToSend.append("Insert").append("&").append(keytoInsert).append("&").append(valuetoInsert);

                        String concatenatedMessage = dataToSend.toString();

                        try {
                            Socket specialsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destinationPort) * 2);
                            DataOutputStream specialdataOutputStream = new DataOutputStream(specialsocket.getOutputStream());
                            specialdataOutputStream.writeUTF(concatenatedMessage);
                            Log.v(TAG, " Special Message sent to " + destinationPort + " and message is : " + concatenatedMessage);

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "doInBackground: Unknown host exception occurred");
                        } catch (IOException e) {
                            Log.e(TAG, "doInBackground: IO exception occurred 2");
                            e.printStackTrace();

                        }


                    }

                } else if (!portRequestRecvd.equals(FIRST_NODE)) {
                    // Case where request is received at node other than 5554, send request to 5554
                    Log.v(TAG, "Inside insert at Client task");
                    dataToSend.append("Insert").append("&").append(keytoInsert).append("&").append(valuetoInsert);
                    dontsendFlag = false;

//                    try {
//                        Socket specialsocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(FIRST_NODE) * 2);
//                        specialsocket.getOutputStream().write(msgToSend.getBytes());
//                    }
//                    catch (UnknownHostException e)
//                    {
//                        Log.e(TAG, "doInBackground: Unknown host exception occurred" );
//                    }
                }

            } else if (operationToPerform.equals("QueryMessage")) {
                String keytoQuery = messages[2];//now this can be both * and a single value
                //handle both cases separately

                if (myPort.equals(FIRST_NODE))//check if flow ever comes here
                {
                    //case where 5554 has the request directly
                    //calculate where the key will lie and send message to that port to return key value pair
                    //if possible, send the port number of origin request to this port so he can reply back directly(this approach to be checked)
                    //also set dont send flag true and see how to handle this case, where will this data be received
                    Log.v(TAG, "Inside empty case: handle me please");

                } else {
                    //case where 5554 doesnot have the request
                    //send request to 5554's server
                    dataToSend.append("Query").append("&").append(keytoQuery).append("&").append(myPort);
                    dontsendFlag = false;


                }
            }
            if (dontsendFlag == false) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(FIRST_NODE) * 2);

                    String concatenatedMessage = dataToSend.toString();
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(concatenatedMessage);
                    Log.v(TAG, "Message sent to 5554 and message is : " + concatenatedMessage);


                    if (operationToPerform.equals("JoinMessage")) {
                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

                        try {
                            String messageFromServer = dataInputStream.readUTF();
                            Log.v(TAG, "doInBackground: Client Task : message received is :" + messageFromServer);
                            if (messageFromServer == null) {
                                Log.v(TAG, "doInBackground: server sent nothing ");


                            } else {
                                Log.v(TAG, "server says that :" + messageFromServer);
                            }

                        } catch (IOException e) {
                            singleNode = true;
                            nodesJoinedList.add(myPort);
                            Log.e(TAG, "IOException : ");
                            e.printStackTrace();
                        }
                    } else if (operationToPerform.equals("QueryMessage")) {
                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

                        try {
                            String messageFromServer = dataInputStream.readUTF();
                            Log.v(TAG, "doInBackground: Client Task : message received is :" + messageFromServer);
                            if (messageFromServer == null) {
                                Log.v(TAG, "doInBackground: server sent nothing ");


                            } else {
                                Log.v(TAG, "server says that :" + messageFromServer);
                                //split the string received
                                queryResultinClientTask = messageFromServer;
                                infinteLoop = false;
                            }

                        } catch (IOException e) {
                            Log.e(TAG, "doInBackground: Stupid IO exception occurred");
                        }

                    }
                    socket.close();
                } catch (IOException e) {
                    singleNode = true;
                    nodesJoinedList.add(myPort);
                    Log.e(TAG, "IOException : ");
                    e.printStackTrace();
                    Log.e(TAG, "IO Exception occurred");
                }
            }
            //Start code for receiving data from server in the form of a string


            return null;
        }
    }
        private void balanceChord()
        {
            //http://stackoverflow.com/questions/6957631/sort-java-collection
            Comparator<String> comparator = new Comparator<String>() {
                public int compare(String c1, String c2) {
                    try {
                        return genHash(c1).compareTo(genHash(c2));
                    }
                    catch(NoSuchAlgorithmException e)
                    {
                    Log.e(TAG,"No such algorithm exception while sorting");
                    }
                return 0;}
            };
            Collections.sort(nodesJoinedList,comparator);



            for (int i=0;i<nodesJoinedList.size();i++)
            {
                Log.v(TAG,"Sorted chord node "+i+" is "+nodesJoinedList.get(i));
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
