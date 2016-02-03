import java.io.*;
import org.json.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.Block;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.bson.*;
import java.util.*;
import java.util.logging.*;
import java.sql.Timestamp;

public class ServerSpoof {
    private static final int MILLI_SEC = 1000;
    private static final int DEFAULT_DELAY = 1;
    private static HashMap<String, Integer> queryWords = new HashMap<String, Integer>();
    private static HashMap<String, Integer> users = new HashMap<String, Integer>();
    private static long totalMessages = 0;
    private static long totalUsers = 0;
    private static long lastNumMessages = 0;
    // 0 = public
    // 1 = protected
    // 2 = private
    private static long[] statusMessages = new long[3];
    // 0 = all
    // 1 = subscriber
    // 2 = another user
    // 3 = self
    private static long[] recepientMessages = new long[4];
    private static JSONObject monitorObject;
    private static String serverLogName;
    private static MongoCollection<Document> collection;
    private static MongoCollection<Document> monCollection;
    private static Timestamp timestamp;
    private static PrintWriter out;
    private static ObjectId lastObjectId;
    private static HashMap<String, List<Document>> queryObjects;

    private static void readQueryWordFile(String filename) {
        try {
            Scanner scanner = new Scanner(new File(filename));
            
            while (scanner.hasNext()) {
                queryWords.put(scanner.nextLine(), 1);
            }
            scanner.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initStats() {
        for (int i = 0; i < 3; i++)
            statusMessages[i] = 0;
        for (int i = 0; i < 4; i++)
            recepientMessages[i] = 0;
    }

    private static void collectStats() {
        totalUsers = 0;
        users.clear();
        initStats();
        lastNumMessages = totalMessages;
        totalMessages = collection.count();
        FindIterable<Document> result = collection.find();

        result.forEach(new Block<Document>() {
            @Override
            public void apply(final Document d) {
                String user = (String)d.get("user");
                String status = (String)d.get("status");
                String recepient = (String)d.get("recepient");

                if (!users.containsKey(user))
                    totalUsers++;
                
                if (status.equals("public"))
                    statusMessages[0]++;
                else if (status.equals("protected"))
                    statusMessages[1]++;
                else
                    statusMessages[2]++;

                if (recepient.equals("all"))
                    recepientMessages[0]++;
                else if (recepient.equals("subscribers"))
                    recepientMessages[1]++;
                else if (recepient.equals("self"))
                    recepientMessages[3]++;
                else 
                    recepientMessages[2]++;
            }
        });
    }

    private static void updateMonRecord() {
        final Document document = new Document("recordType", "monitor totals");
        //Document filter = new Document("recordType", new Document("$ne", null));
        FindIterable<Document> result = monCollection.find(document);
        final ArrayList<Long> msgTotals = new ArrayList<Long>();
        final ArrayList<Long> userTotals = new ArrayList<Long>();
        final ArrayList<Long> newMsgTotals = new ArrayList<Long>();

        if (monCollection.count(document) > 0) {
            result.forEach(new Block<Document>() {
                @Override
                public void apply(final Document d) {
                    ArrayList<Long> array = (ArrayList<Long>)d.get("msgTotals");
                    array.add(totalMessages);
                    document.append("msgTotals", array);

                    array = (ArrayList<Long>)d.get("userTotals");
                    array.add(totalUsers);
                    document.append("userTotals", array);

                    array = (ArrayList<Long>)d.get("newMsgTotals");
                    array.add(totalMessages - lastNumMessages);
                    document.append("newMsgTotals", array);
                }
            });
        }
        else {
            msgTotals.add(totalMessages);
            userTotals.add(totalUsers);
            newMsgTotals.add(totalMessages - lastNumMessages);

            document.put("msgTotals", msgTotals);
            document.put("userTotals", userTotals);
            document.put("newMsgTotals", newMsgTotals);
        }

        if (monCollection.findOneAndReplace(new Document("recordType", "monitor totals"),
            document) == null) {
            monCollection.insertOne(document);
        }
    }

    private static void insertCheckpoint() {
        try {
            FindIterable<Document> result = collection.find()
                .sort(new Document("_id", -1)).limit(1);

            result.forEach(new Block<Document>() {
                @Override
                public void apply(final Document d) {
                    lastObjectId = (ObjectId)d.get("_id");
                }
            });

            Document document = new Document();

            if (lastObjectId != null) {
                document.append("lastId", lastObjectId);
                if (monCollection.findOneAndReplace(
                    new Document("lastId", new Document("$ne", null)),
                    document) == null) {
                    monCollection.insertOne(document);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void insertMonObject() {
        try {
            Document document = new Document();
            document.append("time", monitorObject.get("time"));
            document.append("messages", monitorObject.get("messages"));
            document.append("users", monitorObject.get("users"));
            document.append("new", monitorObject.get("new"));

            JSONArray array = (JSONArray)monitorObject.get("statusStats");
            ArrayList<Document> statusArray = new ArrayList<Document>();
            statusArray.add(new Document().append("public", statusMessages[0]));
            statusArray.add(new Document().append("protected", statusMessages[1]));
            statusArray.add(new Document().append("private", statusMessages[2]));
            document.append("statusStats", statusArray);

            array = (JSONArray)monitorObject.get("statusStats");
            ArrayList<Document> recepientArray = new ArrayList<Document>();
            recepientArray.add(new Document().append("all", recepientMessages[0]));
            recepientArray.add(new Document().append("subscribers", recepientMessages[1]));
            recepientArray.add(new Document().append("userId", recepientMessages[2]));
            recepientArray.add(new Document().append("self", recepientMessages[3]));
            document.append("recepientStats", recepientArray);

            monCollection.insertOne(document);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void printToServerLog() {
        try {
            out.println(monitorObject.toString(2));
            out.flush();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void prepareJSONObject() {
        try {
            monitorObject = new JSONObject();
            monitorObject.put("time", timestamp.toString());
            monitorObject.put("messages", totalMessages);
            monitorObject.put("users", totalUsers);
            monitorObject.put("new", totalMessages - lastNumMessages);

            JSONArray array = new JSONArray();
            array.put(new JSONObject().put("public", statusMessages[0]));
            array.put(new JSONObject().put("protected", statusMessages[1]));
            array.put(new JSONObject().put("private", statusMessages[2]));
            monitorObject.put("statusStats", array);

            array = new JSONArray();
            array.put(new JSONObject().put("all", recepientMessages[0]));
            array.put(new JSONObject().put("subscribers", recepientMessages[1]));
            array.put(new JSONObject().put("userId", recepientMessages[2]));
            array.put(new JSONObject().put("self", recepientMessages[3]));
            monitorObject.put("recepientStats", array);

            System.out.println("----------------------------------------");
            System.out.println(monitorObject.toString(2));

            insertMonObject();
            printToServerLog();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void tryCreatingCollections(MongoDatabase db, String collName, String monName) {
        try {
            db.createCollection(collName);
        }
        catch (Exception e) {
        }

        try {
            db.createCollection(monName);
        }
        catch (Exception e) {
        }
    }

    private static void conductWordSearch() {
        FindIterable<Document> monResult = monCollection.
            find(new Document("lastId", new Document("$ne", null)));

        monResult.forEach(new Block<Document>() {
            @Override
            public void apply(final Document d) {
                lastObjectId = (ObjectId)d.get("lastId");
            }
        });

        Document sortFilter = new Document("_id", -1);

        FindIterable<Document> result = collection
            .find(new Document("_id", new Document("$gt", lastObjectId)))
            .sort(sortFilter);
        queryObjects = new HashMap<String, List<Document>>();

        result.forEach(new Block<Document>() {
            @Override
            public void apply(final Document d) {
                String text = (String)d.get("text");
                
                for (String s : text.split("\\s+")) {
                    if (queryWords.containsKey(s)) {
                        List<Document> temp;
                        if (queryObjects.containsKey(s)) {
                            temp = queryObjects.get(s);
                            temp.add(d);
                            queryObjects.put(s, temp);
                        }
                        else {
                            temp = new ArrayList<Document>();
                            temp.add(d);
                            queryObjects.put(s, temp);
                        }
                    }
                } 
            }
        });

        System.out.println("---Start Query word matches with new documents---");

        for (Map.Entry<String, List<Document>> entry : queryObjects.entrySet()) {
            System.out.println("Keyword: " + entry.getKey());
            for (Document d : entry.getValue()) {
                System.out.println(d.toString());
            }
        }

        System.out.println("---End Query word matches with new documents---");
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Need configuration file name as input parameter");
            System.exit(1);
        }

        try {
            File configFile = new File(args[0]);
            JSONTokener tokenizer = new JSONTokener(new FileReader(configFile));
            tokenizer.skipTo('{');
            JSONObject config = new JSONObject(tokenizer);
            Logger logger = Logger.getLogger("org.mongodb.driver");
            logger.setLevel(Level.OFF);

            String server = config.get("mongo").toString();
            server = (server == null || server.equals("")) ? "localhost" : server;

            String collName = config.get("collection").toString();
            String dbName = config.get("database").toString();
            String monitor = config.get("monitor").toString();
            serverLogName = config.get("serverLog").toString();
            out = new PrintWriter(new BufferedWriter(new FileWriter(serverLogName, true)));

            MongoClient client = new MongoClient(server);
            MongoDatabase db = client.getDatabase(dbName);
            tryCreatingCollections(db, collName, monitor);
            collection = db.getCollection(collName);

            int numDocuments = (int)collection.count();
            int delayAmount = Integer.parseInt(config.get("delay").toString());
            delayAmount = (delayAmount == 0) ? 3 * MILLI_SEC * DEFAULT_DELAY : 3 * MILLI_SEC * delayAmount;
            int cycleCount = 0;

            readQueryWordFile(config.get("wordFilter").toString());
            monCollection = db.getCollection(monitor);
            monCollection.deleteMany(new Document());

            java.util.Date time = new Date();
            timestamp = new Timestamp(time.getTime());
            System.out.println("Current Timestamp: " + timestamp);
            System.out.println("Server: " + server);
            System.out.println("Database: " + dbName);
            System.out.println("Collection: " + collName);
            System.out.println("Number of documents in collection: " + numDocuments);

            while (true) {
                Thread.sleep(delayAmount);
                collectStats();
                updateMonRecord();
                timestamp = new Timestamp(new Date().getTime());
                prepareJSONObject();

                if (cycleCount > 0) {
                    conductWordSearch();
                }
                insertCheckpoint();
                cycleCount++;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
