import java.io.*;
import org.json.*;
import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;
import java.util.*;
import java.util.logging.*;
import java.sql.Timestamp;

class ClientSpoof {

	private static final int MAX_USERS = 10000;
    private static final int WORD_FILE_SIZE = 6832;
    private static ArrayList<String> words = new ArrayList<String>();
    private static int messageId = 0;

    private static int randNum(int min, int max) {
        return (int) (Math.random() * (max - min + 1)) + min;
    }

    private static String getStatus() {
        int dice = randNum(1, 100);
        String status;
        
        if (dice <= 60)
            status = "public";
        else if (dice <= 80)
            status = "protected";
        else
            status = "private";

        return status;
    }

    private static String getRecepient(String status) {
        int dice = randNum(1, 100);
        String recepient;
        String user = "u" + randNum(1, MAX_USERS);
        
        if (status.equals("public")) {
            if (dice <= 40)
                recepient = "all";
            else if (dice <= 80)
                recepient = "subscribers";
            else if (dice <= 90)
                recepient = user;
            else
                recepient = "self";
        } else if (status.equals("protected")) {
            if (dice <= 70)
                recepient = "subscribers";
            else if (dice <= 85)
                recepient = "self";
            else
                recepient = user;
        } else {
            if (dice <= 90)
                recepient = user;
            else
                recepient = "self";
        }

        return recepient;
    }

    private static void scanWordFile(String wordFile) {
        Scanner scanner;

        try {
            scanner = new Scanner(new File(wordFile));

            while (scanner.hasNextLine()) {
                words.add(scanner.nextLine());
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean shouldRespond() {
        int dice = randNum(1, 100);

        return dice <= 20;
    }

    private static String getText() {
        int numWords = randNum(2, 20);
        String text = "";
        
        for (int i = 0; i < numWords; i++) {
            text += words.get(randNum(0, WORD_FILE_SIZE));
            if (i != numWords - 1)
                text += " ";
        }

        return text;
    }

	private static JSONObject generateJSON(PrintWriter writer, String wordFile) {
        JSONObject object = null;
        String status;

        scanWordFile(wordFile);

        status = getStatus();
        try {
            object = new JSONObject();
            object.put("messageId", messageId++);
            object.put("user", "u" + randNum(1, MAX_USERS));
            object.put("status", status);
            object.put("recepient", getRecepient(status));
            
            if (shouldRespond())
                object.put("in-response", messageId - randNum(0, messageId));
            object.put("text", getText());

            writer.println(object.toString(2));
            //System.out.println(object.toString(2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        writer.flush();

        return object;
    }

	public static void main(String[] args) {

		if (args.length < 1) {
			System.out.println("Please input a config file name");
			return;
		}

		try {
			File configFile = new File(args[0]);
			JSONTokener tokenizer = new JSONTokener(new FileReader(configFile));

			tokenizer.skipTo('{');
			JSONObject config = new JSONObject(tokenizer);

			String server = config.get("mongo").toString();
            if (server.isEmpty() || server == "null") {
                server = "localhost";
            }

            int port;
            if (config.get("port").toString() == "null") {
                port = 27017;
            }
            else {
                port = (Integer)config.get("port");
            }

			String dbname = config.get("database").toString();
            if (dbname.isEmpty()) {
                dbname = "test";
            }

			String collection = config.get("collection").toString();
			String monitor = config.get("monitor").toString();
			int delay;
            String delayStr = config.get("delay").toString();
            if (delayStr == "null") {
                delay = 10;
            }
            else {
                delay = (Integer)config.get("delay");
                if (delay == 0) {
                    delay = 10;
                }
            }

			String wordFile = config.get("words").toString();
			String clientLog = config.get("clientLog").toString();
			String serverLog = config.get("serverLog").toString();
			String wordFilter = config.get("wordFilter").toString();

			Logger logger = Logger.getLogger("org.mongodb.driver");
			logger.setLevel(Level.OFF);

			MongoClient client = new MongoClient(server, port);
			MongoDatabase db = client.getDatabase(dbname);
			MongoCollection<Document> dox = db.getCollection(collection);

			Date now = new Date();
            long count = dox.count();

			System.out.println("Current Timestamp: " + new Timestamp(now.getTime()));
			System.out.println("Server: " + server);
			System.out.println("Port: " + port);
			System.out.println("Database: " + dbname);
			System.out.println("Collection: " + collection);
			System.out.println("Documents in collection: " + count);

			File log = new File(clientLog);
			log.createNewFile();
			PrintWriter writer = new PrintWriter(new FileWriter(log));

			int msDelay = delay * 1000;
			int cycle = 1;
			while (true) {
				Thread.sleep(msDelay);
				JSONObject obj = generateJSON(writer, wordFile);
                System.out.println(obj.toString(2));
                System.out.println(new Timestamp((new Date()).getTime()));

                Document newRecord = new Document();
                newRecord.append("recepient", obj.get("recepient"));
                newRecord.append("messageId", obj.get("messageId"));
                newRecord.append("text", obj.get("text"));
                newRecord.append("user", obj.get("user"));
                newRecord.append("status", obj.get("status"));
                if (obj.has("in-response"))
                    newRecord.append("in-response", obj.get("in-response"));

                db.getCollection(collection).insertOne(newRecord);

				if (cycle % 40 == 0) {

                    //query total msgs stored
                    long msgCount = dox.count();
                    System.out.println("\nTotal messages stored in collection: " + msgCount);


                    //query total msg count written by author of last generate msg
                    String lastUser = obj.get("user").toString();

                    Document query = new Document();
                    query.append("user", lastUser);
                    long usrMsgCount = dox.count(query);
                    System.out.println("User " + lastUser + " message count: " + usrMsgCount + "\n");

				}
                cycle += 1;
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}

}
