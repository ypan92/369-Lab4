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

    private static void scanWordFile() {
        Scanner scanner;

        try {
            scanner = new Scanner(new File("sense.txt"));

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

	private static void generateJSON(PrintWriter writer) {
        JSONObject object;
        int messageId = 0;
        String status;

        scanWordFile();

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
            System.out.println(object.toString(2));
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        writer.flush();
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
			int port = (Integer)config.get("port");
			String dbname = config.get("database").toString();
			String collection = config.get("collection").toString();
			String monitor = config.get("monitor").toString();
			int delay = (Integer)config.get("delay");
			String wordFile = config.get("words").toString();
			String clientLog = config.get("clientLog").toString();
			String serverLog = config.get("serverLog").toString();
			String wordFilter = config.get("wordFilter").toString();

			Logger logger = Logger.getLogger("org.mongodb.driver");
			logger.setLevel(Level.OFF);

			MongoClient client = new MongoClient("cslvm31");
			MongoDatabase db = client.getDatabase(dbname);
			MongoCollection<Document> dox = db.getCollection(collection);

			Date now = new Date();
			System.out.println("Current Timestamp: " + new Timestamp(now.getTime()));

			System.out.println("Server: " + server);
			System.out.println("Port: " + port);
			System.out.println("Database: " + dbname);
			System.out.println("Collection: " + collection);

			long count = dox.count();
			System.out.println("Documents in collection: " + count);

			File log = new File("clientLog.txt");
			log.createNewFile();
			PrintWriter writer = new PrintWriter(new FileWriter(log));

			int msDelay = delay * 1000;
			int cycle = 1;
			while (true) {
				Thread.sleep(msDelay);
				generateJSON(writer);
				cycle += 1;
				if (cycle == 6) {
					break;
				}
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}

}