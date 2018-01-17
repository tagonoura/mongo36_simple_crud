package com.mycompany.app;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;;

/**
 * 簡単なCRUD
 *
 */
public class App {

	final static String HOST = "localhost";
	final static int PORT = 27017;
	final static String DATABASE_NAME = "testDb";

	public static void main(String[] args) {

		//【テストデータ作成】
		doMakeData();

		//【登録：１件】
		doInsertOneItem();

		//【登録：複数件】
		doInsertManyItems();

		//【更新：１件】
		doUpdateOneItem();

		//【更新：複数件】
		doUpdateManyItems();

		//【削除：１件】
		doDeleteOneItem();

		//【削除：複数件】
		doDeleteManyItems();

		//【削除：全件】
		doDeleteAllItems();

	}

	//============================================================
	// 呼び出し
	//============================================================
	private static void doMakeData() {
		makeTestData(); //itemsコレクションのクリア＆再インサート
		readItems();
	}

	private static void doInsertOneItem() {
		insertOneItem();
		readItems();
	}

	private static void doInsertManyItems() {
		insertManyItems();
		readItems();
	}

	private static void doUpdateOneItem() {
		insertOneItem();
		readItems();
		updateOne();
		readItems();
	}

	private static void doUpdateManyItems() {
		insertManyItems();
		readItems();
		updateMany();
		readItems();
	}

	private static void doDeleteOneItem() {
		insertOneItem();
		readItems();
		deleteOne();
		readItems();
	}

	private static void doDeleteManyItems() {
		insertManyItems();
		readItems();
		deleteMany();
		readItems();
	}

	private static void doDeleteAllItems() {
		insertManyItems();
		readItems();
		deleteAll();
		readItems();
	}

	//============================================================
	// 処理
	//============================================================
	private static void deleteAll() {

		String collectionName = "items";

		MongoClient mongoClient = new MongoClient(HOST, PORT);
		MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
		MongoCollection<Document> coll = database.getCollection(collectionName);

		try {
			coll.deleteMany(new Document());

		} catch (Exception e) {
			e.printStackTrace();
		}

		mongoClient.close();

	}

	private static void makeTestData() {
		deleteAll();
		insertManyItems();
	}

	/**
	 * update [collection Items] one Data
	 *
	 */
	private static void updateOne() {

		String collectionName = "items";

		MongoClient mongoClient = new MongoClient(HOST, PORT);
		MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
		MongoCollection<Document> coll = database.getCollection(collectionName);

		//更新内容設定（更新対象列を記載）
		String updJson = "{ \"price\" : 3939, \"stock\" : 39 }";
		Document updDoc = Document.parse(updJson);

		//1件更新実行（更新条件フィルタ , 更新列Document）
		coll.updateOne(Filters.eq("name", "notebook"), new Document("$set", updDoc));
		mongoClient.close();

	}

	/**
	 * update [collection Items] Many Data
	 *
	 */
	private static void updateMany() {

		makeTestData();

		String collectionName = "items";

		MongoClient mongoClient = new MongoClient(HOST, PORT);
		MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
		MongoCollection<Document> coll = database.getCollection(collectionName);

		try {

			// 更新対象フィルタ【200 <= price <= 300】
			Bson filter = Filters.and(Filters.gte("price", 200), Filters.lte("price", 300));

			//更新内容設定（更新対象列を記載）
			String updJson = "{ \"stock\" : 777 }";
			Document updDoc = Document.parse(updJson);

			coll.updateMany(filter, new Document("$set", updDoc));

		} catch (Exception e) {
			e.printStackTrace();
		}

		mongoClient.close();

	}

	/**
	 * delete [collection Items] one Data
	 *
	 */
	private static void deleteOne() {

		String collectionName = "items";

		MongoClient mongoClient = new MongoClient(HOST, PORT);
		MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
		MongoCollection<Document> coll = database.getCollection(collectionName);

		//1件更新実行（更新条件フィルタ , 更新列Document）
		coll.deleteOne(Filters.eq("name", "notebook"));
		mongoClient.close();

	}

	/**
	 * update [collection Items] Many Data
	 *
	 */
	private static void deleteMany() {

		makeTestData();

		String collectionName = "items";

		MongoClient mongoClient = new MongoClient(HOST, PORT);
		MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
		MongoCollection<Document> coll = database.getCollection(collectionName);

		try {

			// 更新対象フィルタ【100 <= price <= 199】
			Bson filter = Filters.and(Filters.gte("price", 100), Filters.lte("price", 199));

			coll.deleteMany(filter);

		} catch (Exception e) {
			e.printStackTrace();
		}

		mongoClient.close();

	}

	/**
	 * Read [collection Items]
	 *
	 */
	private static void readItems() {

		String collectionName = "items";

		MongoClient mongoClient = new MongoClient(HOST, PORT);
		MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);
		MongoCollection<Document> coll = database.getCollection(collectionName);

		BasicDBObject query = new BasicDBObject();
		query.put("category", "stationery");
		System.out.println(query);

		FindIterable<Document> find = coll.find(query);
		MongoCursor<Document> cursor = find.iterator();
		try {
			while (cursor.hasNext()) {
				System.out.println(cursor.next().toJson());
			}
		} finally {
			cursor.close();
		}

		long count = coll.count(query);
		System.out.println(count + "件");
		mongoClient.close();

	}

	/**
	 * insert [collection Items] one Data
	 *
	 */
	private static void insertOneItem() {

		deleteAll();

		String collectionName = "items";

		MongoClient mongoClient = new MongoClient(HOST, PORT);
		MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);

		try {
			// compassで適当にデータを探して、json化
			String json = "{\"name\":\"notebook\",\"category\":\"stationery\",\"price\":210,\"stock\":7}";
			Document document = Document.parse(json);
			//insert実行
			database.getCollection(collectionName).insertOne(document);

		} catch (Exception e) {
			e.printStackTrace();
		}

		mongoClient.close();

	}

	/**
	 * insert [collection Items] three data
	 *
	 */
	private static void insertManyItems() {

		deleteAll();

		String collectionName = "items";

		MongoClient mongoClient = new MongoClient(HOST, PORT);
		MongoDatabase database = mongoClient.getDatabase(DATABASE_NAME);

		String jsonItemsAry[] = new String[6];
		jsonItemsAry[0] = "{\"name\":\"crayonRed\",\"category\":\"stationery\",\"price\":212,\"stock\":72}";
		jsonItemsAry[1] = "{\"name\":\"crayonGreen\",\"category\":\"stationery\",\"price\":214,\"stock\":74}";
		jsonItemsAry[2] = "{\"name\":\"crayonBlue\",\"category\":\"stationery\",\"price\":216,\"stock\":76}";
		jsonItemsAry[3] = "{\"name\":\"pencil\",\"category\":\"stationery\",\"price\":105,\"stock\":10}";
		jsonItemsAry[4] = "{\"name\":\"eraser\",\"category\":\"stationery\",\"price\":140,\"stock\":50}";
		jsonItemsAry[5] = "{\"name\":\"ballpoint\",\"category\":\"stationery\",\"price\":700,\"stock\":30}";

		List<Document> documentList = new ArrayList<Document>();
		for (String jsonItemStr : jsonItemsAry) {
			try {
				Document document = Document.parse(jsonItemStr);
				documentList.add(document);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		database.getCollection(collectionName).insertMany(documentList);

		mongoClient.close();

	}

}