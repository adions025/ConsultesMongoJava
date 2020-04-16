package src;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class ConsultesMongo {

	private static MongoCollection<Document> mini;
	
	public static void main(String[] args) throws Exception {
		MongoClient c =  new MongoClient();
		MongoDatabase db = c.getDatabase("test");
		mini = db.getCollection("companies");
		String text = "Triar opcio : \n"+
				"1. Quines companyies tenen mes de tres productes?\n"+ 
				"2. Hi ha companyies que tenen mes d'un document on apareix el seu nom al camp name? \n"+ 
				"3 .Quines companyies tenen mes productes? \n"+
				"4. Quines son les empreses que no tenen camp partners? \n"+ 
				"5. Quantes empreses hi ha?\n"+ 
				"6. Quines empreses hi ha que tinguin tres elements a l'array adquisitions?\n"+ 
				"7. Canvieu el nom del valor \"null\" del camp category pel valor \"buit\"? \n"+ 
				"8. Afegiu un camp nou que es digui \"present\", i per totes aquelles empreses de dates de creacio anterior a l'any 2008, el valor del camp ha de ser \"no\".\n"+
				"9. Actualitzar la collection afegint la competicio \"Minnie\" amb permalink \"minnie\" a les empresesque tenen el competitor name \"Wikia\".\n"+
				"10. Quines companyies de les que son de category_codi \"advertising\", tenen m�s de tres competitions? \n";
		System.out.println(text);

		try{
			String s = "";
			while("exit".compareTo(s)!=0){
				BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
				s = bufferRead.readLine();
				executarMetode(s);
			}
		}
		catch(IOException e)
		{
			e.printStackTrace();
		}
	}

	private static void executarMetode(String opcio){
		switch (opcio) {
		case "1":
			System.out.println("hola");
			mesde3Productes();
			break;
		case "2":
			campName();
			break;
		case "3":
			mesProductes();
			break;
		case "4":
			companyiesSenseCampsPartners();
			break;
		case "5":
			quantesEmpresesHiha();
			break;
		case "6":
			tresElementsArrayAdquisitions();
			break;
		case "7":
			update();
			break;
		case "8":
			afegirCamp();
			break;
		case "9":
			updateMinnie();
			break;
		case "10":
			categoryCode();
			break;
		default:
			break;
		}
	}


	private static void categoryCode() {
		/**
		db.companies.find( {
			$and: [ {
			category_code:"advertising" }, {
			$where: "this.competitions.length>3"
			} ]}).projection({_id:0, name:1});
			 */
		BasicDBObject category_code = new BasicDBObject("category_code","advertising");
		BasicDBObject where = new BasicDBObject("$where","this.competitions.length>3");
		ArrayList al = new ArrayList();
		al.add(category_code);
		al.add(where);
		BasicDBObject find = new BasicDBObject("$and", al);
		BasicDBObject projection = new BasicDBObject("_id", 0).append("name", 1);
		FindIterable<Document> cursor = mini.find(find).projection(projection);
		MongoCursor<Document> iterator = cursor.iterator();
		int i=0;
		while(iterator.hasNext()) {
			System.out.println(iterator.next());
			i++;
		}
		System.out.println(i);
	}

	private static void updateMinnie() {
		/**
		db.companies.updateMany( 
		    {"competitions.competitor.name": "Wikia" },
		    {$push: {"competitions":{"competitor":{"name": "Minnie", "permalink": "minnie"}}}})
			 */
		BasicDBObject update = new BasicDBObject("competitions.competitor.name", "Wikia");
		BasicDBObject minnie = new BasicDBObject("name", "Minnie").append("permalink", "minnie");
		BasicDBObject push = new BasicDBObject("$push", new BasicDBObject("competitions", new BasicDBObject("competitor", minnie)));
		//AggregateIteration<Document> cursor  = mini.updateMany(update);
		System.out.println(mini.updateMany(update, push));
	}

	private static void afegirCamp() {
		/*
		 * Primero creamos el campo a toda la collection
		 * db.companies.updateMany( 
		    {},
		    { 
		        $set:{
		            "present":''
		        }
		    }
		);
		
		* A continuacion actualizamos el valor a 'no' en los casos donde su founded year sea < 2008
		db.companies.updateMany( 
		    {founded_year: {$lt: 2008}},
		    { 
		        $set:{
		            "present":'no'
		        }
		    }
		);*/
		BasicDBObject insertNewPresent = new BasicDBObject();
		BasicDBObject queryInsert = new BasicDBObject("$set", new BasicDBObject("present", ""));
		System.out.println(mini.updateMany(insertNewPresent, queryInsert));
		
		BasicDBObject updateObject = new BasicDBObject("founded_year", new BasicDBObject("$lt", 2008));
		BasicDBObject updateValue = new BasicDBObject("$set", new BasicDBObject("present", "no"));
		System.out.println(mini.updateMany(updateObject, updateValue));
	}

	private static void update() {
		/**
		*db.companies.updateMany(
		   { category_code: null },
		   { $set:
		      {
		        category_code: ""
		      }
		   }
		)
		 */
		
		BasicDBObject object = new BasicDBObject("category_code", null);
		BasicDBObject set = new BasicDBObject("$set", new BasicDBObject("category_code", ""));
		System.out.println(mini.updateMany(object, set));
	}

	private static void tresElementsArrayAdquisitions() {
		
		/**
		db.companies.find({
			"acquisitions":{$exists: true},
			 $where: "this.acquisitions.length>3"
			}).projection({_id:0,name:1}) 
		 */
		BasicDBObject object = new BasicDBObject();
		object.put("acquisitions", new BasicDBObject("$exists", Boolean.TRUE));
		object.put("$where", "this.acquisitions.length>3");
		BasicDBObject value = new BasicDBObject();
		value.put("_id", 0);
		value.put("name", 1);
		FindIterable<Document> cursor = mini.find(object).projection(value);
		MongoCursor<Document> iterator = cursor.iterator();
		int i=0;
		while(iterator.hasNext()) {
			System.out.println(iterator.next());
			i++;
		}
		System.out.println(i);
		

	}
	
	private static void quantesEmpresesHiha() {
		/**
		 * Query in mongoDB
		 * db.companies.find().count()
		 */
		
		System.out.println((mini.count()));

	}

	private static void companyiesSenseCampsPartners() {
		
		//db.companies.find({partners:{$exists:false}}).projection({_id:0,name:1})
		
		BasicDBObject find = new BasicDBObject("partners", new BasicDBObject("$exists", Boolean.FALSE));
		BasicDBObject projection = new BasicDBObject("_id", 0).append("name", 1);
		FindIterable<Document> cursor = mini.find(find).projection(projection);
		MongoCursor<Document> iterator = cursor.iterator();
		int i=0;
		while(iterator.hasNext()) {
			System.out.println(iterator.next());
			i++;
		}
		System.out.println(i);
		

	}

	private static void mesProductes() {
		/**
		 db.companies.aggregate([
		    {$unwind: "$products" },
		    {$group: {_id: "$name", products: {$push: "$products"}, size:{$sum:1}}},
		    {$sort: {size:-1}}])
		 */
		BasicDBObject unwind = new BasicDBObject("$unwind", "$products");
		BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$name").append("products", new BasicDBObject("$push", "$products")).append("size", new BasicDBObject("$sum",1)));
		BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject("size", -1));
		ArrayList al = new ArrayList();
		al.add(unwind);
		al.add(group);
		al.add(sort);
		AggregateIterable<Document> cursor = mini.aggregate(al);
		MongoCursor<Document> iterator = cursor.iterator();
		int i=0;
		while(iterator.hasNext()) {
			System.out.println(iterator.next());
			i++;
		}
		System.out.println(i);
	}

	private static void campName() {
		/**
		db.companies.aggregate([
			{$group:{"_id":"$name","name":{$first:"$name"},"count":{$sum:1}}},
			{$match:{"count":{$gt:1}}},
			{$project:{"name":1,"_id":0}},
			{$group:{"_id":null,"duplicateNames":{$push:"$name"}}},
			{$project:{"_id":0,"duplicateNames":1}}
			])
		 */
		BasicDBObject group = new BasicDBObject("_id", "$name").append("name", new BasicDBObject("$first", "$name")).append("count", new BasicDBObject("$sum",1));
		BasicDBObject groupA = new BasicDBObject("$group", group);
		BasicDBObject matchA = new BasicDBObject("$match", new BasicDBObject("count", new BasicDBObject("$gt", 1)));
		BasicDBObject projectA = new BasicDBObject("$project", new BasicDBObject("name", 1).append("_id", 0));
		BasicDBObject groupB = new BasicDBObject("$group", new BasicDBObject("_id", null).append("duplicateNames", new BasicDBObject("$push", "$name")));
		BasicDBObject projectB = new BasicDBObject("$project", new BasicDBObject("_id", 0).append("duplicateNames",1));
		ArrayList al = new ArrayList();
		al.add(groupA);
		al.add(matchA);
		al.add(projectA);
		al.add(groupB);
		al.add(projectB);
		AggregateIterable<Document> cursor  = mini.aggregate(al);
		MongoCursor<Document> iterator = cursor.iterator();
		while(iterator.hasNext()) {
			System.out.println(iterator.next());
		}
	}

	private static void mesde3Productes() {
		/**	 
		 * Query in mongoDB
		 db.companies.find(
			    	{
			        "products":{$exists: true},
			         $where: "this.products.length>3"
			     }).projection({_id:0,name:1}) 
		 */
		BasicDBObject object = new BasicDBObject();
		object.put("products", new BasicDBObject("$exists", Boolean.TRUE));
		object.put("$where", "this.products.length>3");
		BasicDBObject value = new BasicDBObject();
		value.put("_id", 0);
		value.put("name", 1);
		FindIterable<Document> cursor = mini.find(object).projection(value);
		MongoCursor<Document> iterator = cursor.iterator();
		int i=0;
		while(iterator.hasNext()) {
			System.out.println(iterator.next());
			i++;
		}
		System.out.println(i);
	}
}
