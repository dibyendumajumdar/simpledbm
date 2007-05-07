package org.simpledbm.samples.tupledemo;



public class TupleDemo1 {

	public static void main(String[] args) {

		TupleDemoDb.createServer();
		
		TupleDemoDb db = new TupleDemoDb();
		db.startServer();
		try {
			db.addRow(1, "Rabindranath", "Tagore", "Shanti Niketan");
			db.addRow(2, "John", "Lennon", "New York");
			db.addRow(3, "Albert", "Einstein", "Princeton");
			db.addRow(4, "Mahatma", "Gandhi", "Delhi");
			
			System.out.println("Listing rows by name, surname");
			db.listRowsByKey(TupleDemoDb.SKEY1_CONTNO);

			System.out.println("Listing rows by ID");
			db.listRowsByKey(TupleDemoDb.PKEY_CONTNO);
			
			
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			db.shutdownServer();
		}
	}

}
