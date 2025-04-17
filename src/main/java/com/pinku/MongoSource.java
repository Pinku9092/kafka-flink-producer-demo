package com.pinku;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.pinku.pojos.Employee;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.bson.Document;
public class MongoSource implements SourceFunction<Employee> {

    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DB_NAME = "flinkpersondb";
    private static final String COLLECTION_NAME = "persons";

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Employee> sourceContext) throws Exception {
        try(MongoClient mongoClient = MongoClients.create(MONGO_URI)) {
            MongoDatabase database = mongoClient.getDatabase(DB_NAME);
            MongoCollection<Document> collection = database.getCollection(COLLECTION_NAME);
        while(isRunning) {
            for (Document doc : collection.find()) {
                Employee employee = new Employee();
                employee.setId(doc.getInteger("id"));
                employee.setName(doc.getString("name"));
                employee.setAge(doc.getInteger("age"));
                employee.setSalary(doc.getInteger("salary"));

                sourceContext.collect(employee);


            }
            Thread.sleep(5000);

        }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
