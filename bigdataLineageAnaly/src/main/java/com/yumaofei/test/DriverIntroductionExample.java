package com.yumaofei.test;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DriverIntroductionExample implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(DriverIntroductionExample.class.getName());
    private final Driver driver;

    public DriverIntroductionExample(String uri, String user, String password, Config config) {
        // The driver is a long living object and should be opened during the start of your application
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password), config);
    }

    @Override
    public void close() throws Exception {
        // The driver object should be closed before the application ends.
        driver.close();
    }

    public void createFriendship(String query) {
        // To learn more about the Cypher syntax, see https://neo4j.com/docs/cypher-manual/current/
        // The Reference Card is also a good resource for keywords https://neo4j.com/docs/cypher-refcard/current/
        try (Session session = driver.session()) {
            // Write transactions allow the driver to handle retries and transient errors
            session.writeTransaction(tx -> {
                Result result = tx.run(query);
                return result;
            });
//            System.out.println(String.format("Created friendship between: %s, %s",
//                    record.get("p1").get("name").asString()));
            // You should capture any errors along with the query and data for traceability
        } catch (Neo4jException ex) {
            LOGGER.log(Level.SEVERE, query + " raised an exception", ex);
            throw ex;
        }
    }

//    public Record getTx(Transaction tx){
//        Result result = tx.run(str);
//        return result.single();
//    }

    public void findPerson(final String personName) {
        String readPersonByNameQuery = "MATCH (p:Person)\n" +
                "WHERE p.name = $person_name\n" +
                "RETURN p.name AS name";

        Map<String, Object> params = Collections.singletonMap("person_name", personName);

        try (Session session = driver.session()) {
            Record record = session.readTransaction(tx -> {
                Result result = tx.run(readPersonByNameQuery, params);
                return result.single();
            });
            System.out.println(String.format("Found person: %s", record.get("name").asString()));
            // You should capture any errors along with the query and data for traceability
        } catch (Neo4jException ex) {
            LOGGER.log(Level.SEVERE, readPersonByNameQuery + " raised an exception", ex);
            throw ex;
        }
    }

//    public static void main(String... args) throws Exception {
//        // Aura queries use an encrypted connection using the "neo4j+s" protocol
//        String uri = "neo4j+s://90002f09.databases.neo4j.io";
//
//        String user = "neo4j";
//        String password = "Oskd68Nrb19dstr9BxOPch_UHJfi_TGvdzwiYdp759k";
//
//        try (DriverIntroductionExample app = new DriverIntroductionExample(uri, user, password, Config.defaultConfig())) {
//            app.createFriendship("Alice", "David");
////            app.findPerson("Alice");
//        }
//    }
}
