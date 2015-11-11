package com.tegar.cassandraclient;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


import java.util.List;
import java.util.UUID;

/**
 * Created by tegarnization on 11/11/15.
 */

public class TwitterClient {

    private final Cluster cluster;
    private final Session session;
    private final static String KEYSPACE = "twitter";
    
    
    public void closeSession() {
        session.close();
    }
    
    public void closeCluster() {
        cluster.close();
    }

    public TwitterClient(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.println("Terhubung ke cluster: " + metadata.getClusterName());

        for(Host host : metadata.getAllHosts())
        {
            System.out.printf("Datacenter: %s, Host: %s, Rack: %s\n", host.getDatacenter(), host.getDatacenter(), host.getRack());
        }
        
        session = cluster.connect();
        this.initSchema(KEYSPACE, "SimpleStrategy", "1");
    }
    
    public void initSchema(String keyspace, String replicationStrategy, String replicationFactor) {
        // Create keyspace
        String query = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class':'" + replicationStrategy + "', 'replication_factor':"+replicationFactor+"};";
        session.execute(query);

        session.execute("USE "+KEYSPACE + ";");

        session.execute("CREATE TABLE IF NOT EXISTS users (username text PRIMARY KEY,password text)");
        session.execute("CREATE TABLE IF NOT EXISTS friends (username text, friend text, since timestamp, PRIMARY KEY (username, friend))");
        session.execute("CREATE TABLE IF NOT EXISTS followers (username text,follower text, since timestamp, PRIMARY KEY (username, follower))");
        session.execute("CREATE TABLE IF NOT EXISTS tweets (tweet_id uuid PRIMARY KEY, username text, body text)");
        session.execute("CREATE TABLE IF NOT EXISTS userline (username text, time timeuuid, tweet_id uuid, PRIMARY KEY (username, time)) WITH CLUSTERING ORDER BY (time DESC)");
        session.execute("CREATE TABLE IF NOT EXISTS timeline (username text, time timeuuid, tweet_id uuid, PRIMARY KEY (username, time)) WITH CLUSTERING ORDER BY (time DESC)");
    }

    public void addNewUser(String username, String password) {
        String query = "INSERT INTO users (username, password) " +
                "VALUES ('" + username + "', '" + password + "');";
        session.execute(query);
    }

    public void followUser(String followerUsername, String followedUsername) {
        String query = "INSERT INTO followers (username, follower, since) " +
                "VALUES ('" + followedUsername + "', '" + followerUsername+ "', toUnixTimestamp(now()));";
        session.execute(query);

        query = "INSERT INTO friends (username, friend, since) " +
                "VALUES ('" + followerUsername + "', '" + followedUsername+ "', toUnixTimestamp(now()));";
        session.execute(query);
    }
    
    
    public void tweeting(String username, String tweet) {
        String tweetId = UUID.randomUUID().toString();
        String query = "INSERT INTO tweets (tweet_id, username, body) " +
                "VALUES (" + tweetId + ", '" + username +"', '"+ tweet +"');";
        session.execute(query);

        query = "INSERT INTO userline (username, time, tweet_id) " +
                "VALUES ('" + username + "', now(), "+ tweetId +");";
        session.execute(query);

        query = "INSERT INTO timeline (username, time, tweet_id) " +
                "VALUES ('" + username + "', now(), "+ tweetId +");";
        session.execute(query);

        ResultSet results = session.execute("SELECT follower FROM followers WHERE username = '" + username + "';");

        List<Row> rows = results.all();
        if(rows.size()>0) 
        {
            query = "INSERT INTO timeline (username, time, tweet_id) VALUES ";

            for (int i=0;i<rows.size();++i) 
            {
                query += "('" + rows.get(i).getString(0) + "', now(), " + tweetId + ")";
                if(i!=rows.size()-1)
                    query += ",";
            }
            query += ";";

            session.execute(query);
        }
    }

    public static void printTweet(ResultSet results) {
        for (Row row:results) {
            System.out.println("@" + row.getString("username") + ": " +row.getString("body"));
        }
    }

    public ResultSet getUserline(String username) {
        ResultSet results = session.execute("SELECT tweet_id FROM userline WHERE username = '" + username + "';");
        List<Row> rows = results.all();

        String tweetIds = "";
        for (int i=0;i<rows.size();++i) 
        {
            tweetIds += rows.get(i).getUUID("tweet_id");
            if(i!=rows.size()-1)
                tweetIds += ",";
        }

        String query = "SELECT username, body FROM tweets WHERE tweet_id IN ("+ tweetIds +");";
        results = session.execute(query);

        return results;
    }

    public ResultSet showTimeline(String username) {
        ResultSet results = session.execute("SELECT tweet_id FROM timeline WHERE username = '" + username + "';");
        List<Row> rows = results.all();

        String tweetIds = "";
        for (int i=0;i<rows.size();++i) 
        {
            tweetIds += rows.get(i).getUUID("tweet_id");
            if(i!=rows.size()-1)
                tweetIds += ",";
        }

        String query = "SELECT * FROM tweets WHERE tweet_id IN ("+ tweetIds +");";
        results = session.execute(query);
        return results;
    }
    
    public void printMenu() {
        System.out.println("Welcome to CassandraTwitter");
        System.out.println("Type any command below\n\n");        
        System.out.println("SIGNUP <username>");
        System.out.println("FOLLOW <follower_username> <followed_username>");
        System.out.println("TWEET <username> <tweet>");
        System.out.println("USERLINE <username>");
        System.out.println("TIMELINE <username>");
        System.out.println("EXIT\n\n");

    }
    
     public static void main(String[] args) throws IOException {
        TwitterClient twitterClient = new TwitterClient("127.0.0.1");

        String input = null;
        String command = null;
        String unsplittedParams = null;

        do {
            twitterClient.printMenu();
            input = new BufferedReader(new InputStreamReader(System.in)).readLine().trim();

            if(input.isEmpty())
                System.err.println("Try again");
            else {
                String[] parameters = new String[0];
                int whitespaceIdx = input.indexOf(" ");

                if (whitespaceIdx > -1) {
                    command = input.substring(0, whitespaceIdx);
                    unsplittedParams = input.substring(whitespaceIdx + 1);
                    parameters = unsplittedParams.split(" ");
                } else
                    command = input;

                if (command.equalsIgnoreCase("SIGNUP") && parameters.length == 2) {
                    twitterClient.addNewUser(parameters[0], parameters[1]);
                    System.out.println("* " + parameters[0] + " succesfully registered");
                } else if (command.equalsIgnoreCase("FOLLOW") && parameters.length == 2) {
                    twitterClient.followUser(parameters[0], parameters[1]);
                    System.out.println("* " + parameters[0] + " is now following " + parameters[1]);
                } else if (command.equalsIgnoreCase("TWEET") && parameters.length>=2) {
                    twitterClient.tweeting(parameters[0], unsplittedParams.substring(parameters[0].length()));
                    System.out.println("* " + parameters[0] + " tweet has been added");
                } else if (command.equalsIgnoreCase("USERLINE") && parameters.length==1) {
                    ResultSet results = twitterClient.getUserline(parameters[0]);
                    printTweet(results);
                } else if (command.equalsIgnoreCase("TIMELINE") && parameters.length==1) {
                    ResultSet results = twitterClient.showTimeline(parameters[0]);
                    printTweet(results);
                } else if (command.equalsIgnoreCase("EXIT")) {
                    System.out.println("Exiting...");
                } else
                    System.err.println("Error command");
            }
        } while(!input.equalsIgnoreCase("EXIT"));

        twitterClient.closeSession();
        twitterClient.closeCluster();
    }

}
