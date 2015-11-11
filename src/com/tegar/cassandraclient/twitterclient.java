package com.tegar.cassandraclient;

/**
 * Created by tegarnization on 11/11/15.
 */
public class twitterclient {

	private Cluster cluster;
    private Session session;
    private final static String TWITTER_KEYSPACE = "twitter";
    private final static String TABLE_USERS = "users";
    private final static String TABLE_FOLLOWERS = "followers";
    private final static String TABLE_USERLINE = "userline";
    private final static String TABLE_TIMELINE = "timeline";
    private final static String TABLE_TWEETS = "tweets";
    private final static String TABLE_FRIENDS = "friends";

    public void connect(String node) {
        cluster = Cluster.builder().addContactPoint(node).build();
        Metadata metadata = cluster.getMetadata();
        System.out.println("Terhubung ke cluster: " + metadata.getClusterName());

        for(Host host:metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s, Host: %s, Rack: %s\n", host.getDatacenter(), host.getDatacenter(), host.getRack());
        }
        session = cluster.connect();
    }
    
    public void createSchema(String keyspace, String replicationStrategy, String replicationFactor) {
        // Create keyspace
        String query = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class':'" + replicationStrategy + "', 'replication_factor':"+replicationFactor+"};";
        session.execute(query);

        session.execute("USE "+TWITTER_KEYSPACE + ";");

        // Create table 'users'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_USERS +" (" +
                "username text PRIMARY KEY, " +
                "password text" +
                ")";
        session.execute(query);

        // Create table 'friends'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_FRIENDS +" (" +
                " username text, " +
                " friend text, " +
                " since timestamp, " +
                " PRIMARY KEY (username, friend) " +
                ")";
        session.execute(query);

        // Create table 'followers'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_FOLLOWERS+" (" +
                " username text, " +
                " follower text, " +
                " since timestamp, " +
                " PRIMARY KEY (username, follower) " +
                ")";
        session.execute(query);

        // Create table 'tweets'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_TWEETS +" (" +
                " tweet_id uuid PRIMARY KEY, " +
                " username text, " +
                " body text " +
                ")";
        session.execute(query);

        // Create table 'userline'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_USERLINE +" (" +
                " username text, " +
                " time timeuuid, " +
                " tweet_id uuid, " +
                " PRIMARY KEY (username, time)" +
                ") WITH CLUSTERING ORDER BY (time DESC)";
        session.execute(query);

        // Create table 'timeline'
        query = "CREATE TABLE IF NOT EXISTS "+ TABLE_TIMELINE  +" (" +
                " username text, " +
                " time timeuuid, " +
                " tweet_id uuid, " +
                " PRIMARY KEY (username, time)" +
                ") WITH CLUSTERING ORDER BY (time DESC)";
        session.execute(query);
    }
}
