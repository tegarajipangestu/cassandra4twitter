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
    
    public void initSchema(String keyspace, String replicationStrategy, String replicationFactor) {
        // Create keyspace
        String query = "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class':'" + replicationStrategy + "', 'replication_factor':"+replicationFactor+"};";
        session.execute(query);

        session.execute("USE "+TWITTER_KEYSPACE + ";");

        session.execute("CREATE TABLE IF NOT EXISTS users (username text PRIMARY KEY,password text)");
        session.execute("CREATE TABLE IF NOT EXISTS friends (username text, friend text, since timestamp, PRIMARY KEY (username, friend))");
        session.execute("CREATE TABLE IF NOT EXISTS followers (username text,follower text, since timestamp, PRIMARY KEY (username, follower))");
        session.execute("CREATE TABLE IF NOT EXISTS tweets (tweet_id uuid PRIMARY KEY, username text, body text)");
        session.execute("CREATE TABLE IF NOT EXISTS userline (username text, time timeuuid, tweet_id uuid, PRIMARY KEY (username, time)) WITH CLUSTERING ORDER BY (time DESC)");
        session.execute("CREATE TABLE IF NOT EXISTS timeline (username text, time timeuuid, tweet_id uuid, PRIMARY KEY (username, time)) WITH CLUSTERING ORDER BY (time DESC)");
    }
}
