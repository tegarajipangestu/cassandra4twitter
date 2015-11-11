# cassandra4twitter
Simple implementation for Tweeting with Cassandra

### Requirements

```java 1.8```

### How to Run

 - cd to ```target```
 - ```java -jar cassandra-twitter-1.0-SNAPSHOT.jar```

### Commands available

 - SIGNUP <username>
	 - To register new user
 - FOLLOW <usernamefollower> <usernametofollow>
	 - To follow another user
 - TWEET <username> <tweet>
	 - To tweet something
 - USERLINE <username>
	 - Show ```username``` timeline
 - TIMELINE <username>
	 - Show global timeline
 - EXIT
