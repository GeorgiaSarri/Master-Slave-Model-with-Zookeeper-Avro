# Master-Slave-Model-with-Zookeeper-Avro

The following project was my dimploma thesis for my BSc in Software Engineering.
A master-slave design model was implemented to act as a middleware for a distributed database system.
The goal was to be able to distribute the load of requests from clients to different servers depending on various criteria
that will be analyzed below. The project was implemented on Java and Apache's Zookeerer and Avro were used for the synchronization and the message exchange respectively.

## Model Description

### Components

 * Master : The master is responsible to assign a server to a specific client depending on one of the following load balancing criteria:
    1) Total server run time: priority is given to servers with smallest up time
    2) Average server run time: the total run time of the server is calculated as the sum of the time that it took to perform each request. Priority is given to the servers with the  smallest run time divided by the total of its requests.
    3) Number of clients on server: priority is given to the servers with the smallest number of clients
    4) Random selection
    
    He has his own unique id (masterId) and keeps a ConcurrentSkipListMap (serversMap) with the servers' information and a CopyOnWriteList (clientList) with the clients' information.
    Each time a new client is up, it connects with the Master through the (avro) protocol MasterClient and through the hello(Hello record) method (implemented in MasterClientImpl Class) the Master gets the client's information and stores it in its clientList.
    Then, through the method selectServer(serversMap :ConcurrentSkipListMap < String; ServerObj >) based on one of the above criteriaa server is selected for the client.
    Additionally the master communicates with the Zookeeper to inform the later for any changes on the servers' status.
    
  * Server: The server is responisble to handle all the requests towards the DB. It also connects with ZooKeper using it as a medium for his communication with the Master. It keeps a ConcurrentHashMap (clientsMap)
  with the clients' info that are assigned to it and two Schedulers, one to measure its average up time and one to remove from his clientsMap the non-responsive clients.
  The communication with the clients is performed through (avro) protocol ServerClient and the messages are handled by the class ServerClientImpl which holds a list with all the necessary information about each request.
  The different requests that are handled by the server are:
    1) INIT: initialize the connection between Server-Client. For this request the server creates a new Client object in its clientsMap and initializes the requests timestamp.
    2) SELECT
    3) INSERT
    4) UPDATE
    5) DELETE
  
    When the connection with the client is terminated, the server calcuates the total time used for the request, removes the client from his list and updates ZooKeeper for its new run time and total number of clients.
  
  * Client: The client sends requests to the database. It first connects to a Master from which asks a server. Then after the Master's response with the server's information, the clients connects with the server. Client is 
  represented by three classes: 
    1) User: we can say it represents the UI and instantiates a Client object.
    2) Client: connects with the master to get the server's information and instantiates a Connection with the server. Also, closes the connection with the Master when the process is done.
    3) Connection: connects with a server.
    
  ## User Case
  Initially the user requests a transaction with the database, then User Object creates a Client object which communicates with a master requesting a server. The master 
  checks if an available server exists, if not it sends a message back to the client to try again later and creates one server. In case that many servers are
  available it chooses one based on a selection criterio and sends back to the client the necessary information. Once the Client receives those information it creates a Connection object
  so as to connect with the server. The Connection object sends first an INIT request so as to connect with the server. Once the server receives an INIT request it communicates with 
  ZooKeeper to update its information and in turn ZooKeeper communicates with Master to inform him about the changes in the specific server. The server sends back a message to the client for the 
  sucess or failure of the connection. In case of a failure the connection is interupted else client's Connection object sends all the requests to the specific server.
  The process is terminated by the user, then Connection object sends a message of termination to the server which in turn removes the client from his list of clients and updates ZooKeeper. Next the client closes the
  connection with the master and then the process is completed.
  
  ## Execution Steps
  
  1) Install Apache ZooKeeper from its [official website](http://zookeeper.apache.org "Apache Zookeeper Homepage")
  
  
