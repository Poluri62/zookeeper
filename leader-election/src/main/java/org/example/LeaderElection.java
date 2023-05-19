package org.example;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/* Watcher interface provides a callback mechanism for receiving events from Zookeeper
 */
public class LeaderElection implements Watcher {
    // specifies the address of thr Zookeeper server
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    // specifies timeout value for Zookeeper session
    private static final int SESSION_TIMEOUT = 10000;
    private static final String ELECTION_NAMESPACE = "/";//election";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }

    /*
    * Node volunteering for leadership by creating a znode
    * */
    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String znodePrefix = ELECTION_NAMESPACE; // c stands for candidate
        // path, data, access control list(acl), create mode
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE, "");
    }

    /*
    * Electing the leader based on the znodes created by the candidates.
    * Leader is determined based on the smallest znode name.
    * */
    public void electLeader() throws InterruptedException, KeeperException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestChild = children.get(0);

        if(smallestChild.equals(currentZnodeName)){
            System.out.println("I am the leader");
            return;
        }

        System.out.println("I am not the leader, " + smallestChild + " is the leader");
    }

    // Zookeeper Client API is synchronous and event driven

    /* establishes a connection to the ZooKeeper server by creating a new ZooKeeper object with the specified address,
     * session timeout, and a reference to the current instance of LeaderElection as the Watcher
     * */
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT,this );
    }

    /* Puts the main thread into a waiting state using zooKeeper.wait(). This effectively keeps the application running
     * until it is notified of an event by ZooKeeper.
     */
    public void run() throws InterruptedException{
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    /*  closes the connection to the ZooKeeper server by invoking the close() method on the zooKeeper object.
     */
    public void close() throws InterruptedException{
        zooKeeper.close();
    }

    /* Callback method from the Watcher interface.
     *
     * Called when an event occurs in ZooKeeper. In this code, it handles the None event type and checks if the event state
     * is SyncConnected. If so, it prints a message indicating a successful connection. If the event state is not
     * SyncConnected, it prints a message indicating disconnection, notifies any waiting threads, and allows the
     * application to exit.
     */
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()){
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully connected to Zookeeper");
                } else{
                    synchronized (zooKeeper){
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
            default:
                System.out.println("Client State: " + event.getState());
        }
    }
}
