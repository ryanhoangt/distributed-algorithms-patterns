package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {

    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnode = null;
    // shared variable
    private List<String> allServiceAddresses;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry.");
    }

    private void createServiceRegistryZnode() {
        try {
            if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) {
                zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            /**
             * There is a race condition, which is resolved by ZooKeeper. Specifically,
             * it ensures only one 'create' action to one path will succeed; the remaining
             * will throw 'KeeperException'
             */
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void updateCachedAddress() throws InterruptedException, KeeperException {
        List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);

        List<String> addresses = new ArrayList<>(workerZnodes.size());

        for (String znode: workerZnodes) {
            String workerFullPath = REGISTRY_ZNODE + "/" + znode;
            Stat stat = zooKeeper.exists(workerFullPath, false);

            // Race condition: znode deleted between 'getChildren' and 'exists'
            if (stat == null) {
                continue;
            }

            byte[] addressByte = zooKeeper.getData(workerFullPath, false, stat);
            String address = new String(addressByte);
            addresses.add(address);
        }

        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster's all addresses: " + this.allServiceAddresses);
    }

    public void registerForUpdates() {
        try {
            updateCachedAddress();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws InterruptedException, KeeperException {
        if (allServiceAddresses == null) {
            updateCachedAddress();
        }
        return allServiceAddresses;
    }

    public void unregisterFromCluster() throws InterruptedException, KeeperException {
        if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
            zooKeeper.delete(currentZnode, -1);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateCachedAddress();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
