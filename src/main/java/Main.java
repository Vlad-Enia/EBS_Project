import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class Main {
    public static void main(String[] args) {
        try {
            TopologyBuilder builder = new TopologyBuilder();
            PublisherSpout publisher = new PublisherSpout();

            builder.setSpout("publisher_spout", publisher);

            for (int i = 1; i <= 3; ++i) {
                builder.setSpout("subscription" + i, new SubscriptionSpout());
            }
            for (int i = 1; i <= 3; ++i) {
                builder.setBolt("broker" + i, new BrokerBolt()).allGrouping("publisher_spout").shuffleGrouping("subscription1", "broker" + i).shuffleGrouping("subscription2", "broker" + i).shuffleGrouping("subscription3", "broker" + i);
            }
            for (int i = 1; i <= 3; i++) {
                builder.setBolt("notifier" + i, new NotifierBolt()).shuffleGrouping("broker1", "notifier" + i).shuffleGrouping("broker2", "notifier" + i).shuffleGrouping("broker3",  "notifier" + i);
            }


            Config config = new Config();

            LocalCluster cluster = new LocalCluster();
            StormTopology topology = builder.createTopology();

            config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
            config.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 1);

            cluster.submitTopology("publish_subscribe_topology", config, topology);

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            cluster.killTopology("publish_subscribe_topology");
            cluster.shutdown();

            cluster.close();
        } catch(Exception ex) {

        }
    }
}
