import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SubscriptionSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1;
    private SpoutOutputCollector collector;
    private String task;
    private Boolean isSent = false;
    private int i = 1;


    // remove template type qualifiers from conf declaration for Storm v1
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;
        this.task = context.getThisComponentId();
        System.out.println("----- Started subscription spout task: "+this.task);

    }

    public void nextTuple() {
        if(isSent)
            return;
        isSent = true;
        List<FieldSubscription> subscription = new ArrayList<>();
        if(task.equals("subscription1")){
            subscription.add(new FieldSubscription("city", "=", "Bucharest"));
        } else if(task.equals("subscription2")){
            subscription.add(new FieldSubscription("temp", ">", "10"));
        } else if(task.equals("subscription3")){
            subscription.add(new FieldSubscription("wind", ">", "5"));
        }

        this.collector.emit("broker" + i, new Values(subscription));
        i++;
        if(i==4)
            i=1;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("broker1", new Fields("subscription"));
        declarer.declareStream("broker2", new Fields("subscription"));
        declarer.declareStream("broker3", new Fields("subscription"));
    }

}