import java.util.*;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class BrokerBolt extends BaseRichBolt {

    private static final long serialVersionUID = 2;

    private OutputCollector collector;
    private String task;
    private int processed_tuples = 0;
    private List<Float> temps = new ArrayList<>();
    static final private int WINDOW_SIZE = 2;

    private HashMap<String, List<List<FieldSubscription>>> subscriptions = new HashMap<>();

    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        int task_id = context.getThisTaskId();
        this.task = context.getThisComponentId();
        System.out.println("----- Started task: " + this.task);

        // for custom grouping example
        // for custom grouping example
        int drop_task = context.getComponentTasks(context.getThisComponentId()).get(0);
    }

    public void execute(Tuple input) {
        String sourceName = input.getSourceComponent();
        if (sourceName.contains("subscription")) {
            System.out.println(this.task + " got subscription " + sourceName);
            String sourceId = sourceName.substring(sourceName.length() - 1);
            List<FieldSubscription> subscription = (List<FieldSubscription>) input.getValue(0);
            if (!subscriptions.containsKey(sourceId)) {
                List<List<FieldSubscription>> newList = new ArrayList<>();
                subscriptions.put(sourceId, newList);
            }
            List<List<FieldSubscription>> existingList = subscriptions.get(sourceId);
            existingList.add(subscription);
            subscriptions.put(sourceId, existingList);
        } else {
            if (this.temps.size() == WINDOW_SIZE)
                this.temps.clear();
            this.temps.add(Float.parseFloat(input.getStringByField("temp")));
            for (String boltId : subscriptions.keySet()) {
                List<List<FieldSubscription>> aList = subscriptions.get(boltId);
                for (List<FieldSubscription> subscriptions1 : aList) {
                    boolean ok = true;
                    for (FieldSubscription fieldSubscription : subscriptions1) {
                        if (fieldSubscription.operator.equals("=")) {
                            String publication = input.getStringByField(fieldSubscription.field);
                            if (!publication.equals(fieldSubscription.value)) {
                                ok = false;
                                break;
                            }
                        } else if (fieldSubscription.operator.equals("!=")) {
                            String publication = input.getStringByField(fieldSubscription.field);
                            if (publication.equals(fieldSubscription.value)) {
                                ok = false;
                                break;
                            }
                        } else if (fieldSubscription.operator.equals("<")) {
                            float publication = Float.parseFloat(input.getStringByField(fieldSubscription.field));
                            float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                            if (!(publication < subscriptionValue)) {
                                ok = false;
                                break;
                            }
                        } else if (fieldSubscription.operator.equals(">")) {
                            float publication = Float.parseFloat(input.getStringByField(fieldSubscription.field));
                            float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                            if (!(publication > subscriptionValue)) {
                                ok = false;
                                break;
                            }
                        } else if (fieldSubscription.operator.equals("<=")) {
                            float publication = Float.parseFloat(input.getStringByField(fieldSubscription.field));
                            float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                            if (!(publication <= subscriptionValue)) {
                                ok = false;
                                break;
                            }
                        } else if (fieldSubscription.operator.equals(">=")) {
                            float publication = Float.parseFloat(input.getStringByField(fieldSubscription.field));
                            float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                            if (!(publication >= subscriptionValue)) {
                                ok = false;
                                break;
                            }
                        } else if (fieldSubscription.operator.equals("~")) {
                            if (this.temps.size() < WINDOW_SIZE) {
                                ok = false;
                                break;
                            } else {
                                float publication = 0;
                                for (float temp : this.temps) {
                                    publication += temp;
                                }
                                publication /= temps.size();
                                float subscriptionValue = Float.parseFloat(fieldSubscription.value.toString());
                                if (!(publication == subscriptionValue)) {
                                    ok = false;
                                    break;
                                }
                            }
                        }
                    }
                    if (ok) {
                        collector.emit("notifier" + boltId, new Values(input.getString(0), input.getString(1), input.getString(2), input.getString(3)));
                    }
                }
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("notifier1", new Fields("stationid", "city", "temp", "wind"));
        declarer.declareStream("notifier2", new Fields("stationid", "city", "temp", "wind"));
        declarer.declareStream("notifier3", new Fields("stationid", "city", "temp", "wind"));
    }

}