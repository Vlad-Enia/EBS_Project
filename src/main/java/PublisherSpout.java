import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class PublisherSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1;
    private SpoutOutputCollector collector;
    private final List<Values> valueList = new ArrayList<>();
    private int i = 0;

    // remove template type qualifiers from conf declaration for Storm v1
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        String task = context.getThisComponentId();
        this.valueList.add(new Values("1", "Bucharest", "2", "2"));
        this.valueList.add(new Values("1", "Iasi", "20", "1"));
        this.valueList.add(new Values("1", "Bucharest", "9", "6"));
        System.out.println("----- Started publisher spout task: " + task);
    }

    public void nextTuple() {
        // continuous tuple emission variant
		/*
		this.collector.emit(new Values(sourcetext[i]));
		i++;
		if (i >= sourcetext.length) {
			i = 0;
		}
		*/
        if(this.i == this.valueList.size())
            return;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.collector.emit(this.valueList.get(i++));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("stationid", "city", "temp", "wind"));
    }

}