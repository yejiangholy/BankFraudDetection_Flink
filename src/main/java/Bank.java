import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Bank {

    public static final MapStateDescriptor<String, AlarmedCustomer> alarmedCustomerMapStateDescriptor =
            new MapStateDescriptor<String, AlarmedCustomer>("alarmed_customers", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(AlarmedCustomer.class));

    public static final MapStateDescriptor<String, LostCard> lostCardStateDescriptor =
            new MapStateDescriptor<String, LostCard>("lost_cards", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(LostCard.class))

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<AlarmedCustomer> alarmedCustomers = env.readTextFile("src/data/alarmed_cust.txt")
                .map(new MapFunction<String,AlarmedCustomer>(){
                    public AlarmedCustomer map(String value){
                        return new AlarmedCustomer(value);
                    }
                });

        //1.broadcast alarmed customer data
        BroadcastStream<AlarmedCustomer> alarmedCustomerBroadcast = alarmedCustomers.broadcast(alarmedCustomerMapStateDescriptor);

        DataStream<LostCard> lostCards = env.readTextFile("src/data/lost_cards.txt")
                .map(new MapFunction<String, LostCard>() {
                    public LostCard map(String value) {
                        return new LostCard(value);
                    }
                });

        //2.broadcast lost card data
        BroadcastStream<LostCard> lostCardBroadcast = lostCards.broadcast(lostCardStateDescriptor);

        //3.transaction data keyed by customer_id




    }
}
