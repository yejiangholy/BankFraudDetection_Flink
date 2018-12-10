import java.util.Map;

import org.apache.flink.util.Collector;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.functions.ProcessFunction.Context;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction.ReadOnlyContext;

public class Bank {

    public static final MapStateDescriptor<String, AlarmedCustomer> alarmedCustStateDescriptor =
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
        BroadcastStream<AlarmedCustomer> alarmedCustomerBroadcast = alarmedCustomers.broadcast(alarmedCustStateDescriptor);

        DataStream<LostCard> lostCards = env.readTextFile("src/data/lost_cards.txt")
                .map(new MapFunction<String, LostCard>() {
                    public LostCard map(String value) {
                        return new LostCard(value);
                    }
                });

        //2.broadcast lost card data
        BroadcastStream<LostCard> lostCardBroadcast = lostCards.broadcast(lostCardStateDescriptor);

        //3.transaction data keyed by customer_id
        DataStream<Tuple2<String, String>> data = env.socketTextStream("localhost", 9090)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    public Tuple2<String, String> map(String value) {
                        String[] words = value.split(",");

                        return new Tuple2<String, String>(words[3], value); //{(id_347hfx) (HFXR347924,2018-06-14 23:32:23,Chandigarh,id_347hfx,hf98678167,123302773033,774
                    }
                });

        // (1) check against alarmed customers
        DataStream<Tuple2<String, String>> alarmedCustTransactions = data
                .keyBy(0)
                .connect(alarmedCustomerBroadcast)
                .process(new AlarmedCustCheck());


        // (2) Check against lost cards
        DataStream<Tuple2<String, String>> lostCardTransactions = data
                .keyBy(0)
                .connect(lostCardBroadcast)
                .process(new LostCardCheck());







    }

    public static class AlarmedCustCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, AlarmedCustomer,
            Tuple2<String, String>> {

        public void processBroadcastElement(AlarmedCustomer cust, Context ctx,Collector<Tuple2<String, String>> out)throws Exception
        {
            ctx.getBroadcastState(alarmedCustStateDescriptor).put(cust.id, cust);
        }

        public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            for (Map.Entry<String, AlarmedCustomer> custEntry : ctx.getBroadcastState(alarmedCustStateDescriptor).immutableEntries()) {
                final String alarmedCustId = custEntry.getKey();
                final AlarmedCustomer cust = custEntry.getValue();

                // get customer_id of current transaction
                final String tId = value.f1.split(",")[3];
                if (tId.equals(alarmedCustId)) {
                    out.collect(new Tuple2<String, String>("____ALARM___", "Transaction: " + value + " is by an ALARMED customer"));
                }
            }
        }
    }

    public static class LostCardCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, LostCard, Tuple2<String, String>> {
        public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple2<String, String>> out)throws Exception {
            for (Map.Entry<String, LostCard> cardEntry: ctx.getBroadcastState(lostCardStateDescriptor).immutableEntries()) {
                final String lostCardId = cardEntry.getKey();
                final LostCard card = cardEntry.getValue();

                // get card_id of current transaction
                final String cId = value.f1.split(",")[5];
                if (cId.equals(lostCardId)) {
                    out.collect(new Tuple2<String, String>("__ALARM__",	"Transaction: " + value + " issued via LOST card"));
                }
            }
        }
        public void processBroadcastElement(LostCard card, Context ctx,	Collector<Tuple2<String, String>> out)	 throws Exception {
            ctx.getBroadcastState(lostCardStateDescriptor).put(card.id, card);
        }
    }
}
