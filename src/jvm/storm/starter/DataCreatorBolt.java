/*    */ package storm.starter;
/*    */ 
/*    */ import java.util.Map;
/*    */ import org.apache.storm.task.OutputCollector;
/*    */ import org.apache.storm.task.TopologyContext;
/*    */ import org.apache.storm.topology.IRichBolt;
/*    */ import org.apache.storm.topology.OutputFieldsDeclarer;
/*    */ import org.apache.storm.tuple.Fields;
/*    */ import org.apache.storm.tuple.Tuple;
/*    */ import org.apache.storm.tuple.Values;
/*    */ import org.slf4j.Logger;
/*    */ import org.slf4j.LoggerFactory;
/*    */ 
/*    */ public class DataCreatorBolt
/*    */   implements IRichBolt
/*    */ {
/* 24 */   private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);
/*    */   private OutputCollector collector;
/*    */ 
/*    */   public void prepare(Map conf, TopologyContext context, OutputCollector collector)
/*    */   {
/* 31 */     this.collector = collector;
/*    */   }
/*    */ 
/*    */   public void execute(Tuple tuple)
/*    */   {
/* 38 */     Object word1 = tuple.getString(0);
                if(word1 != null){
                    String word = word1.toString();
                    this.collector.emit(new Values(new Object[] { word}));
                    this.collector.ack(tuple);
                }
//            if(word1 != null){
///* 39 */     String word = word1.toString();
///* 40 */     String cvsSplitBy = " ";
///* 41 */     String[] temp_data = word.split(cvsSplitBy);
///* 42 */     //if (temp_data.length > 4) {
///* 43 */      // String tweet = temp_data[5];
///* 44 */       //String[] tweet_arr = tweet.split(" ");
///* 45 */       if (temp_data.length > 0){
///* 46 */         for (String tw : temp_data) {
///* 47 */           this.collector.emit(new Values(new Object[] { tw,1L }));
///* 48 */           this.collector.ack(tuple);
///*    */         }
///*    */     }
//            }
/*    */   }
// public void execute(Tuple tuple)
///*    */   {
///* 38 */     Object word1 = tuple.getString(0);
///* 39 */     String word = word1.toString();
///* 40 */     String cvsSplitBy = ",";
///* 41 */     String[] temp_data = word.split(cvsSplitBy);
///* 42 */     if (temp_data.length > 4) {
///* 43 */       String tweet = temp_data[5];
///* 44 */       String[] tweet_arr = tweet.split(" ");
///* 45 */       if (tweet_arr.length > 0)
///* 46 */         for (String tw : tweet_arr) {
///* 47 */           this.collector.emit(new Values(new Object[] { tw,1L }));
///* 48 */           this.collector.ack(tuple);
///*    */         }
///*    */     }
///*    */   }
/*    */ 
/*    */   public void cleanup()
/*    */   {
/*    */   }
/*    */ 
/*    */   public void declareOutputFields(OutputFieldsDeclarer declarer)
/*    */   {
/* 77 */     declarer.declare(new Fields(new String[] { "word" }));
/*    */   }
/*    */ 
/*    */   public Map<String, Object> getComponentConfiguration()
/*    */   {
/* 82 */     return null;
/*    */   }
/*    */ }

/* Location:           D:\mudassar\phd\HUST\Prof Chen\experiments\storm\paritioning algo paper\files\storm-starter-1.2.2.jar
 * Qualified Name:     storm.starter.DataCreatorBolt
 * JD-Core Version:    0.6.2
 */