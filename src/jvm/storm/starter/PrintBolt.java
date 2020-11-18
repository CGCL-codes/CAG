/*    */ package storm.starter;
/*    */ 
/*    */ import java.util.Map;
/*    */ import org.apache.storm.task.OutputCollector;
/*    */ import org.apache.storm.task.TopologyContext;
/*    */ import org.apache.storm.topology.IRichBolt;
/*    */ import org.apache.storm.topology.OutputFieldsDeclarer;
/*    */ import org.apache.storm.tuple.Fields;
/*    */ import org.apache.storm.tuple.Tuple;
/*    */ 
/*    */ public class PrintBolt
/*    */   implements IRichBolt
/*    */ {
/*    */   private OutputCollector collector;
/*    */ 
/*    */   public void prepare(Map conf, TopologyContext context, OutputCollector collector)
/*    */   {
/* 28 */     this.collector = collector;
/*    */   }
/*    */ 
/*    */   public void execute(Tuple tuple)
/*    */   {
/* 33 */     this.collector.emit(tuple.getValues());
/* 34 */     this.collector.ack(tuple);
/*    */   }
/*    */ 
/*    */   public void cleanup()
/*    */   {
/*    */   }
/*    */ 
/*    */   public void declareOutputFields(OutputFieldsDeclarer declarer)
/*    */   {
/* 45 */     declarer.declare(new Fields(new String[] { "word" }));
/*    */   }
/*    */ 
/*    */   public Map<String, Object> getComponentConfiguration()
/*    */   {
/* 50 */     return null;
/*    */   }
/*    */ }

/* Location:           D:\mudassar\phd\HUST\Prof Chen\experiments\storm\paritioning algo paper\files\storm-starter-1.2.2.jar
 * Qualified Name:     storm.starter.PrintBolt
 * JD-Core Version:    0.6.2
 */