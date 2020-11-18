/*    */ package storm.starter;
/*    */ 
/*    */ import java.io.PrintStream;
/*    */ import org.apache.storm.Config;
/*    */ import org.apache.storm.LocalCluster;
/*    */ import org.apache.storm.StormSubmitter;
/*    */ import org.apache.storm.generated.StormTopology;
/*    */ import org.apache.storm.topology.BoltDeclarer;
/*    */ import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
/*    */ import org.apache.storm.utils.Utils;
/*    */ 
/*    */ public class CostModelSh
/*    */ {
/*    */   public static final String KAFKA_SPOUT_ID = "kafka-spout";
/*    */   public static final String AGGREGATOR_BOLT_ID = "aggregator-bolt";
/*    */   public static final String WORDCOUNTER_BOLT_ID = "wordcountter-bolt";
/*    */   public static final String TOPOLOGY_NAME = "keyGroupingBalancing-topology";
/*    */ 
/*    */   public static void main(String[] args)
/*    */     throws Exception
/*    */   {
/*    */     try
/*    */     {
/* 62 */       TopologyBuilder builder = new TopologyBuilder();
/* 63 */       Integer spouts = Integer.valueOf(args[1]);
/* 64 */       Integer bolt_counts = Integer.valueOf(args[2]);
               Integer bolt_agg = Integer.valueOf(args[3]);
               Integer is_aggregation_step = Integer.valueOf(args[4]);
               Integer tuples_to_emit = Integer.valueOf(args[5]);
               Integer number_of_workers = Integer.valueOf(args[6]); 
               Integer ackers = Integer.valueOf(args[7]); 
               Integer sleep_time = Integer.valueOf(args[8]); 
               Double skew = Double.valueOf(args[9]); 
               String file_name = args[10]; 
               String ds = args[11]; 
               if (is_aggregation_step == 0  && skew>0 ){
                   //builder.setSpout("kafka-spout", new DataReaderSpoutZipF(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg,sleep_time,skew,ackers), spouts);
                  // builder.setSpout("kafka-spout", new DataReaderSpout1(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg,ackers,sleep_time,file_name,ds), spouts);
                   builder.setSpout("kafka-spout", new DataReaderSpout(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit), spouts);
                   builder.setBolt("call-log-counter-bolt", new DataCounterBolt(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,skew,ackers), bolt_counts).shuffleGrouping("kafka-spout");
            
               }else if(is_aggregation_step == 0 && skew == 0 ){
                   
                    builder.setSpout("kafka-spout", new DataReaderSpout1(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,ackers,sleep_time,file_name,ds), spouts);
                    //builder.setSpout("kafka-spout", new DataReaderSpoutDidi(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg,ackers,sleep_time), spouts);
                    builder.setBolt("call-log-counter-bolt", new DataCounterBolt(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,skew,ackers), bolt_counts).shuffleGrouping("kafka-spout");
            
               } else{
                   //builder.setSpout("kafka-spout", new DataReaderSpoutZipF(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg,sleep_time,skew,ackers), spouts);
                  builder.setSpout("kafka-spout", new DataReaderSpout1(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,ackers,sleep_time,file_name,ds), spouts);
                   //  builder.setSpout("kafka-spout", new DataReaderSpout(args[0], spouts, bolt_words,bolt_agg, is_aggregation_step, tuples_to_emit), spouts);
                    builder.setBolt("call-log-counter-bolt", new DataCounterBolt(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,skew,ackers), bolt_counts).shuffleGrouping("kafka-spout");
                    builder.setBolt("call-log-agg-bolt", new MyAggregator(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,ackers), bolt_agg).shuffleGrouping("call-log-counter-bolt");
                 }
                
//                  builder.setBolt("call-log-creator-bolt", new DataCreatorBolt(), bolt_words).shuffleGrouping("kafka-spout");
//                    builder.setBolt("call-log-counter-bolt", new DataCounterBolt(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg,skew), bolt_counts).shuffleGrouping("call-log-creator-bolt");
//                    builder.setBolt("call-log-agg-bolt", new MyAggregator(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg), bolt_agg).shuffleGrouping("call-log-counter-bolt");
//                 
                  /*  if (is_aggregation_step.intValue() == 0) {
                    builder.setBolt("call-log-creator-bolt", new DataCreatorBolt(), bolt_words).shuffleGrouping("kafka-spout");
                    builder.setBolt("print-bolt", new PrintBolt(), bolt_counts).shuffleGrouping("print-bolt");
                }
                 else
           {*/
          
/*    */ 
/* 82 */       Config config = new Config();
/* 83 */       //config.put("topology.message.timeout.secs", Integer.valueOf(60));
            config.setNumAckers(ackers);
/* 84 */       config.setDebug(false);
/*    */ 
/* 86 */       if ((args != null) && (args.length > 0) && (!args[0].equals("0")))
/*    */       {
/* 89 */         config.setNumWorkers(number_of_workers.intValue());
/*    */ 
/* 91 */         StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
/*    */       }
/*    */       else {
/* 94 */         LocalCluster cluster = new LocalCluster();
/* 95 */         StormTopology topology = builder.createTopology();
/* 96 */         cluster.submitTopology("CostModel", config, topology);
/* 97 */         Utils.sleep(600000L);
/* 98 */         cluster.killTopology("CostModel");
/* 99 */         cluster.shutdown();
/*    */       }
/*    */     }
/*    */     catch (Exception e) {
/* 103 */       System.out.println("Exception in producer");
/* 104 */       System.out.println(e.toString());
/*    */     }
/*    */   }
/*    */ }

/* Location:           D:\mudassar\phd\HUST\Prof Chen\experiments\storm\paritioning algo paper\files\storm-starter-1.2.2.jar
 * Qualified Name:     storm.starter.CostModelSh
 * JD-Core Version:    0.6.2
 */