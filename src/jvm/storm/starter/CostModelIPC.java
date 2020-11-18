/*    */ package storm.starter;
/*    */ 
/*    */ import java.io.PrintStream;
/*    */ import org.apache.storm.Config;
/*    */ import org.apache.storm.LocalCluster;
/*    */ import org.apache.storm.StormSubmitter;
/*    */ import org.apache.storm.generated.StormTopology;
/*    */ import org.apache.storm.topology.BoltDeclarer;
/*    */ import org.apache.storm.topology.TopologyBuilder;
         import storm.starter.IPCAGroupingV03;
/*    */ import org.apache.storm.tuple.Fields;
/*    */ import org.apache.storm.utils.Utils;
        import java.util.*;
/*    */ 
/*    */ public class CostModelIPC
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
/* 61 */        TopologyBuilder builder = new TopologyBuilder();
/* 62 */        Integer spouts = Integer.valueOf(args[1]);
                Integer bolt_counts = Integer.valueOf(args[2]);
                Integer bolt_agg = Integer.valueOf(args[3]);
                Integer is_aggregation_step = Integer.valueOf(args[4]);
                Integer tuples_to_emit = Integer.valueOf(args[5]);
                Integer number_of_workers = Integer.valueOf(args[6]); 
                Integer ackers = Integer.valueOf(args[7]); 
                Integer sleep_time = Integer.valueOf(args[8]); 
                Double skew = Double.valueOf(args[9]); 
                Long threshold= Long.valueOf(args[10]); 
                Integer supervisors= Integer.valueOf(args[11]); 
                Integer version_number= Integer.valueOf(args[12]); 
                String file_name = args[13]; 
                String ds = args[14]; 
                HashMap<String,Integer> cli_list_b1 = new HashMap<String, Integer>();
                cli_list_b1.put("workers", number_of_workers);
                cli_list_b1.put("ackers", ackers);
                cli_list_b1.put("PreLevel", 0);
                cli_list_b1.put("count_instances", bolt_counts);
                cli_list_b1.put("agg_instances",bolt_agg);
                cli_list_b1.put("worker_process",16);
                cli_list_b1.put("supervisors", supervisors); 
                cli_list_b1.put("spouts",spouts);
                cli_list_b1.put("level",0);
                HashMap<String,Integer> cli_list_b2 = new HashMap<String, Integer>();
                cli_list_b2.put("workers", number_of_workers);
                cli_list_b2.put("ackers", ackers);
                cli_list_b2.put("PreLevel", 1);
                cli_list_b2.put("count_instances",bolt_counts);
                cli_list_b2.put("agg_instances",bolt_agg);
                cli_list_b2.put("spouts",spouts);
                cli_list_b2.put("supervisors", supervisors); 
                if(is_aggregation_step == 0){
                    cli_list_b2.put("level",2);
                } else{
                    cli_list_b2.put("level",3);
                }
                cli_list_b2.put("worker_process",16); 
                //builder.setSpout("kafka-spout", new DataReaderSpoutZipF(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg), spouts);
                if (is_aggregation_step == 0 && skew>0 ){
                   builder.setSpout("kafka-spout", new DataReaderSpoutZipF(args[0], spouts,  bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,sleep_time,skew,ackers), spouts);
                   builder.setBolt("call-log-counter-bolt", new DataCounterBolt(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,skew,ackers), bolt_counts).customGrouping("kafka-spout", new IPCAGroupingV03(cli_list_b2,threshold));
                
                }else if(is_aggregation_step == 0 && skew == 0 ){
                   builder.setSpout("kafka-spout", new DataReaderSpout1(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,ackers,sleep_time,file_name,ds), spouts);
                   //builder.setSpout("kafka-spout", new DataReaderSpoutDidi(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg,ackers,sleep_time), spouts);
                   builder.setBolt("call-log-counter-bolt", new DataCounterBolt(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,skew,ackers), bolt_counts).customGrouping("kafka-spout", new IPCAGroupingV03(cli_list_b2,threshold));
                }
                else{
                     builder.setSpout("kafka-spout", new DataReaderSpout1(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,ackers,sleep_time,file_name,ds), spouts);
                    if(version_number==3){
                       
                        builder.setBolt("call-log-counter-bolt",new DataCounterBolt(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,skew,ackers), bolt_counts).customGrouping("kafka-spout", new IPCAGroupingV03(cli_list_b1,threshold));
                        builder.setBolt("call-log-agg-bolt", new MyAggregator(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,ackers), bolt_agg).customGrouping("call-log-counter-bolt", new IPCAGroupingV03(cli_list_b2,threshold));
                    }else{
                        builder.setBolt("call-log-counter-bolt",new DataCounterBolt(args[0], spouts, bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,skew,ackers), bolt_counts).customGrouping("kafka-spout", new IPCAGroupingV02(cli_list_b1,threshold));
                        builder.setBolt("call-log-agg-bolt", new MyAggregator(args[0], spouts,  bolt_counts,bolt_agg, is_aggregation_step, tuples_to_emit,ackers), bolt_agg).customGrouping("call-log-counter-bolt", new IPCAGroupingV02(cli_list_b2,threshold));

                    }                  
                    //builder.setBolt("call-log-agg-bolt", new MyAggregator(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg), bolt_agg).customGrouping("call-log-counter-bolt", new IPCAGrouping(cli_list_b2));
                    //builder.setBolt("call-log-creator-bolt", new DataCreatorBolt(), bolt_words).customGrouping("kafka-spout", new IPCAGrouping(cli_list_sp));
                    //builder.setBolt("call-log-counter-bolt", new DataCounterBolt(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg,skew), bolt_counts).customGrouping("call-log-creator-bolt", new IPCAGrouping(cli_list_b1));
                    //builder.setBolt("call-log-agg-bolt", new MyAggregator(args[0], spouts, bolt_words, bolt_counts, is_aggregation_step, tuples_to_emit,bolt_agg), bolt_agg).customGrouping("call-log-counter-bolt", new IPCAGrouping(cli_list_b2));

                } 
/* 84 */       Config config = new Config();
/* 85 */       //config.put("topology.message.timeout.secs", Integer.valueOf(60));
/*topology.message.timeout.secs (default: 30): This configures the maximum amount of time (in seconds) for a 
tutple's tree to be acknowledged (fully processed) before it is considered failed (timed out). 
Setting this value too low may cause tuples to be replayed repeatedly. For this setting to take effect, a spout must be configured to emit anchored tuples.
*
/*    */       config.setNumAckers(ackers);
/* 87 */       config.setDebug(false);
/*    */ 
/* 89 */       if ((args != null) && (args.length > 0) && (!args[0].equals("0")))
/*    */       {
/* 91 */         config.setNumWorkers(number_of_workers.intValue());
/* 92 */         StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
/*    */       }
/*    */       else {
/* 95 */         LocalCluster cluster = new LocalCluster();
/* 96 */         StormTopology topology = builder.createTopology();
/* 97 */         cluster.submitTopology("CostModel", config, topology);
/* 98 */         Utils.sleep(60000L);
/* 99 */         cluster.killTopology("CostModel");
/* 100 */         cluster.shutdown();
/*    */       }
/*    */     }
/*    */     catch (Exception e) {
/* 104 */       System.out.println("Exception in producer");
/* 105 */       System.out.println(e.toString());
/*    */     }
/*    */   }
/*    */ }

/* Location:           D:\mudassar\phd\HUST\Prof Chen\experiments\storm\paritioning algo paper\files\storm-starter-1.2.2.jar
 * Qualified Name:     storm.starter.CostModelPk
 * JD-Core Version:    0.6.2
 */