/*     */ package storm.starter;
/*     */ 
import java.io.File;
/*     */ import java.io.FileReader;
/*     */ import java.io.FileWriter;
/*     */ import java.io.IOException;
/*     */ import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
/*     */ import java.util.Map;
/*     */ import java.util.Random;
/*     */ import java.util.UUID;
/*     */ import java.util.concurrent.ConcurrentHashMap;
/*     */ import org.apache.storm.spout.SpoutOutputCollector;
/*     */ import org.apache.storm.task.TopologyContext;
/*     */ import org.apache.storm.topology.OutputFieldsDeclarer;
/*     */ import org.apache.storm.topology.base.BaseRichSpout;
/*     */ import org.apache.storm.tuple.Fields;
/*     */ import org.apache.storm.tuple.Values;
/*     */ import org.apache.storm.utils.Utils;
/*     */ import org.slf4j.Logger;
/*     */ import org.slf4j.LoggerFactory;
/*     */ 
/*     */ public class DataReaderSpout extends BaseRichSpout
/*     */ {
/*  30 */   private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);
/*     */   private FileWriter fileWriter;
/*     */   private FileWriter fileWriter1;
/*     */   private FileWriter fileWriter2;
/*     */   private Integer number_of_tuples;
/*     */   String arg0;
/*  36 */   boolean is_latency = false; boolean is_tt_300 = false; boolean is_tt_600 = false;
/*     */   private Integer spout;
/*     */   private Integer bolt1;
/*     */   private Integer bolt2;
/*     */   private Integer is_aggregator;
/*     */   private Integer number_of_tuples_per_sec=0;

/*  41 */   String partStr1 = "/home/mudassar/output/";
/*     */   private ConcurrentHashMap<UUID, Long> pending;
/*     */   private SpoutOutputCollector collector;
/*     */   private Random rand;
/*  45 */   private long msgId = 0L;
/*     */   private Long start_time;
/*     */   private Long emit_last;
/*     */   private Long emit_current;
/*  50 */   private boolean completed = false;
/*     */   private FileReader fileReader;
/*  52 */   private boolean readed = false;
/*     */   private TopologyContext context;
/*  55 */   Integer tuple_failed = Integer.valueOf(0);
/*  56 */   Integer tuple_count = Integer.valueOf(0);
/*  57 */   Integer tuple_emit = Integer.valueOf(0);
/*  58 */   long cet = 0L;
/*     */   private ArrayList tuple_records;
/*  62 */   private Random randomGenerator = new Random();
/*  63 */   private Integer idx = Integer.valueOf(0);
/*  64 */   private ArrayList csv_records = new ArrayList();
/*     */ private  HashMap<Long,Integer> endTimeList=new HashMap<Long,Integer>();
/*     */   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
/*  67 */     this.context = context;
/*  68 */     this.start_time = Long.valueOf(System.nanoTime());
/*  69 */     Long ft = Long.valueOf(System.nanoTime());
/*     */     try {
/*  71 */       String partStr2 = ft.toString() + "_" + this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString() + ".txt";
/*  72 */       String partStr3 = ft.toString() + "_" + this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString() + ".txt";
                String dirStr = this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.number_of_tuples.toString() ;
                String dirStr1 =  this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.number_of_tuples.toString() ;
                 String dirPath_th = partStr1+"zf"+dirStr1+"/zf_"+dirStr;
                String dirPath_fail = partStr1+"fail"+dirStr1+"/fail_"+dirStr;
                //this.fileWriter = new FileWriter(this.partStr1 + "throughput_" + partStr2);
                  //this.fileWriter.write(str+"\r\n");   
///*  73 */       this.fileWriter1 = new FileWriter(this.partStr1 + "fail_" + partStr2);
///*  74 */       this.fileWriter2 = new FileWriter(this.partStr1 + "emit_" + partStr2);     
                        File dir_th=new File(dirPath_th); 
                        if (!dir_th.exists()) {
                            dir_th.mkdirs();
                        }
                        File dir_fail=new File(dirPath_fail); 
                        if (!dir_fail.exists()) {
                            dir_fail.mkdirs();
                        }
                        this.fileWriter = new FileWriter(dirPath_th+ "/throughput_" + partStr2);
                        this.fileWriter1 = new FileWriter(dirPath_fail + "/fail_" + partStr3);
                      
                    

/*     */     } catch (IOException e) {
/*  76 */       throw new RuntimeException("Error write file");
/*     */     }
/*  78 */     this.collector = collector;
/*  79 */     this.pending = new ConcurrentHashMap();
/*  80 */     this.tuple_records = new ArrayList();
/*  81 */     this.emit_current = Long.valueOf(System.nanoTime());
/*  82 */     this.emit_last = Long.valueOf(System.nanoTime());
/*     */   }
/*     */   DataReaderSpout(String arg0, int spout, int bolt1, int bolt2, int is_aggregation, int number_tuples) {
/*  85 */     this.number_of_tuples = Integer.valueOf(number_tuples);
/*  86 */     this.arg0 = arg0;
/*  87 */     this.spout = Integer.valueOf(spout);
/*  88 */     this.bolt1 = Integer.valueOf(bolt1);
/*  89 */     this.bolt2 = Integer.valueOf(bolt2);
/*  90 */     this.is_aggregator = Integer.valueOf(is_aggregation);
//                for (int i=0;i<600;i++){
//                    endTimeList.put(0, 0);
//                }
                
/*     */   }
/*     */ 
/*     */   public void nextTuple()
/*     */   {
/*  98 */     Utils.sleep(100L);
/*  99 */     for (int inc = 0; inc < this.number_of_tuples.intValue(); inc++)
/*     */     {
/* 101 */       UUID msgId = UUID.randomUUID();
/* 102 */       Integer randNum = Integer.valueOf(this.randomGenerator.nextInt(1000));
/*     */ 
/* 104 */       this.collector.emit(new Values(new Object[] { randNum.toString(), Integer.valueOf(1) }), msgId);
/* 105 */       DataReaderSpout localDataReaderSpout = this; Integer localInteger1 = localDataReaderSpout.tuple_emit; Integer localInteger2 = localDataReaderSpout.tuple_emit = Integer.valueOf(localDataReaderSpout.tuple_emit.intValue() + 1);
/*     */ 
/* 107 */       Long t1 = Long.valueOf(System.nanoTime());
/* 108 */       this.pending.put(msgId, t1);
/*     */     }
/*     */ 
/* 111 */     //Long temp_time = Long.valueOf(System.nanoTime());
///* 112 */     Double emit_diff = Double.valueOf((temp_time.longValue() - this.start_time.longValue()) / 1000000000.0D);
///*     */     try
///*     */     {
///* 115 */       this.fileWriter2.write("Duration:" + emit_diff + "Tuple emit:" + this.tuple_emit + "\r\n");
///*     */     } catch (IOException e) {
///* 117 */       e.printStackTrace();
///*     */     }
/*     */   }
/*     */ 
/*     */   public void declareOutputFields(OutputFieldsDeclarer declarer)
/*     */   {
/* 123 */     declarer.declare(new Fields(new String[] { "word", "count" }));
/*     */   }
/*     */ 
/*     */   public void ack(Object msgId)
/*     */   {
/* 129 */     Long t2 = Long.valueOf(System.nanoTime());
/* 130 */     Long pending_message = Long.valueOf(0L);
/* 131 */     Integer tuple_size = Integer.valueOf(0);
/*     */ 
/* 133 */     Long end_time = Long.valueOf(t2.longValue() - this.start_time.longValue());
/*     */ 
/* 136 */     end_time = Long.valueOf(end_time.longValue() / 1000000000L);
/*     */ 
/* 138 */     this.tuple_records.add(end_time);
/*     */ 
/* 140 */     boolean is_new_sec = true;
/* 141 */     if (end_time.longValue() - this.cet > 0L) {
/* 142 */       this.cet = end_time.longValue();
/* 143 */       is_new_sec = true;
/*     */     } else {
/* 145 */       is_new_sec = false;
/*     */     }
/*     */     try {
/* 148 */       t2 = Long.valueOf(t2.longValue() - ((Long)this.pending.get(msgId)).longValue());
/* 149 */       pending_message = (Long)this.pending.get(msgId);
/*     */     } catch (ExceptionInInitializerError e) {
/* 151 */       e.printStackTrace();
/*     */     }
/* 153 */     this.pending.remove(msgId);
/* 154 */   //  e = this; Integer localInteger1 = e.tuple_count; Integer localInteger2 = e.tuple_count = Integer.valueOf(e.tuple_count.intValue() + 1);
/* 155 */       t2 = Long.valueOf(t2.longValue() / 1000000L);
/* 156 */       tuple_size = Integer.valueOf(this.tuple_records.size());
                number_of_tuples_per_sec = number_of_tuples_per_sec +(number_of_tuples * 9);
                Integer temp_tuple_emit_index = ( (int)(end_time/60)>0)? ((int)(end_time/60)):1;
                String temp_tuple_emit = String.valueOf(tuple_emit/temp_tuple_emit_index);
                 List<String> line = new LinkedList<String>();
                
                if( endTimeList.get(end_time)==null){
                    try
                    {
                        line.add((String)temp_tuple_emit);
                        line.add(tuple_size.toString());
                        line.add(number_of_tuples_per_sec.toString());
                        line.add(t2.toString());
                        line.add(end_time.toString());
                        String str = Utils.join(line, ",");
                        this.fileWriter.write(str+"\r\n");
                        //this.fileWriter.write("Tuple emit:" + temp_tuple_emit + " Latency:" + t2 + " Tuple Count:" + tuple_size +  " Tuple Count per sec:" + number_of_tuples_per_sec + "Topology Duration: " + end_time + "\r\n");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    endTimeList.put(end_time, 1);
                }
                         number_of_tuples_per_sec=0;
                //}
                number_of_tuples_per_sec++; 
///* 157 */     if (is_new_sec == true)
///*     */       try
///*     */       {
///* 160 */         this.fileWriter.write("Tuple0 emit:" + this.tuple_emit + " Latency:" + t2 + " Tuple Count:" + tuple_size + "Topology Duration: " + end_time + "\r\n");
///*     */       } catch (IOException e) {
///* 162 */         e.printStackTrace();
///*     */       }
/*     */   }
/*     */ 
/*     */   public void fail(Object msgId)
/*     */   {
/* 205 */     DataReaderSpout localDataReaderSpout = this; Integer localInteger1 = localDataReaderSpout.tuple_failed; Integer localInteger2 = localDataReaderSpout.tuple_failed = Integer.valueOf(localDataReaderSpout.tuple_failed.intValue() + 1);
/*     */     try {
/* 207 */       this.fileWriter1.write("Tuple Id:" + msgId + " Failed:" + this.tuple_failed + "\r\n");
/*     */     } catch (IOException e) {
/* 209 */       e.printStackTrace();
/*     */     }
/*     */   }
/*     */ }

/* Location:           D:\mudassar\phd\HUST\Prof Chen\experiments\storm\paritioning algo paper\files\storm-starter-1.2.2.jar
 * Qualified Name:     storm.starter.DataReaderSpout
 * JD-Core Version:    0.6.2
 */