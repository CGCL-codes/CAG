package storm.starter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
/*     */ public class DataReaderSpoutDidi extends BaseRichSpout
/*     */ {
/*  29 */   private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);
/*     */   private FileWriter fileWriter;
/*     */   private FileWriter fileWriter1;
 
/*     */   private Integer number_of_tuples;
/*     */   private Integer number_of_tuples_per_sec=0;
            final Set<String> header = new LinkedHashSet<String>();
/*     */   String arg0;
/*     */   private Integer spout;
/*     */   private Integer bolt1;
/*     */   private Integer bolt2;
            private Integer ackers;
            private Long sleep_time;
            private Integer bolt3;
/*     */   private Integer is_aggregator;
/*  39 */   boolean is_latency = false; boolean is_tt_300 = false; boolean is_tt_600 = false;
/*  40 */   String partStr1 = "/home/mudassar/output/";
/*     */   private ConcurrentHashMap<UUID, Long> pending;
/*     */   private SpoutOutputCollector collector;
/*     */   private Random rand;
/*  44 */   private long msgId = 0L;
/*     */   private Long start_time;
/*     */   private Long emit_last;
/*     */   private Long emit_current;
/*  48 */   Iterator<String> iterator = null;
/*  49 */   private boolean completed = false;
/*     */  public  FileReaderChild reader;
/*  51 */   private boolean readed = false;
/*     */   private TopologyContext context;
/*  54 */   Integer tuple_failed = Integer.valueOf(0);
/*  55 */   Integer tuple_count = Integer.valueOf(0);
/*  56 */   Integer tuple_emit = Integer.valueOf(0);
/*  57 */   long cet = 0L;
/*     */   private ArrayList tuple_records;
/*  61 */   private Random randomGenerator = new Random();
/*  62 */   private Integer idx = Integer.valueOf(0);
/*  63 */   private ArrayList csv_records = new ArrayList();
 /*  65 */   private FileReaderChild file = null;
/*     */ 
/*     */   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
/*     */   {
                 
            this.context = context;
            this.start_time = Long.valueOf(System.nanoTime());
            Long ft = Long.valueOf(System.nanoTime());
            String partStr2 = ft.toString() + "_" + this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString() + ".csv";
            String partStr3 = ft.toString() + "_" + this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString() + ".txt";
            this.reader = new FileReaderChild("/home/mudassar/didi2.txt","");
            header.add("Tuple Emit");
            header.add("Tuple Count");
            header.add("Tuple Count/Sec");
            header.add("Latency");
            header.add("Duration");
            String str = Utils.join(header, ",");
            String dirStr = this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString()+ "_" + this.is_aggregator.toString()+ "_" + this.number_of_tuples.toString()+ "_" + this.ackers.toString() ;
            String dirStr1 =  this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString()+ "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString()+ "_" + this.ackers.toString() ;
            String dirPath_th = partStr1+"tw_"+dirStr1+"/tw_"+dirStr;    
            String dirPath_fail = partStr1+"fail_"+dirStr1+"/fail_tw_"+dirStr;
/*     */     try {
                File dir_th=new File(dirPath_th); 
                    if (!dir_th.exists()) {
                           dir_th.mkdirs();
                    }
                File dir_fail=new File(dirPath_fail); 
                    if (!dir_fail.exists()) {
                         dir_fail.mkdirs();
                    }
/*  82 */       this.fileWriter = new FileWriter(dirPath_th + "/throughput_" + partStr2);
/*  83 */       this.fileWriter1 = new FileWriter(dirPath_fail + "/fail_" + partStr3);
///*  84 */       this.fileWriter2 = new FileWriter(this.partStr1 + "emit_"+"task_comp_"+task_comp + partStr2);
                this.fileWriter.write(str+"\r\n");
/*     */     } catch (IOException e) {
/*  86 */       throw new RuntimeException("Error write file");
/*     */     }
/*     */ 
/*  89 */     this.collector = collector;
/*  90 */     this.pending = new ConcurrentHashMap();
/*  91 */     this.tuple_records = new ArrayList();
/*  92 */     this.emit_current = Long.valueOf(System.nanoTime());
/*  93 */     this.emit_last = Long.valueOf(System.nanoTime());
/*     */   }
/*     */ /*     */ 
/*     */   DataReaderSpoutDidi(String arg0, int spout, int bolt1, int bolt2, int is_aggregation, int number_tuples, int bolt3,int ackers,int sleep_time) {
/*  97 */     this.number_of_tuples = Integer.valueOf(number_tuples);
/*  98 */     this.arg0 = arg0;
/*  99 */     this.spout = Integer.valueOf(spout);
/* 100 */     this.bolt1 = Integer.valueOf(bolt1);
/* 101 */     this.bolt2 = Integer.valueOf(bolt2);
              this.bolt3 = Integer.valueOf(bolt3);
              this.ackers = Integer.valueOf(ackers);
              this.sleep_time= Long.valueOf(sleep_time);
              
/* 102 */     this.is_aggregator = Integer.valueOf(is_aggregation);
/*     */   }
/*     */ 
/*     */   public void nextTuple()
/*     */   {
/* 109 */    Utils.sleep(this.sleep_time);
/*     */ 
/* 111 */     Integer temp = Integer.valueOf(0);
/*     */ 
/* 113 */     while (temp < this.number_of_tuples.intValue()) {
/* 114 */       String word1 = null;
/*     */         word1 = reader.nextLine();
                    
///*     */       } catch (IOException e) {
///* 119 */         e.printStackTrace();
///*     */       }
                String word="";
                if(word1 != null)
                    word = word1;
                  
                String cvsSplitBy = ",";
                String[] temp_data = word.split(cvsSplitBy);
                if (temp_data.length > 4){
                        //for (int i = 0; i < temp_data.length; i++) {
                            if(temp_data[3] != "" && temp_data[4] !=""){
                                UUID msgId = UUID.randomUUID();
                                temp = Integer.valueOf(temp.intValue() + 1);
                                //this.collector.emit(new Values(new Object[] { tw, Long.valueOf(1L) }), msgId);
                                this.collector.emit(new Values(new Object[] { temp_data[3]+temp_data[4]}), msgId);
                                tuple_emit = tuple_emit + 1;
                                Long t1 = Long.valueOf(System.nanoTime());
                                this.pending.put(msgId, t1);
                                if (temp.intValue() > this.number_of_tuples.intValue())
                                    break;
                            }
                        //}
                    }
                }
                

 
           }
/*     */ 
/*     */   public void declareOutputFields(OutputFieldsDeclarer declarer)
/*     */   {
/* 174 */     declarer.declare(new Fields(new String[] { "word" }));
/*     */   }
/*     */ 
/*     */   public void ack(Object msgId)
/*     */   {
                Long t2 = Long.valueOf(System.nanoTime());
                Long pending_message = Long.valueOf(0L);
                Integer tuple_size = Integer.valueOf(0);
                Long end_time = Long.valueOf(t2.longValue() - this.start_time.longValue());
                end_time = Long.valueOf(end_time.longValue() / 1000000000L);
                tuple_records.add(end_time);
                try {
                        t2 = Long.valueOf(t2.longValue() - ((Long)this.pending.get(msgId)).longValue());
                        pending_message = (Long)this.pending.get(msgId);
                } catch (ExceptionInInitializerError e) {
                      e.printStackTrace();
                }
                this.pending.remove(msgId);
                t2 = Long.valueOf(t2.longValue() / 1000000L);
                tuple_size = Integer.valueOf(this.tuple_records.size());
                if((end_time % 60) ==0   && end_time > 59L){
                    number_of_tuples_per_sec = number_of_tuples_per_sec +(number_of_tuples * 9);
                    Integer temp_tuple_emit_index = ( (int)(end_time/60)>0)? ((int)(end_time/60)):1;
                    String temp_tuple_emit = String.valueOf(tuple_emit/temp_tuple_emit_index);
                     List<String> line = new LinkedList<String>();
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
                         number_of_tuples_per_sec=0;
                }
                number_of_tuples_per_sec++; 
            }

/*     */   public void fail(Object msgId)
/*     */   {
/* 218 */     Long t2 = Long.valueOf(System.nanoTime());
/* 219 */     Long pending_message = Long.valueOf(0L);
/* 220 */     Integer tuple_size = Integer.valueOf(0);
/*     */ 
/* 222 */     Long end_time = Long.valueOf(t2.longValue() - this.start_time.longValue());
/* 223 */     end_time = Long.valueOf(end_time.longValue() / 1000000000L);
/* 224 */     DataReaderSpoutDidi localDataReaderSpout1 = this; Integer localInteger1 = localDataReaderSpout1.tuple_failed; Integer localInteger2 = localDataReaderSpout1.tuple_failed = Integer.valueOf(localDataReaderSpout1.tuple_failed.intValue() + 1);
/*     */     try
/*     */     {
/* 228 */       this.fileWriter1.write(" Failed:" + this.tuple_failed + " Topology Duration: " + end_time + "\r\n");
/*     */     } catch (IOException e) {
/* 230 */       e.printStackTrace();
/*     */     }
/*     */   }
/*     */ }

/* Location:           D:\mudassar\phd\HUST\Prof Chen\experiments\storm\paritioning algo paper\files\storm-starter-1.2.2.jar
 * Qualified Name:     storm.starter.DataReaderSpout1
 * JD-Core Version:    0.6.2
 */