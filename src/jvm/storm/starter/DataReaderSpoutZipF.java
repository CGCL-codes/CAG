/*
 * Copyright 2019 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
/**
 *
 * @author Administrator
 */
 
package storm.starter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.math3.distribution.ZipfDistribution;
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
/*     */ public class DataReaderSpoutZipF extends BaseRichSpout
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
          //  private Integer bolt3;
            private Integer ackers;
/*     */   private Integer is_aggregator;
            private Long iteration_count;
            private Double skew;
/*  39 */   boolean is_latency = false; boolean is_tt_300 = false; boolean is_tt_600 = false;
/*  40 */   String partStr1 = "/home/mudassar/output/";
/*     */   private HashMap<UUID, Long> pending;
/*     */   private SpoutOutputCollector collector;
/*     */   private Random rand;
/*  44 */   //private long msgId = 0L;
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
            Random _rand;
            Integer numMessages;
            int k; //unique Elements
            
            ZipfDistribution zipf;
            String randomStr;
            Integer messageCount;
            private  HashMap<Long,Integer> endTimeList=new HashMap<Long,Integer>();
            
            public String fillString(int count,char c) {
                StringBuilder sb = new StringBuilder( count );
                for( int i=0; i<count; i++ ) {
                    sb.append( c ); 
                }
                return sb.toString();
            }

            private static String createDataSize(int msgSize) {
                    StringBuilder sb = new StringBuilder(msgSize);
                    for (int i=0; i<msgSize; i++) {
                            sb.append('a');
                    }
                    return sb.toString();
            }
/*     */   public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
/*     */   {
                 
                this.context = context;
                this.start_time = Long.valueOf(System.nanoTime());
                Long ft = Long.valueOf(System.nanoTime());
                String partStr2 = context.getThisTaskId()+(ft.toString().substring(ft.toString().length()-4)) + "_" + this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString() + "_" + this.iteration_count.toString()+ "_" + this.skew.toString()+ ".csv";
                String partStr3 = ft.toString() + "_" + this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString()+ "_" + this.iteration_count.toString()+ "_" + this.skew.toString() + ".txt";
                String dirStr = this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.number_of_tuples.toString()+ "_" + this.skew.toString()+ "_" + this.ackers.toString() ;
                String dirStr1 =  this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.number_of_tuples.toString()+ "_" + this.skew.toString()+ "_" + this.ackers.toString() ;
                 String dirPath_th = partStr1+"zf"+dirStr1+"/zf_"+dirStr;
                String dirPath_fail = partStr1+"fail"+dirStr1+"/fail_"+dirStr;
               
                header.add("Tuple Emit");
                header.add("Tuple Count");
                header.add("Tuple Count/Sec");
                header.add("Latency");
                header.add("Duration");
                String str = Utils.join(header, ",");
                try {
                        
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
                        this.fileWriter.write(str+"\r\n");
                    } catch (IOException e) {
                       throw new RuntimeException("Error write file");
                    }
                this.collector = collector;
                this.pending = new HashMap();
                this.tuple_records = new ArrayList();
                this.emit_current = Long.valueOf(System.nanoTime());
                this.emit_last = Long.valueOf(System.nanoTime());
                _rand = new Random();
		numMessages = 2000000 ;//this.number_of_tuples;
		k = 10000;
		zipf = new ZipfDistribution(k,this.skew);
		randomStr = createDataSize(500);
		messageCount = 0;
/*     */   }
/*     */ 
/*     */   DataReaderSpoutZipF(String arg0, int spout, int bolt1, int bolt2, int is_aggregation, int number_tuples,int sleep_time,double skew,int ackers) {
/*  97 */     this.number_of_tuples = Integer.valueOf(number_tuples);
/*  98 */     this.arg0 = arg0;
/*  99 */     this.spout = Integer.valueOf(spout);
/* 100 */     this.bolt1 = Integer.valueOf(bolt1);
/* 101 */     this.bolt2 = Integer.valueOf(bolt2);
              //this.bolt3 = Integer.valueOf(bolt3);
/* 102 */     this.is_aggregator = Integer.valueOf(is_aggregation);
              this.iteration_count = Long.valueOf(sleep_time);
               this.ackers = Integer.valueOf(ackers);
              this.skew = skew;
/*     */   }
/*     */ 
            public void nextTuple()
            {
                 Utils.sleep(this.iteration_count);
                //messageCount=0; 
                Integer local_count=0;
                if(messageCount < numMessages ) {
                    while(local_count<this.number_of_tuples){
                        UUID msgId = UUID.randomUUID();
                        long num = zipf.sample();
                        long timeStamp = System.currentTimeMillis();
                        String sentence = String.valueOf(num);
                        //_collector.emit(new Values(sentence,timeStamp),num);
                        this.collector.emit(new Values(sentence,timeStamp), msgId);
                        Long t1 = System.nanoTime();
                        this.pending.put(msgId, t1);
                        tuple_emit = tuple_emit + 1; 
                        local_count++;
                        messageCount++;
                    }
                } 
            }
 
/*     */   public void declareOutputFields(OutputFieldsDeclarer declarer)
/*     */   {
/* 174 */     declarer.declare(new Fields("word","count"));
/*     */   }
/*     */ 
/*     */   public void ack(Object msgId)
/*     */   {
               //Utils.sleep(10L);
                Integer tuple_size = 0;
                Long t2 = System.nanoTime();
                
                Long end_time =t2 - this.start_time;
                end_time = end_time / 1000000000L;
                tuple_records.add(end_time);
                try {
                    t2 =  Long.valueOf(t2.longValue() - ((Long)this.pending.get(msgId)).longValue());
                      // pending_message = (Long)this.pending.get(msgId);
                } catch (ExceptionInInitializerError e) {
                      e.printStackTrace();
                }
                this.pending.remove(msgId);
                t2 = t2.longValue() / 1000000L;
                //if(t2<610){
                
                tuple_size = this.tuple_records.size();
                //if((end_time % 60) ==0   && end_time > 59L){
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
                //}
            }

/*     */   public void fail(Object msgId)
/*     */   {
/* 218 */     Long t2 = Long.valueOf(System.nanoTime());
/* 219 */     Long pending_message = Long.valueOf(0L);
/* 220 */     Integer tuple_size = Integer.valueOf(0);
/*     */ 
/* 222 */     Long end_time = Long.valueOf(t2.longValue() - this.start_time.longValue());
/* 223 */     end_time = Long.valueOf(end_time.longValue() / 1000000000L);
/* 224 */      Integer localInteger1 = tuple_failed; 
              Integer localInteger2 = tuple_failed = Integer.valueOf(tuple_failed.intValue() + 1);
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