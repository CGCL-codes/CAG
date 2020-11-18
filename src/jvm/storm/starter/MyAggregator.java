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
package storm.starter;
/*     */ 
import java.io.File;
/*     */ import java.io.FileWriter;
/*     */ import java.io.IOException;
/*     */ import java.io.PrintStream;
/*     */ import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
/*     */ import java.util.Map;
/*     */ import java.util.Map.Entry;
import java.util.Set;
/*     */ import org.apache.storm.task.OutputCollector;
/*     */ import org.apache.storm.task.TopologyContext;
/*     */ import org.apache.storm.topology.IRichBolt;
/*     */ import org.apache.storm.topology.OutputFieldsDeclarer;
/*     */ import org.apache.storm.tuple.Fields;
/*     */ import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
/*     */ import org.slf4j.Logger;
/*     */ import org.slf4j.LoggerFactory;
/*     */ 
/*     */ public class MyAggregator
/*     */   implements IRichBolt
/*     */ {
/*  28 */   private static final Logger LOG = LoggerFactory.getLogger(StatefulTopology.class);
/*     */   Map<String, Long> counterMap;
/*     */   private OutputCollector collector;
/*     */   private FileWriter fileWriter;
/*  34 */   String partStr1 = "/home/mudassar/output/";
/*     */   private Integer number_of_tuples;
/*     */   String arg0;
            Long start_time;
            boolean is_add_line=false;
/*     */   private Integer spout;
/*     */   private Integer bolt1;
/*     */   private Integer bolt2;
           
              private Integer ackers;
/*     */   private Integer is_aggregator;
/*     */   final Set<String> header = new LinkedHashSet<String>();

/*     */   public void prepare(Map conf, TopologyContext context, OutputCollector collector)
/*     */   {
                this.start_time = Long.valueOf(System.nanoTime());
                header.add("Word");
                header.add("Count");
              

/*  44 */     this.counterMap = new HashMap();
/*  45 */     this.collector = collector;
              String task_comp = "Comp="+context.getThisComponentId()+"task="+String.valueOf(context.getThisTaskId());
              
/*  46 */    Long ft = Long.valueOf(System.nanoTime());
/*  47 */     String partStr2 = ft.toString() + task_comp+ "_" + this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString()+ "_"  + ".csv";
             String dirStr = this.arg0 + "_" + this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_"  + "_" + this.number_of_tuples.toString()+ "_" + this.ackers.toString();
            String dirStr1 =this.spout.toString() + "_" + this.bolt1.toString() + "_" + this.bolt2.toString() + "_" + this.is_aggregator.toString() + "_" + this.number_of_tuples.toString()+ "_" + this.ackers.toString();
            String dirPath_agg = partStr1+"agg2_"+dirStr1+"/"+this.arg0;
             File dir_agg=new File(dirPath_agg); 
             if (!dir_agg.exists()) {
                   dir_agg.mkdirs();
             }
            try {
/*  49 */           this.fileWriter = new FileWriter(dirPath_agg + "/aggregator2_" + partStr2);
                    String str = Utils.join(header, ",");
                    this.fileWriter.write(str+"\r\n");
/*     */     } catch (IOException e) {
/*  51 */       throw new RuntimeException("Error write file");
/*     */     }
/*     */   }
/*     */ 
/*     */   MyAggregator(String arg0, int spout, int bolt1, int bolt2, int is_aggregation, int number_tuples,int ackers) {
/*  56 */     this.number_of_tuples = Integer.valueOf(number_tuples);
/*  57 */     this.arg0 = arg0;
/*  58 */     this.spout = Integer.valueOf(spout);
/*  59 */     this.bolt1 = Integer.valueOf(bolt1);
/*  60 */     this.bolt2 = Integer.valueOf(bolt2);
              //this.bolt3 = Integer.valueOf(bolt3);
                this.ackers = Integer.valueOf(ackers);
/*  61 */     this.is_aggregator = Integer.valueOf(is_aggregation);
/*     */   }
/*     */ 
/*     */   public void execute(Tuple tuple)
/*     */   {
                String word = tuple.getStringByField("word");
                Long count = tuple.getLongByField("count");
                if (count == null) {
                    count = 0L;
                }
                if (!this.counterMap.containsKey(word)) {
                    //+" task id="+tuple.getSourceTask()
                    counterMap.put(word, count);
                } else {
                   count = counterMap.get(word)+1;
                   counterMap.put(word, count);
                }
                this.collector.ack(tuple);
                List<String> line = new LinkedList<String>();
                line.add((String)word);
                line.add(count.toString());
                Long t2 = System.nanoTime();
                Long end_time =t2 - this.start_time;
                end_time = end_time / 1000000000L; 
                 line.add(end_time.toString());
                //if(end_time  >= 300 &&  end_time  < 301 ){
                    try {
                        String str = Utils.join(line, ",");
                        this.fileWriter.write(str+"\r\n");
                        is_add_line=true;
                        } catch (IOException e) {
                        e.printStackTrace();
                    }
               // }
 
            }
            public void cleanup()
           {
                List<String> line = new LinkedList<String>();
                Long t2 = System.nanoTime();
                Long end_time =t2 - this.start_time;
                for (Map.Entry entry : this.counterMap.entrySet()) {
                //System.out.println((String)entry.getKey() + " : " + entry.getValue());
                 try {
                     //for (String h : header) {
                     line.add((String)entry.getKey());
                     line.add((String)entry.getValue());
                      line.add(end_time.toString());
                     String str = Utils.join(line, ",");
                     this.fileWriter.write(str+"\r\n");
 /*  93 */               //this.fileWriter.write("Word:" + (String)entry.getKey() + " count:" + entry.getValue() + "\r\n");

 /*     */       } catch (IOException e) {
 /*  95 */         e.printStackTrace();
 /*     */       }
/*     */     }
/*     */   }
/*     */ 
/*     */   public void declareOutputFields(OutputFieldsDeclarer declarer)
/*     */   {
/* 102 */     declarer.declare(new Fields(new String[] { "word" }));
/*     */   }
/*     */ 
/*     */   public Map<String, Object> getComponentConfiguration()
/*     */   {
/* 107 */     return null;
/*     */   }
/*     */ }

/* Location:           D:\mudassar\phd\HUST\Prof Chen\experiments\storm\paritioning algo paper\files\storm-starter-1.2.2.jar
 * Qualified Name:     storm.starter.DataCounterBolt
 * JD-Core Version:    0.6.2
 */