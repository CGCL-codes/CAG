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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.math3.util.FastMath;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.generated.GlobalStreamId;
import java.util.Random;
import storm.starter.util.HelperIPC;
/**
 *
 * @author Administrator
 */
public class IPCAGroupingV03 implements CustomStreamGrouping,Serializable{
    WorkerTopologyContext context;
    String partStr1 = "/home/mudassar/output/";
    private FileWriter fileWriter=null;
    private List<Integer> targetTasks;	
    
    Map<String,Integer> cli_list;
    Long start_time=0L;
    Long tuples =0L;
    int is_write=0;
    Long threshold=0L;
    List<Integer> ackerList = new ArrayList();
    List<Integer> counterList = new ArrayList();
    List<Integer> aggList = new ArrayList();
    private  HashMap<Integer,ArrayList<Integer>> NodeList=new HashMap<Integer,ArrayList<Integer>>();
    Integer supervisors;
//    static HashMap<Integer,HashMap<Integer,Integer>> CountAssigned = new HashMap<Integer,HashMap<Integer,Integer>>();
//   static HashMap<Integer,HashMap<Integer,Integer>> AggAssigned = new HashMap<Integer,HashMap<Integer,Integer>>();
    HashMap<Integer,Integer> TaskAssigned = new HashMap<Integer,Integer>();
   //static HashMap<Integer,HashMap<Integer,Integer>> AggAssigned = new HashMap<Integer,HashMap<Integer,Integer>>();
    
    Long total_task_assigned;
    List<Integer> TPWTasks= new ArrayList();
    Integer TPworkerPort; 
    Map<Integer,String> TPTTComp = new HashMap<Integer,String>();
    HelperIPC IPC_helper;// = new HelperIPC();
    Boolean is_new_assignment=false;
     HashMap<Integer,String> targetTaskStats=new HashMap<Integer,String>();
    IPCAGroupingV03( HashMap<String,Integer> cli_list,Long threshold){
        //this.isLog=false;
        this.total_task_assigned=0L;
        this.cli_list=cli_list;
        this.threshold=threshold;
        supervisors = cli_list.get("supervisors");
        
        for(int counter_task=1;counter_task<=cli_list.get("ackers");counter_task++){
            ackerList.add(counter_task);
            AddTasksToNode(counter_task);
            //writeTaskFile("Acker askId: " +counter_task+"\r\n");
        }
        
         for(int counter_task = 1;counter_task<=cli_list.get("agg_instances");counter_task++){
             Integer aggTask=cli_list.get("ackers")+counter_task;
            aggList.add(aggTask);
            AddTasksToNode(aggTask);
            //writeTaskFile("Counter askId: " +counter_task+"\r\n");
        }
        
        for(int counter_task = 1;counter_task<=cli_list.get("count_instances");counter_task++){
             Integer countTask=cli_list.get("ackers")+cli_list.get("agg_instances")+counter_task;
            counterList.add(countTask);
            AddTasksToNode(countTask);
            //writeTaskFile("Agg askId: " +counter_task+"\r\n");

        }
        
            
    }
        public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
            // TODO Auto-generated method stub
            this.start_time=System.nanoTime();
            this.context = context;
            this.targetTasks = targetTasks;
            TPWTasks = context.getThisWorkerTasks();
            TPworkerPort = context.getThisWorkerPort();
 
          
 


        }   
        public List<Integer> chooseTasks(int taskId, List<Object> values) {
             //showLog(cli_list.get("PreLevel"),taskId);
            List<Integer> boltIds = new ArrayList();
            Integer PreLevel = cli_list.get("PreLevel");
            Integer targetTaskId =0;
            targetTaskId = getTargetTaskId(taskId,PreLevel); 
            boltIds.add(targetTaskId);
            return boltIds;
	}
       
        public Integer getTargetTaskId(Integer TaskId,Integer PreLevel){
           Integer targetTaskId=0;
           Integer node_id =1;
            /*Start Get Node Id 
            TotalSupervisors=14
            spout id =160, 
            ( 160%14)=6, So node_id=6  */
            node_id =   getNodeId(TaskId);
            /*End Get Node Id */
           
            /*Start Get Target Task Id
            PreLevel = 0 means spout will assign to counter bolt
            PreLevel =1 means counter bolt will assign to agg bolt */
            ArrayList<Integer> taskList;
            taskList = NodeList.get(node_id);
            //Assigned spout to Count Bolt or Agg Bolt
            targetTaskId =  TaskAssignToCountorAggList(TaskId,taskList,PreLevel);
            // writeTargetTaskId( PreLevel,  TaskId, targetTaskId,"");
            if(targetTaskId == 0){
                targetTaskId=  getRandomTargetTaskId(PreLevel);
                //writeTargetTaskId( PreLevel,  TaskId, targetTaskId,"random");    
                    //writeTaskFile("Task id="+TaskId.toString()+"Pick from getRandomTargetTaskId(): Target task Id="+targetTaskId.toString()+"\r\n" );
            
            }
             /*End  Get Target Task Id*/
           
           return targetTaskId;
       }
        public Integer TaskAssignToCountorAggList(Integer TaskId,ArrayList<Integer> taskList,Integer PreLevel){
            Integer targetTaskId=0;
            
            //writeTaskFile("Prelevel="+PreLevel.toString()+"\r\n" );
            
            //Assigned spout to Count Bolt
            //1a: If task already assigned to TargetTask
            //Integer return_val= isCurrentTaskExist(TaskId,PreLevel);
            
            Integer task_tuple_count=null;
            Integer task_target_id=null;
             String[] target_str;
            String target_val = targetTaskStats.get(TaskId);
            if(target_val!=null){
                target_str =getStatsVal(target_val);
                if(target_str.length==2){
                    task_tuple_count=Integer.parseInt(target_str[0]);
                    task_target_id=Integer.parseInt(target_str[1]);
                    //writeTaskFile("1a: task_tuple_count="+task_tuple_count.toString()+"task_target_id="+task_target_id.toString()+"\r\n" );
                }
            }
           
            //Long return_val= targetTaskStats.get(TaskId);
            Boolean is_threshold_limit_reach= false;
            if(task_tuple_count !=null && task_tuple_count> 0){
                //1a:2 check for threshold
//                Integer tuples_count = getTuples( PreLevel,targetTaskId,TaskId);
               // Long tuples_count = targetTaskStats.get(TaskId);
                //writeTaskFile("return_value:1a:2: ="+task_tuple_count.toString()+"\r\n" );

                //writeTaskFile("Threshold1: Tuples="+tuples_count+"\r\n" );
                if( task_tuple_count !=null && task_tuple_count> this.threshold){
                    is_threshold_limit_reach=true; 
                    //writeTaskFile("Threshold2: Tuple limit reach: Tuples="+task_tuple_count+"\r\n" );
                }
            }
             //1a:3: get Already assigned targetTaskId if within threshold limit
            if(task_tuple_count !=null && task_tuple_count> 0 && is_threshold_limit_reach==false){
                    //targetTaskId  = getTargetTask(TaskId,PreLevel);
                    targetTaskId  = task_target_id;
                    //writeTaskFile("targetTaskId la:3: ="+targetTaskId.toString()+"\r\n" );
                    //Integer tuple_count = return_val[2];
                      //1a:4: add task to Count or Agg Assigned. list
                    updateTaskstoAssignedList(targetTaskId,TaskId);
                
            }else{
 
            //1b: Get Target Task Id of New Tasks
                ArrayList<Integer> tempTasks = new ArrayList<Integer>();
                for (int counter = 0; counter < taskList.size(); counter++) { 	
                    Integer tmp_target = taskList.get(counter);
                 //   writeTaskFile("tmp_target="+tmp_target.toString()+"\r\n" );
                    // 1b: 1 Get Temp Tasks and not assigned to existing task
                     Boolean isTaskExists= getTempTaskList( PreLevel,tmp_target);
                    // Boolean isTaskExists=TaskAssigned.containsKey(tmp_target);
                     if(isTaskExists== true) tempTasks.add(tmp_target);
                }    
                for (int counter = 0; counter < tempTasks.size(); counter++) { 	
                   // writeTaskFile("tempTasks data: Task="+tempTasks.get(counter).toString()+"\r\n" );
                }
                 //1b:2: Get Random Task Id from Temp  Task List
                Integer tmp_random_index = getRandomNumber(0, tempTasks.size()-1);
                //writeTaskFile("tmp_random_index="+tmp_random_index.toString()+"\r\n" );

                //1b:3: Assign random task from current node to target task 
                if(tempTasks.size()>0){
                    targetTaskId = tempTasks.get(tmp_random_index);
                   // writeTaskFile("targetTaskId="+targetTaskId.toString()+"\r\n" );
                }    
                //1b:4: add task to Count or Agg Assigned. list
                //writeTaskFile("targetTaskId lb:4: ="+targetTaskId.toString()+"\r\n" );

                updateTaskstoAssignedList(targetTaskId,TaskId);
            }//end 1b
 
                
                return targetTaskId;
        }
        
       
        public void updateTaskstoAssignedList( Integer targetTaskId,Integer TaskId){
 
                Integer tuple_count ;
                String target_val;
                target_val = targetTaskStats.get(TaskId);
                
                if(target_val== null){
                    target_val = "1_"+targetTaskId.toString();
                    targetTaskStats.put(TaskId,target_val );
                    //writeTaskFile("targetTaskStats: Add Task"+TaskId.toString()+ " val="+target_val.toString() +"\r\n" );
                }else {
                    //target_val = "1_"+targetTaskId.toString();
                    String[] str_arr =getStatsVal(target_val);
                    if(str_arr.length==2){
                        tuple_count = Integer.parseInt(str_arr[0])+1;
                        target_val = tuple_count.toString()+"_"+str_arr[1] ; 
                        targetTaskStats.put(TaskId, target_val);
                        // writeTaskFile("targetTaskStats: Add Task"+TaskId.toString()+ " val="+target_val +"\r\n" );
                    }
                }
                
                if(TaskAssigned.containsKey(targetTaskId)== false){
                   // writeTaskFile("TaskAssigned: Target Task"+targetTaskId.toString()+ " task id="+TaskId.toString() +"\r\n" );
                    TaskAssigned.put(targetTaskId, TaskId);
                }
                 
            //}
        }
       public Boolean getTempTaskList(Integer PreLevel, Integer tmp_target){
//             //ArrayList<Integer> tempTasks = new ArrayList<Integer>();
            Boolean isTaskExist = false;
            Boolean iskey=TaskAssigned.containsKey(tmp_target);
            if (iskey==false  ){
//                    // Assign task to temp Task list
                if(PreLevel==0){
                    if(counterList.contains(tmp_target)){
                        isTaskExist= true; 
                        //writeTaskFile("CountAssigned tempTaskItem="+tmp_target.toString()+ "key="+iskey.toString() +"\r\n" );
                    }
                }else{
                    if(aggList.contains(tmp_target)){
                        isTaskExist= true; 
                     //   writeTaskFile("AggAssigned tempTaskItem="+tmp_target.toString()+ "key="+iskey.toString() +"\r\n" );
                    }
                  
                }
            }
            
 
            return isTaskExist;
        
        }
        String[] getStatsVal(String target_val){
            
            String[] str_arr = target_val.split("_");
//            if(str_arr.length==2){
//                Integer tuple_count = Integer.parseInt(str_arr[0])+1;
//                target_val = tuple_count.toString()+"_"+targetTaskId.toString();
//            }
            return str_arr;
        }
        
        public Integer getRandomTargetTaskId(Integer PreLevel){
            Integer targetTaskId=0;
             if(PreLevel==1){
                 if(aggList.size()>0){
                  Integer tmp_ind = getRandomNumber(0, aggList.size()-1);
                  targetTaskId=   aggList.get(tmp_ind);
                 } else {
                  targetTaskId= 25;
                 }
             }else if(PreLevel==0){
                if(counterList.size()>0){ 
                Integer tmp_ind = getRandomNumber(0, counterList.size()-1);
                targetTaskId=   counterList.get(tmp_ind);
                 } else {
                  targetTaskId= 100;
                 }
            }
            //writeTaskFile("Random: PreLevel="+PreLevel.toString()+" target task id="+targetTaskId.toString());
           return targetTaskId;
        }
       
        public Integer getNodeId(Integer taskId){
            Integer node_id =1;
            node_id =  (taskId % supervisors);
            node_id =  (taskId % supervisors)== 0 ? supervisors: node_id;
            //writeTaskFile("getNodeId: taskId="+taskId.toString()+" node id="+node_id.toString());
            return node_id;
            
       }
    
        public void AddTasksToNode(Integer counter_task){
            ArrayList<Integer> taskList;
            Integer node_id=0 ;
            node_id =   getNodeId(counter_task);
             if(NodeList.containsKey(node_id)){
               taskList = NodeList.get(node_id);
            } else{
             taskList = new ArrayList();
            }
            if(taskList.contains(counter_task)==false){
                taskList.add(counter_task);
            }
            NodeList.put(node_id,taskList );
          
       }
       
        
        
        public  void createTaskFile(Integer PreLevel,Integer TaskId)
        { 
            if (this.fileWriter == null){ 
                createTaskFilesDetails(PreLevel,TaskId);
            } 
        } 
        public void createTaskFilesDetails(Integer PreLevel,Integer TaskId){
            Long ft = Long.valueOf(System.nanoTime());
                String partStr2 = ft.toString() + "_" + ".txt";
                String dirPath_th = partStr1+"choose"+PreLevel.toString();    
                try{
                     File dir_th=new File(dirPath_th); 
                    if (!dir_th.exists()) {
                           dir_th.mkdirs();
                    }
                    this.fileWriter = new FileWriter(dirPath_th + TaskId.toString() + partStr2);
                    // writeTaskFile("counterList: size="+counterList.size()+"\r\n");
                }catch (IOException e) {
                        throw new RuntimeException("Error write file");
                } 
        }   
        
        public  void writeTaskFile(String str){
            try
            {
                this.fileWriter.write( str);
            } catch (IOException e) {
                throw new RuntimeException("Error write file in write task file");
                //e.printStackTrace();
            }
        }
        public int getRandomNumber(int min, int max) {
                return (int) ((Math.random() * (max - min)) + min);
        }
      
        public void showLog(Integer PreLevel,Integer taskId){
            //if(isLogList.size()==0){
                createTaskFile(PreLevel,taskId);
               // PrintContextData();
               /* writeTaskFile("supervisors"+ cli_list.get("supervisors")+
                        "ackers"+ cli_list.get("ackers")+
                        "agg_instances"+ cli_list.get("agg_instances")+
                        "count_instances"+ cli_list.get("count_instances")+"\r\n");
                 PrintSBLists();
                //
                PrintNodeList();
              //  PrintTaskStats();*/
               
                

               
        }
        public void writeTargetTaskId(Integer PreLevel, Integer TaskId,Integer targetTaskId,String isRand){
            Boolean iskey=TaskAssigned.containsKey(targetTaskId);
//            if(PreLevel==0){
//                 iskey = TaskAssigned.containsKey(targetTaskId);
//            }else{
//                 iskey = AggAssigned.containsKey(targetTaskId);
//                 
//            }
            if(iskey==false ){
               writeTaskFile("Task id="+TaskId.toString()+" Pick from getTargetTaskId() "+isRand+": Target task Id="+targetTaskId.toString()+" Tuples=1"+"\r\n" );
            } else{
                //Integer tuples_count = getTuples( PreLevel,targetTaskId,TaskId);
                String target_val = targetTaskStats.get(TaskId);
               String[] target_str =getStatsVal(target_val);// targetTaskStats.get(TaskId);
               if(target_str.length==2){
                   Integer tuples_count =Integer.parseInt(target_str[0]);
//                    if(tuples_count%50000==0){
                        writeTaskFile("Task id="+TaskId.toString()+" Pick from getTargetTaskId() "+isRand+": Target task Id="+targetTaskId.toString()+" Tuples="+tuples_count+"\r\n" );
//                    } else{
//                         writeTaskFile("Task id="+TaskId.toString()+" Pick from getTargetTaskId(): Target task Id="+targetTaskId.toString()+" Tuples="+tuples_count+"\r\n" );
//
//                    }
               }
                    
            }
            
        }
        public void PrintNodeList(HashMap<Integer,ArrayList<Integer>> NodeList){
            ArrayList<Integer> taskList;
            Iterator iterator = NodeList.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry me2 = (Map.Entry) iterator.next();
                Integer node_id = (Integer)(me2.getKey());
                taskList = (ArrayList<Integer>)me2.getValue();
                for (int counter = 0; counter < taskList.size(); counter++) { 
                    writeTaskFile("Node Id="+node_id+" Task Id="+ taskList.get(counter)+"\r\n" );
                 }
                //System.out.println("Key: "+me2.getKey() + " & Value: " + me2.getValue());
            } 
        }
        public void PrintTaskStats( HashMap<Integer,Long> targetTaskStats){
             Iterator iterator = targetTaskStats.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry me2 = (Map.Entry) iterator.next();
                Integer ttask_id = (Integer)(me2.getKey());
                String val = (String)me2.getValue();
                   writeTaskFile("Task Statss: Task Id="+ttask_id+" val="+ val+"\r\n" );
                 
                //System.out.println("Key: "+me2.getKey() + " & Value: " + me2.getValue());
            } 
        }
        public void PrintSBLists(List<Integer> ackerList,List<Integer> aggList,List<Integer> counterList){
            
            writeTaskFile("Ack task size"+ ackerList.size()+"\r\n" );
            writeTaskFile("Agg task size"+ aggList.size()+"\r\n" );
            writeTaskFile("Counter task size"+ counterList.size()+"\r\n" );
            for (int counter = 0; counter < ackerList.size(); counter++) { 
                    writeTaskFile("Ack task Id"+ ackerList.get(counter)+"\r\n" );
            }
            
            for (int counter = 0; counter < aggList.size(); counter++) { 
                    writeTaskFile("Agg task Id"+ aggList.get(counter)+"\r\n" );
            }
            for (int counter = 0; counter < counterList.size(); counter++) { 
                    writeTaskFile("Counter task Id"+ counterList.get(counter)+"\r\n" );
            }
        } 
        public void PrintContextData(Integer TPworkerPort, List<Integer> TPWTasks,Map<Integer,String> TPTTComp){
               

           writeTaskFile("Work Port" + TPworkerPort.toString()+"+\r\n" );

           for (int counter = 0; counter < TPWTasks.size(); counter++) { 
                writeTaskFile("Work Task" + TPWTasks.get(counter)+"\r\n" );
            }
            Iterator iterator = TPTTComp.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry me2 = (Map.Entry) iterator.next();
                Integer key = (Integer)(me2.getKey());
                
                String val = (String)me2.getValue();
                writeTaskFile("key="+key+" val="+ val+"\r\n" );
               

                //System.out.println("Key: "+me2.getKey() + " & Value: " + me2.getValue());
            }  
        }
}
