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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.io.FileReader;
 import java.io.FileNotFoundException;
/**
 *
 * @author Administrator
 */
public class FileReaderChild implements Serializable {

  private static final long serialVersionUID = -7012334600647556267L;

  public final String file;
  public final String ds;
  
  private final List<String> contents;
  private int index = 0;
  private int limit = 0;
private BufferedReader reader = null;
 private FileReader file_obj = null;
  public FileReaderChild(String file,String ds) {
//    if(ds=="didi"){
//        Random rand = new Random(); //instance of random class
//        int upperbound = 7;
//        int int_random = rand.nextInt(upperbound); 
//        file = file+ Integer.toString(int_random)+".txt";
//    }  
    this.file = file;
    this.ds = ds;
    if (this.file != null) {
        try{
            this.file_obj = new FileReader(this.file);
            this.reader = new BufferedReader(this.file_obj);
        } catch (FileNotFoundException e) {
                e.printStackTrace();
        }
    
      this.contents = readLines(this.reader);
      this.limit = contents.size();
    } else {
      throw new IllegalArgumentException("file name cannot be null");
    }
  }
public static List<String> readLines(BufferedReader reader) {
    List<String> lines = new ArrayList<String>();
    try {
    //  BufferedReader reader = new BufferedReader(new InputStreamReader(input));
      try {
        String line;
        while((line = reader.readLine()) != null) {
          lines.add(line);
          //break;
        }
      } catch (IOException e) {
        throw new RuntimeException("Reading file failed", e);
      } finally {
        reader.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error closing reader", e);
    }
    return lines;
  }

  public String nextLine() {
    if (index >= limit) {
	    index = 0;
	  }
    String line = contents.get(index);
    index++;
    return line;
  }
   public String getKey() {
    String key="";
    String []line_arr;
    String line = this.nextLine();
    
    switch(this.ds){
        case "tw":
        case "hash":
        case "didi":
            key = line;
            break;
        case "stock":
            line_arr = line.split("\t");
            key = line_arr[0];
            break;            
 
            
    }
    return key;
  }
  


}