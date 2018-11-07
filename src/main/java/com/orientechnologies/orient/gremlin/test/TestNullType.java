/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.orientechnologies.orient.gremlin.test;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 * @author mdjurovi
 */
public class TestNullType {
  
  public static void main(String[] args){
    Random rand = new Random();
    OrientDB orientDB = new OrientDB("embedded:/data/services/databases", "root", "000000", OrientDBConfig.defaultConfig());    
    orientDB.createIfNotExists(TestNullType.class.getSimpleName(), ODatabaseType.PLOCAL);
    ODatabaseSession db = orientDB.open(TestNullType.class.getSimpleName(), "admin", "admin");
    String url = db.getURL();
    OClass claz = db.createClassIfNotExist("ElementClass");
//    db.begin();    
    for (int i = 0; i < 3000; i++){
      OElement el = db.newElement(claz.getName());
      el.setProperty("simpleProp1", i + 136, OType.INTEGER);
      el.setProperty("simpleProp2", "No: " + i);
      List<Map<Integer, List<Map<Integer, List<String>>>>> nestedCollections = new ArrayList<>();
      for (int j = 0; j < 7; j++){
        Map<Integer, List<Map<Integer, List<String>>>> firstLevelMap = new HashMap<>();
        for (int k = 0; k < 6; k++){          
          List<Map<Integer, List<String>>> secondLevelList = new ArrayList<>();
          for (int l = 0; l < 5; l++){
            Map<Integer, List<String>> secondLevelMap = new HashMap<>();
            for (int m = 0; m < 4; m++){
              List<String> thirdLevelList = new ArrayList<>();
              for (int n = 0; n < 3; n++){
                String str = "SomeInfo:" + rand.nextInt();
                thirdLevelList.add(str);
              }              
              secondLevelMap.put(m, thirdLevelList);
            }            
            secondLevelList.add(secondLevelMap);
          }          
          firstLevelMap.put(k, secondLevelList);
        }
        nestedCollections.add(firstLevelMap);
      }
      el.setProperty("nested", nestedCollections, OType.EMBEDDEDLIST);
      System.out.println("Rec no: " + i);
      db.save(el);      
    }
//    db.commit();
    
    //now read and deserializes them
    ORecordIteratorClass<ODocument> iter = db.browseClass(claz.getName());
    while (iter.hasNext()){
      ODocument doc = iter.next();
      System.out.println("Just one filed: " + doc.getProperty("nested"));
    }
  }
  
}
