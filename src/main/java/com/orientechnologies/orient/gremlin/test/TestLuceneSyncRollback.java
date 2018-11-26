package com.orientechnologies.orient.gremlin.test;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author marko
 */
public class TestLuceneSyncRollback {
  public static void main(String[] args){
    Random rand = new Random();
    String dbName = TestLuceneSync.class.getSimpleName();
    OrientDB orientDB = new OrientDB("embedded:D:/Services/databases", "root", "000000", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(dbName, ODatabaseType.PLOCAL);
    ODatabaseSession db = orientDB.open(dbName, "admin", "admin");
    String className = "ElementClass";
    OClass claz = db.createClassIfNotExist(className);
    String fieldName = "indexedField";
    claz.createProperty(fieldName, OType.STRING);
    claz.createProperty("dummyField", OType.STRING);
    String indexName = className + "." + fieldName;
    claz.createIndex(indexName, "FULLTEXT", null, null, "LUCENE", new String[] {fieldName});    
    db.begin();
    int num = 15000;
    String[] vals = new String[num];
    Set<String> indexKeys = new HashSet<>();
    for (int i = 0; i < num; i++){
      ODocument doc = db.newInstance(className);
      vals[i] = "val_" + rand.nextInt();
      indexKeys.add(vals[i]);
      doc.field(fieldName, vals[i]);
      doc.field("dummyField", "brmBrm_" + rand.nextInt());
      doc.save();
    }
    db.commit();
    
//    for (int i = 0; i < num; i++){
      OResultSet rs = db.query("SELECT FROM " + className + " WHERE SEARCH_CLASS('val_*') = true", (Object[])null);
      int count = 0;
      while (rs.hasNext()){
        rs.next();
        count++;
      }
      if (count > 0){
        System.out.println("found in index: " + count);
        System.out.println("Generated keys size: " + indexKeys.size());
      }
//    }
   
    db.close();
    orientDB.close();
  }
}
