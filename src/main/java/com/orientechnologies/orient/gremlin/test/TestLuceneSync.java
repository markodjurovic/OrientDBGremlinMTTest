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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Random;

/**
 *
 * @author mdjurovi
 */
public class TestLuceneSync {
  
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
    for (int i = 0; i < 3000; i++){
      ODocument doc = db.newInstance(className);
      String val = "val_" + rand.nextInt();
      doc.field(fieldName, val);
      doc.field("dummyField", "brmBrm_" + rand.nextInt());
      doc.save();
    }
    
    db.close();
    orientDB.close();
  }
  
}
