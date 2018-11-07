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
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Random;

/**
 *
 * @author marko
 */
public class TestLuceneIndex {
  
  public static void main(String[] args){
    Random rand = new Random();
    OrientDB orientDB = new OrientDB("embedded:/data/services/databases", "root", "000000", OrientDBConfig.defaultConfig());    
    String databaseName = TestLuceneIndex.class.getSimpleName();
    orientDB.createIfNotExists(databaseName, ODatabaseType.PLOCAL);
    ODatabaseSession db = orientDB.open(databaseName, "admin", "admin");
    String url = db.getURL();
    String className = "ElementClass";
    OClass claz = db.createClassIfNotExist(className);
    String fieldName = "textField";
    OProperty prop = claz.createProperty(fieldName, OType.STRING);
//    db.execute("sql", "CREATE INDEX ElementClass.textField ON ElementClass(textField) FULLTEXT ENGINE LUCENE", (Object[])null);
    claz.createIndex(className + "." + fieldName, "FULLTEXT", null, null, "LUCENE", new String[]{fieldName});
    for (int i = 0; i < 3000; i++){
      ODocument doc = db.newInstance(className);
      String value = "value" + rand.nextInt();
      doc.field(fieldName, value, OType.STRING);
      doc.save();
    }
    db.close();
    orientDB.close();
  }
  
}
