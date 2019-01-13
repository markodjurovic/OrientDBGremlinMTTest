/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.orientechnologies.orient.gremlin.test;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Iterator;
import java.util.Random;

/**
 *
 * @author mdjurovi
 */
public class RidBagPipeline {
  
  private static class DoJob extends Thread{
    private final ODatabaseSession db;
    private final OClass claz;
    private final OClass eClaz;
    
    public DoJob(OrientDB orientDB, OClass claz, OClass eClaz){
      this.db = orientDB.open(TestNullType.class.getSimpleName(), "admin", "admin"); 
      this.claz = claz;
      this.eClaz = eClaz;
    }
    
    @Override
    public void run(){
      db.activateOnCurrentThread();
      long startTime = System.currentTimeMillis();
      OVertex vertex = createBigVertex(db, claz);
      for (int i = 0; i < 15000; i++){
        OVertex secondEnd = createBigVertex(db, claz);
        OEdge edge = db.newEdge(vertex, secondEnd, eClaz);
        secondEnd.save();
        edge.save();              
      }
      vertex.save();    
      System.out.println("Durration: " + (System.currentTimeMillis() - startTime));
      db.close();
    }
    
  }
  
  
  private static OVertex createBigVertex(ODatabaseSession db, OClass claz){
    Random rand = new Random();
    OVertex v = db.newVertex(claz);
    for (int i = 0; i < 256; i++){
      v.setProperty(Integer.toString(i), rand.nextInt());
    }
    return v;
  }
  
  public static void main(String[] args){
    int threadCount = 10;
    
    OrientDB orientDB = new OrientDB("embedded:D:/Services/databases", "root", "000000", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(TestNullType.class.getSimpleName(), ODatabaseType.PLOCAL);
    ODatabaseSession db = orientDB.open(TestNullType.class.getSimpleName(), "admin", "admin");   
    OClass claz = db.createVertexClass("VertexClass");
    OClass eClaz = db.createEdgeClass("EdgeClass");    
    
    DoJob[] jobs = new DoJob[threadCount];
    for (int i = 0; i < jobs.length; i++){
      jobs[i] = new DoJob(orientDB, claz, eClaz);
    }
    for (int i = 0; i < jobs.length; i++){
      jobs[i].start();
    }
    for (int i = 0; i < jobs.length; i++){
      try{
        jobs[i].join();
      }
      catch (InterruptedException exc){
        exc.printStackTrace();
      }
    } 
    db.activateOnCurrentThread();
    db.close();
    
    //check number of edges
    db = orientDB.open(TestNullType.class.getSimpleName(), "admin", "admin");
    ORecordIteratorClass<ODocument> iter = db.browseClass("VertexClass");
    while (iter.hasNext()){
      OVertex v = (OVertex)iter.next();
      Iterator<OEdge> edgeIter = v.getEdges(ODirection.OUT).iterator();
      int counter = 0;
      while (edgeIter.hasNext()){
        OEdge edge = edgeIter.next();
        ++counter;
      }
      if (counter > 0){
        OVertex secondEnd = createBigVertex(db, claz);
        OEdge edge = db.newEdge(v, secondEnd, eClaz);
        secondEnd.save();
        edge.save();        
        v.save();
        System.out.println("Counter: " + counter);
      }
    }
    
    db.close();
  }
}
