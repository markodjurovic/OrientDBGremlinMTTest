/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.orientechnologies.orient.gremlin.test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 *
 * @author marko
 */
public class MainClass {
  public static void main(String[] args){

    OrientGraphFactory factory = new OrientGraphFactory("memory:testDB");
    ODatabaseDocument db = factory.getDatabase(true, true);
         
    db.execute("sql",
        "CREATE CLASS TestSequence EXTENDS V;\n"
            + " CREATE SEQUENCE TestSequenceIdSequence TYPE ORDERED;\n"
            + "CREATE PROPERTY TestSequence.mm LONG (MANDATORY TRUE, default \"sequence('TestSequenceIdSequence').next()\");\n");            
    
    
    OrientGraph graph = null;
    final int recCount = 50;
    final int threadCount = 200;
    int responseCode = 0;
    long startTime = System.currentTimeMillis();
    try{      
      Thread[] threads = new Thread[threadCount];
      for (int j = 0; j < threadCount; j++){
        final int index = j;
        Thread thread = new Thread(new Runnable() {
          @Override
          public void run() {
            OrientGraph graph = null;
            try{            
              if (index % 2 == 0){
                graph = factory.getNoTx();
              }
              else{
                graph = factory.getTx();
              }
              for (int i = 0; i < recCount; i++){            
                graph.addVertex("TestSequence");
              }
              if (index % 2 != 0){
                graph.commit();
              }
            }
            finally{
              if (graph != null){
                graph.close();
              }
            }
          }
        });
        threads[j] = thread;
        thread.start();
      }

      for (int i = 0; i < threads.length; i++){
        try{
          threads[i].join();
        }
        catch (InterruptedException exc){
          exc.printStackTrace();
        }
      }

      graph = factory.getNoTx();
      Iterator<Vertex> iter = graph.vertices();

      int counter = 0;
      Set<Long> vals = new HashSet<>();
      while (iter.hasNext()){
        Vertex v = iter.next();
        VertexProperty<Long> vp = v.property("mm");
        long a = vp.value();
        if (vals.contains(a)){
          System.out.println("ERROOOOOOR duplicate value");
          responseCode = 1;
          break;
        }
        System.out.println("Val: " + a);
        vals.add(a);
        counter++;
      }
      if (counter != threadCount * recCount){
        System.out.println("Inavlid number of records");
        responseCode = 1;
      }
      else{
        System.out.println("Succesfuly created: " + counter + " records");
      }
    }
    finally{
      long durration = System.currentTimeMillis() - startTime;
      System.out.println("Durration: " + durration);
      if (graph != null){
        graph.close();
      }
      db.activateOnCurrentThread();
      db.close();
    }
    System.exit(responseCode);
  }
}
