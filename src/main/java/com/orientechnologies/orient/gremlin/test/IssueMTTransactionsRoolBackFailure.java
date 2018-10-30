/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.orientechnologies.orient.gremlin.test;

/**
 *
 * @author marko
 */
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.OEdge;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.concurrent.atomic.AtomicInteger;

public class IssueMTTransactionsRoolBackFailure {

    private static final String DB_NAME = "test";

    private static final String TEACHER_STUDENT_EDGE = "_TeacherStudentEdge";

    private static final String TEACHER_BLOCKING_EDGE = "_TeacherBlockingEdge";

    private static final String TEACHER = "Teacher";

    private static final String TEACHER_PROP_NAME = "name";

    private static final String TEACHER_PROP_ID = "teacherId";

    private static final String STUDENT = "Student";

    private static final String STUDENT_PROP_TEACHER_ID = "teacherId";

    private static final String STUDENT_PROP_IDENTIFIER = "studentId";

    private static final String BLOCKING = "Blocking";

    private static final String BLOCKING_PROP_KEY = "key";        
    
    private static AtomicInteger counter = new AtomicInteger(0);
    
//    public static void checkTeacherVertices(){
//      OrientDB orientDB = new OrientDB("remote:localhost", "root", "000000", OrientDBConfig.defaultConfig());
//      ODatabaseSession db = orientDB.open("test", "admin", "admin");
//      
//      OVertex blocking = db.getRecord(blockingId);
//      blocking.reload();
//      
//      Iterable<OEdge> edges = blocking.getEdges(ODirection.IN, "_TeacherBlockingEdge");
//      Iterator<OEdge> edgesIter = edges.iterator();
//      int counter = 0;
//      while (edgesIter.hasNext()){
//        Object obj = edgesIter.next();
//        counter++;
//      }
//      
////      for (OEdge edge : edges){
////        System.out.println("Edge id: " + edge.getIdentity());
////        OVertex tv = edge.getVertex(ODirection.OUT);
////        System.out.println("Vert id: " + tv.getIdentity());
////      }
////      
////      System.out.println("-----------------------------------------------------");
////      
////      Iterable<OVertex> iterable = blocking.getVertices(ODirection.IN, "_TeacherBlockingEdge");
////      for (OVertex teacher : iterable){
////        System.out.println("Teacher id: " + teacher.getIdentity());
////      }
//    }
    
    // The final result should probably be only one Teacher record in database,
    // even more than one, the bocking edges final state is not correct.
    public static void main(String[] args) throws InterruptedException {
        OrientDB orientDB = new OrientDB("embedded:d:/services/databases", "root", "000000", OrientDBConfig.defaultConfig());
        schema(orientDB);
        String teacherName = "Jim";
        // create one record first
        create(orientDB, UUID.randomUUID().toString(), teacherName, 0);

        // concurrent create records
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 3000; i++) {
          final int index = i;  
          executorService.execute(new Runnable() {
              @Override
              public void run() {
                create(orientDB, UUID.randomUUID().toString(), teacherName, index);
              }
            });                    
        }
        
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.HOURS);
        
        ODatabaseSession db = orientDB.open(DB_NAME, "admin", "admin");
        ORecordIteratorClass<ODocument> iter = db.browseClass(TEACHER);
        while (iter.hasNext()){
          ODocument doc = iter.next();
          doc.delete();
        }
        
        orientDB.close();                        
    }

    private static void create(OrientDB orientDB, String studentId, String teacherName, int index) {
        ODatabaseSession db = null;
        synchronized(orientDB){
          db = orientDB.open(DB_NAME, "admin", "admin");
        }
        try {
            db.begin();
            String teacherId = UUID.randomUUID().toString();

            OVertex blockingV = getBlockingVIfNoneCreate(teacherName, db);

            Iterable<OVertex> candidateVs = blockingV.getVertices(ODirection.IN, TEACHER_BLOCKING_EDGE);
            Set<OVertex> candidates = new HashSet<>();
            candidateVs.forEach(candidates::add);
            Set<OVertex> students = new HashSet<>();
            candidates.forEach(candidateV -> {
                Iterable<OVertex> studentVs = candidateV.getVertices(ODirection.OUT, TEACHER_STUDENT_EDGE);
                studentVs.forEach(students::add);
                candidateV.delete();                
            });

            OVertex teacherV = db.newVertex(TEACHER);
            teacherV.setProperty(TEACHER_PROP_NAME, teacherName);
            teacherV.setProperty(TEACHER_PROP_ID, teacherId); 
           teacherV = teacherV.save();

            OVertex studentV = db.newVertex(STUDENT);
            studentV.setProperty(STUDENT_PROP_IDENTIFIER, studentId);
            studentV.setProperty(STUDENT_PROP_TEACHER_ID, teacherId);
            studentV = studentV.save();
           
            OEdge e = db.newEdge(teacherV, studentV, TEACHER_STUDENT_EDGE);//  teacherV.addEdge(studentV, TEACHER_STUDENT_EDGE);
            e.save();

            final ODatabaseSession finalDb = db;
            final OVertex finalTeacher = teacherV;
            students.forEach(student -> {
                student.setProperty(STUDENT_PROP_TEACHER_ID, teacherId);
                OEdge ed = finalDb.newEdge(finalTeacher, student, TEACHER_STUDENT_EDGE); // teacherV.addEdge(student, TEACHER_STUDENT_EDGE);
                ed.save();
            });

            e = db.newEdge(teacherV, blockingV, TEACHER_BLOCKING_EDGE);// teacherV.addEdge(blockingV, TEACHER_BLOCKING_EDGE);
            e.save();
            //added by me
            studentV.save();
            teacherV.save(); 
            
            db.commit();
        }
        catch (Exception e) {
            e.printStackTrace();
            try{
              db.rollback();
            }
            catch (Exception ex){
              System.out.println("ROLL BACK EXCEPTION");
              ex.printStackTrace();
            }
        } finally {
          db.close();
          System.out.println("KRAJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJ" + counter.incrementAndGet());
        }
        
    }

    private synchronized static OVertex getBlockingVIfNoneCreate(String name, ODatabaseSession db) {
        ORecordIteratorClass<ODocument> rs =  db.browseClass(BLOCKING);
//        OResultSet rs = db.query("select from " + BLOCKING + " where " + BLOCKING_PROP_KEY + "=?", name);
        if (rs.hasNext()) {
//            return rs.next().getVertex().orElseThrow(() -> new RuntimeException());
            return rs.next().asVertex().orElse(null);
        } else {
            OVertex blockingVertex = db.newVertex(BLOCKING);
            blockingVertex.setProperty(BLOCKING_PROP_KEY, name);
            blockingVertex.save();            
            return blockingVertex;
        }
    }

    private static void schema(OrientDB orientDB) throws InterruptedException {
        orientDB.createIfNotExists(DB_NAME, ODatabaseType.PLOCAL);

        try (ODatabaseSession db = orientDB.open(DB_NAME, "admin", "admin")) {
            OClass teacher = db.getClass(TEACHER);
            if (teacher == null) {
                teacher = db.createVertexClass(TEACHER);
                teacher.createProperty(TEACHER_PROP_ID, OType.STRING).setNotNull(true);
                teacher.createProperty(TEACHER_PROP_NAME, OType.STRING).setNotNull(true);
            }
            OClass student = db.getClass(STUDENT);
            if (student == null) {
                student = db.createVertexClass(STUDENT);
                student.createProperty(STUDENT_PROP_TEACHER_ID, OType.STRING).setNotNull(true);
                student.createProperty(STUDENT_PROP_IDENTIFIER, OType.STRING).setNotNull(true);
            }
            OClass blocking = db.getClass(BLOCKING);
            if (blocking == null) {
                blocking = db.createVertexClass(BLOCKING);
                blocking.createProperty(BLOCKING_PROP_KEY, OType.STRING).setNotNull(true);
            }
            if (db.getClass(TEACHER_STUDENT_EDGE) == null) {
                db.createEdgeClass(TEACHER_STUDENT_EDGE);
            }
            if (db.getClass(TEACHER_BLOCKING_EDGE) == null) {
                db.createEdgeClass(TEACHER_BLOCKING_EDGE);
            }
        }

    }

}

