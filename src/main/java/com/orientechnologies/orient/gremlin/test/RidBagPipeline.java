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
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;

/**
 *
 * @author mdjurovi
 */
public class RidBagPipeline {
  public static void main(String[] args){
    OrientDB orientDB = new OrientDB("embedded:D:/Services/databases", "root", "000000", OrientDBConfig.defaultConfig());
    orientDB.createIfNotExists(TestNullType.class.getSimpleName(), ODatabaseType.PLOCAL);
    ODatabaseSession db = orientDB.open(TestNullType.class.getSimpleName(), "admin", "admin");
    OClass claz = db.createVertexClass("VertexClass");
    OClass eClaz = db.createEdgeClass("EdgeClass");
    OVertex vertex = db.newVertex(claz);
    for (int i = 0; i < 30000; i++){
      OVertex secondEnd = db.newVertex(claz);
      OEdge edge = db.newEdge(vertex, secondEnd, eClaz);
      secondEnd.save();
      edge.save();              
    }
    vertex.save();
    db.close();
  }
}
