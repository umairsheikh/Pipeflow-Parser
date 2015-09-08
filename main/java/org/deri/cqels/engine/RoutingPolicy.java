package org.deri.cqels.engine;

import org.deri.cqels.data.Mapping;

import com.hp.hpl.jena.query.Query;

import java.util.ArrayList;
import java.util.HashMap;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.lang.cqels.OpStream;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
/** 
 * This interface contains set of methods which are behaviors of routing policy
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public interface RoutingPolicy 
{
	

	 
   
	public OpRouter next(OpRouter curRouter, Mapping mapping);
	public ContinuousSelect registerSelectQuery(Query query);
	public ContinuousConstruct registerConstructQuery(Query query);
}
