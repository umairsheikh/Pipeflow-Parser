import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.IndexedTripleRouter;
import org.deri.cqels.engine.RoutingPolicy;
import org.deri.cqels.lang.cqels.OpStream;

import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.sun.tools.javac.util.List;

import java.util.regex.*;




public class RunQuery {

	private static final String HOME = "/Users/fuadshah/Desktop/GS/cqels_data";
 	
	public static void main(String[] args) 
    {
    	
            String floorPlanFilepath = "/Users/fuadshah/Desktop/GS/cqels_data/floorplan.rdf";
            String query2=  "PREFIX lv: <"+floorPlanFilepath+"> "+
            			"SELECT  ?person1 ?person2 "+ 
						"FROM NAMED <"+floorPlanFilepath+"> "+
						"WHERE { "+
						"GRAPH <"+floorPlanFilepath+"> "+ 
						"{?loc1 lv:connected ?loc2} "+
						"STREAM <http://deri.org/streams/rfid> [NOW] "+ 
						"{?person1 lv:detectedAt ?loc1}  "+ 
						"STREAM <http://deri.org/streams/rfid> [RANGE 3s] {?person2 lv:detectedAt ?loc2} } ";
            
 
            String queryString ="PREFIX lv: <http://deri.org/floorplan/> \n SELECT ?loc WHERE {"+
            					" STREAM <http://deri.org/streams/rfid> [TRIPLES 1] \n"+
            					" {?person lv:detectedAt ?loc} }";

            String queryString2 ="PREFIX lv: <http://deri.org/floorplan/> \n SELECT ?person WHERE {"+
		                         "STREAM <http://deri.org/streams/rfid> [TRIPLES 1] \n"+
		                         "{?person lv:detectedAt ?loc} }";
            
            String query1Ls =   "PREFIX sib:  <http://www.ins.cwi.nl/sib/vocabulary/> "+
								"PREFIX foaf: <http://xmlns.com/foaf/0.1/> "+
								"PREFIX sioc: <http://rdfs.org/sioc/ns#> "+
								"select ?p ?o "+
								"where{ "+
								 " STREAM <http://deri.org/poststream> [RANGE 1s] "+
								  "{<http://www.ins.cwi.nl/sib/user/u984> ?p ?o.} "+
								" }";
         
            String query2Ls =   "PREFIX sib:  <http://www.ins.cwi.nl/sib/vocabulary/> "+
			            		"PREFIX foaf: <http://xmlns.com/foaf/0.1/> "+
			            		"PREFIX sioc: <http://rdfs.org/sioc/ns#> "+
			            		"select ?post "+
			            		"where{ "+
			            		 " STREAM <http://deri.org/poststream> [RANGE 1s] "+
			            		" {?user sioc:creator_of ?post.} "+
			            		" ?user sioc:account_of <http://www.ins.cwi.nl/sib/person/p984>. "+
			            		" }";
            
            String query3Ls= "PREFIX foaf: <http://xmlns.com/foaf/0.1/> "+
					  "PREFIX sioc: <http://rdfs.org/sioc/ns#> "+
					  "select ?friend ?post "+
					  "where{ "+
					  " STREAM <http://deri.org/poststream> [RANGE 5s]{ "+
					  "?friend sioc:creator_of ?post. "+
					  "} "+
					  "?user foaf:knows ?friend. "+
					  "?user sioc:account_of <http://www.ins.cwi.nl/sib/person/p984>. }";
            
            
            String query4Ls =   " PREFIX foaf: <http://xmlns.com/foaf/0.1/> "+
								" PREFIX sib:  <http://www.ins.cwi.nl/sib/vocabulary/> "+
								" PREFIX sioc: <http://rdfs.org/sioc/ns#>"+
								" select ?post1 ?post2 ?tag "+
								" where{ "+ 
								"  STREAM <http://deri.org/poststream> [RANGE 15s] "+
								"  {?post1 sib:hashtag ?tag. ?user sioc:creator_of ?post1.} "+
								"  STREAM <http://deri.org/poststream> [RANGE 15s] "+
								"  {?post2 sib:hashtag ?tag. ?user sioc:creator_of ?post2.}"+
								"  FILTER(?post1 !=?post2) "+
								" }";
            
			String query5Ls = " PREFIX sib:  <http://www.ins.cwi.nl/sib/vocabulary/> "+
							  " PREFIX foaf: <http://xmlns.com/foaf/0.1/> "+
							  " select ?friend1 ?friend2 ?photo "+
							  " where{ " + 
							 " STREAM <http://deri.org/likedphotostream> [RANGE 100ms] "+
							 " {?friend2 sib:like ?photo. } "+
							 " STREAM <http://deri.org/photostream> [RANGE 1m ] "+
							 " {?photo sib:usertag ?friend1.} "+
							 " ?friend1 foaf:knows ?friend2. "+
							" }	";
            

			String query6Ls = 	" PREFIX sib:  <http://www.ins.cwi.nl/sib/vocabulary/> "+
					" PREFIX foaf: <http://xmlns.com/foaf/0.1/> "+
					" PREFIX sioc: <http://rdfs.org/sioc/ns#> "+
					" select ?user ?friend ?post ?channel "+
					" where{ "+
					"  STREAM <http://deri.org/poststream> [RANGE 1m] "+
					"  { ?channel sioc:container_of ?post. } "+
					"  STREAM <http://deri.org/likedpoststream> [RANGE 100ms] "+
					"  {?friend sib:like ?post.} "+
					"  ?user foaf:knows ?friend. "+
					"  ?user sioc:subscriber_of ?channel."+
					" } ";
           
            System.out.println("Startin  Query- From "+ HOME);
    	    final ExecContext context=new ExecContext(HOME, false,true);
            TextStream stream = new TextStream(context, "http://deri.org/streams/rfid", HOME+"/stream/rfid_1000.stream");
            ContinuousSelect selQuery=context.registerSelect(query4Ls); 
           
            CqelsParser cqelsParser = new CqelsParser();
            String inputFile = "rdfPostStream.csv";
    		String endPointConfigFile = "sibdataset.rdf";
           
            String pipeflowQuery =  cqelsParser.parse(query4Ls, context, selQuery, inputFile, endPointConfigFile);
            System.out.println("::::::::::---pipeflow query---:::::: \n"+pipeflowQuery);
             
    }
}
