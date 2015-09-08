import java.lang.Object;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;

import java.util.Iterator;

import com.hp.hpl.jena.query.*;
import com.hp.hpl.jena.sparql.core.Var;

public class Query {

	private static final String HOME = "/Users/fuadshah/Desktop/GS/cqels_data";
    
    public static void main(String[] args) {
            
    	    
    	    final ExecContext context=new ExecContext(HOME, false);
            
            TextStream stream = new TextStream(context, "http://deri.org/streams/rfid", HOME+"/stream/rfid_1000.stream");
            //context.loadDefaultDataset("{DIRECTORY TO LOAD DEFAULT DATASET}");
            /*
             *      PREFIX lv: <http://deri.org/floorplan/>
                    SELECT ?loc  
                    WHERE {
                    STREAM <http://deri.org/streams/rfid> [TRIPLES 100] 
                    {?person lv:detectedAt ?loc} 
             */
            
            String queryString ="PREFIX lv: <http://deri.org/floorplan/> \n SELECT ?loc WHERE {"+
                            "STREAM <http://deri.org/streams/rfid> [TRIPLES 1] \n"+
                            "{?person lv:detectedAt ?loc} }";//put your query here


            String queryString2 ="PREFIX lv: <http://deri.org/floorplan/> \n SELECT ?person WHERE {"+
                            "STREAM <http://deri.org/streams/rfid> [TRIPLES 1] \n"+
                            "{?person lv:detectedAt ?loc} }";//put your query here
            
            for (int i = 0; i < 100; i++) 
            {
                    //With the select-type query
                   ContinuousSelect selQuery=context.registerSelect(queryString2);  
                   
                  selQuery.register(new ContinuousListener()
                  {
                        public void update(Mapping mapping)  
                        {
                           String result="";
                           for(Iterator<Var> vars=mapping.vars();vars.hasNext();)
                           //Use context.engine().decode(...) to decode the encoded value to RDF Node
                           result+=" "+ context.engine().decode(mapping.get(vars.next()));
                           System.out.println(result);
                         } 
                   });
                    //With the select-type query
     
            }
            
//           long start = System.currentTimeMillis();
            (new Thread(stream)).start();
//            (new Thread(stream)).run();
//           long time = System.currentTimeMillis() - start;
            
            System.out.println("Running Query");
    }
}
