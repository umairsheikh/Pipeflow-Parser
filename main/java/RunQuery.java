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

import java.io.*;
import java.util.regex.*;




public class RunQuery {

	private static final String HOME = "/Users/fuadshah/Desktop/GS/cqels_data";
 	private static boolean generateLogicalPlan= false;
 	private static  String QueryInputFilePath="path";
 	private static String QueryOutputFilePath;
 	
 	
	public static void main(String[] args) throws IOException 
    {
		 String floorPlanFilepath = "/Users/fuadshah/Desktop/GS/cqels_data/floorplan.rdf";
		 String line;
		 
		if(args.length == 0)
		{
			System.out.println("No parameters found");
			
		}
		else if(args.length ==1 )
		{
			if(args[0].contains("-h"))
			   System.out.println(" RunQuery.jar <Options> <InputFile> <OutputFile>");
			   System.out.println(" Options ");
			   System.out.println("        -h  Help runing the command ");
			   System.out.println("        -L  Output logical plan with pipeflow query ");
			   
		}
		else if(args.length >1)
		{
			if(args[0].equals("-L"))
			{
				try 
				{
				    FileReader fileReader =  new FileReader(args[1]);
		            BufferedReader bufferedReader = new BufferedReader(fileReader);
		            String QueryText = "";
		            while((line = bufferedReader.readLine()) != null) 
		            {
		                
		                QueryText = QueryText + line +"\n";
		            }   
		            System.out.println(QueryText);
		            bufferedReader.close();     
		            
		            System.out.println("Startin  Query- From "+ HOME);
		            final ExecContext context=new ExecContext(HOME, false,true);
		            TextStream stream = new TextStream(context, "http://deri.org/streams/rfid", HOME+"/stream/rfid_1000.stream");
		            ContinuousSelect selQuery=context.registerSelect(QueryText); 
		           
		            CqelsParser cqelsParser = new CqelsParser();
		            String rdfInputFile = "rdfPostStream.csv";
		    		String endPointConfigFile = "sibdataset.rdf";
		    		
		            String pipeflowQuery =  cqelsParser.parse(context, selQuery, rdfInputFile, endPointConfigFile);
		            System.out.println("::::::::::---pipeflow query---:::::: \n"+pipeflowQuery);
		        }
				catch(FileNotFoundException ex )
				{
		            System.out.println(
		                "Unable to open file '" + 
		                		QueryInputFilePath + "'");                
		        }
			}
			
		}
		System.exit(0);
             
    }
}
