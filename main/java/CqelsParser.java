import java.util.ArrayList;

import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.OpRouter;

import com.hp.hpl.jena.graph.query.Element;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Quad;

import java.util.regex.*;


//A main query algebric operator by ARQ Jena to be serialized into 
//our implementation and linked to other operators as instance to this class
class QuadPattern
{
	public String Subject;
	public String Object;
	public String Predicate;
	public String InputGraph;	

	QuadPattern()
	{
		this.Subject = null;
		this.Predicate = null;
		this.Object = null;
		this.InputGraph = null;
	}

}

//Pipeflow Input 
class InputOp
{
	//Keeps the link to the tripple to which 
	//Input quad points to
	QuadPattern quad;
	String inputOperatorText;
	String InputOperatorVariableName;
}

//Pipeflow filterOperator
class filterOp
{
	InputOp inOp;	
	String filterBy;
	String filterByValue;
	String filterCommandText;
	String filterVariableName;
	
	//Filter can be applied to multiple Quads of different tripple patterns
	ArrayList<QuadPattern> QuadList;

	filterOp()
	{
		QuadList = new ArrayList<QuadPattern>();
	}
}

//SparqlJoin in Pipeflow
class Sparql_Join
{
	String SparqlJoinVariableName;
	String SparqlJoinInput;
	String SparqlJoinOutputVariable;
	boolean isSparqlJoinExist;
	String sparqlJoinPipeflowQueryText= null;
	String EndPointConfigFile;
	 
}

//PipeFlow StreamWriter
class StreamWriter
{
	String streamWriterOperatorText = "$FinalOutput := stream_writer($input) using (stream = \"std::cout\");";
	String OutputVariable;
	String InputVariable;
	
	String GetStreamWriter(String OutputVariable, String InputVariable)
	{
		this.InputVariable = InputVariable;
		this.OutputVariable = OutputVariable;
		this.streamWriterOperatorText = this.streamWriterOperatorText.replace("$input",this.InputVariable);
		if(!OutputVariable.isEmpty())
		{
			this.streamWriterOperatorText = this.streamWriterOperatorText.replace("$FinalOutput",this.OutputVariable);
		}
		return this.streamWriterOperatorText;
	}
}

class PipeFlowProjectOperator 
{
	public QuadPattern quadPatternAs;
	public String PipeflowProjectVar;
	public String PipeFlowProjectOutString;		
	boolean isSparqlJoinExist;
}

class PipeflowJoinOperator extends PipeFlowProjectOperator
{
	public filterOp f1;
	public filterOp f2;

	public QuadPattern J1;
	public QuadPattern J2;
	public String PipeflowJoinString;
	public String PipeflowJoinVariable;
	public String JoinVariableName;



	public PipeflowJoinOperator()
	{
		J1= null;
		J2=null;
	}

	private String HashJoinSyntax = "$out := relation_hash_join($J1, $J2) "
			+ " on J1.x, J2.x, by"
			+ " J1.x == J2.x ";
	String Project = "$proj := project ($PipeflowJoinVariable) generate";

	void trimQuads()
	{
		J1.Subject = J1.Subject.trim();
		J1.Predicate = J1.Predicate.trim();
		J1.Object = J1.Object.trim();

		J2.Subject = J2.Subject.trim();
		J2.Predicate = J2.Predicate.trim();
		J2.Object = J2.Object.trim();

	}
	public String PipeFlowRelationalHashJoinOnMatchingAttributes()
	{
		trimQuads();

		//System.out.println("J1.Subject-"+J1.Subject);
		//System.out.println("J2.Object-"+J2.Object);
		//resolve J1x and J2x

		if(J1.Subject.equals(J2.Subject))
		{
			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Sub");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Sub");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);

			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			/*else if(J2.Subject.contains("?"))
				Project = Project + "Subj as" + J2.Subject+ ", ";*/
			if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";
			if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";

			return HashJoinSyntax;

		}
		if(J1.Subject.equals(J2.Predicate))
		{


			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Sub");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Pred");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);

			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			else if(J2.Subject.contains("?"))
				Project = Project + "Subj as" + J2.Subject+ ", ";
			/*if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";*/
			if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";

			return HashJoinSyntax;

		}
		if (J1.Subject.equals(J2.Object))
		{
			// System.out.println("J1.Subject-"+J1.Subject);
			//System.out.println("J2.Object-"+J2.Object);

			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Sub");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Obj");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);

			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			else if(J2.Subject.contains("?"))
				Project = Project + "Subj as" + J2.Subject+ ", ";
			if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";
			/*if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";*/

			return HashJoinSyntax;

		}

		if(J1.Object.equals(J2.Subject))
		{
			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Obj");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Sub");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);

			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			/*else if(J2.Subject.contains("?"))
					Project = Project + "Subj as" + J2.Subject+ ", ";*/
			if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";
			if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";

			return HashJoinSyntax;
		}
		if(J1.Object.equals(J2.Predicate))
		{

			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Obj");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Pred");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);


			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			else if(J2.Subject.contains("?"))
				Project = Project + "Subj as" + J2.Subject+ ", ";
			/*if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";*/
			if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";

			return HashJoinSyntax;

		}
		if (J1.Object.equals(J2.Object))
		{

			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Obj");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Obj");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);

			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			else if(J2.Subject.contains("?"))
				Project = Project + "Subj as" + J2.Subject+ ", ";
			if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";
			/*if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";*/

			return HashJoinSyntax;

		}

		if(J1.Predicate.equals(J2.Subject))
		{

			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Pred");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Sub");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);


			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			/*else if(J2.Subject.contains("?"))
				Project = Project + "Subj as" + J2.Subject+ ", ";*/
			if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";
			if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";

			return HashJoinSyntax;

		}
		if(J1.Predicate.equals(J2.Predicate))
		{

			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Pred");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Pred");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);

			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			else if(J2.Subject.contains("?"))
				Project = Project + "Subj as" + J2.Subject+ ", ";
			/*if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";*/
			if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";

			return HashJoinSyntax;


		}
		if (J1.Predicate.equals(J2.Object))
		{

			this.HashJoinSyntax=HashJoinSyntax.replace("J1.x", f1.filterVariableName+".Pred");
			this.HashJoinSyntax=HashJoinSyntax.replace("J2.x", f2.filterVariableName+".Obj");
			this.HashJoinSyntax=HashJoinSyntax.replace("$J1",f1.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$J2",f2.filterVariableName);
			this.HashJoinSyntax=HashJoinSyntax.replace("$out", JoinVariableName);

			if(J1.Subject.contains("?"))
				Project = Project + "Subj as" + J1.Subject + ", ";
			else if(J1.Predicate.contains("?"))
				Project = Project + "Pred as" + J1.Predicate + ", ";
			else if(J1.Object.contains("?"))
				Project = Project + "Obj as" + J1.Object+ ", ";
			else if(J2.Subject.contains("?"))
				Project = Project + "Subj as" + J2.Subject+ ", ";
			if(J2.Predicate.contains("?"))
				Project = Project + "Pred as" + J2.Predicate + ", ";
			/*if(J2.Object.contains("?"))
				Project = Project + "Obj as" + J2.Object + ", ";*/
			return HashJoinSyntax;

		}
		return HashJoinSyntax;
	}
	public String PipeFlowProjectOperator()
	{
		super.quadPatternAs.Subject = "SubjectAs";
		super.quadPatternAs.Predicate = "PredicateAs";
		super.quadPatternAs.Object = "ObjectAs";
		return "";
	}
	public void showJoinOperators()
	{
		if(J1.Subject.trim() == J2.Object.trim())
			System.out.println(" s=o");

		//if(J1.Subject.trim() == J2.Object.trim())
		{
			System.out.println(" - "+": "+J1.Subject+": "+J1.Predicate+": "+J1.Object);	
			System.out.println(" - "+J2.Subject+": "+J2.Predicate+": "+J2.Object);
		}
	}

}


public class CqelsParser 
{
	private ArrayList<InputOp> InputOperatorList;
	private ArrayList<filterOp> FilterOperatorList;
	private ArrayList<PipeflowJoinOperator> joinsList;	
	private ArrayList<Sparql_Join> sparqlJoinList;

	public  String EndPointConfigFile;	
	private boolean isSparqlJoinExist;
	private ArrayList<String> inputList;

	private ArrayList<String> staticSparqlQuery;
	private ArrayList<QuadPattern> QuadPatterns;
	private String SparqlJoinSelectParameter;
	private String PipeFlowQuery;
	private String CQELSQuery;
	private ExecContext context;
	private ContinuousSelect selQuery;

	public CqelsParser()
	{
		//List of Input Operators
		this.InputOperatorList = new ArrayList<InputOp>();
		//List of Filter Operators 
		this.FilterOperatorList = new ArrayList<filterOp>();
		//List of Spqrql Join Operators
		this.sparqlJoinList = new ArrayList<Sparql_Join>();
		//List of Quads with InputGraph as Input to stream link of the tripple
		this.QuadPatterns = new ArrayList<QuadPattern>();
		//EndPointConfig file for pipeflow
		this.EndPointConfigFile = null;
		//Flag for if SparqlJoin is needed
		this.isSparqlJoinExist = false;
		//List of Static query part as Tripple Set
		this.staticSparqlQuery = new ArrayList<String>();
		//Final Sparql to Pipeflow query
		this.PipeFlowQuery = null;
		//Sparql Query to be transformed to pipeflow
		this.CQELSQuery = null;

	}
	public String GetLogicPlan()
	{
		Op _op = selQuery.getOp();
		System.out.println("Complete query logical plan:: "+_op);   
		return _op.toString();
	}

	private void fetchQuadPatterns()
	{
		//make inputs
		for(Quad object: context.engine().getQuads())
		{	
			QuadPattern e = new QuadPattern();
			e.InputGraph = object.getGraph().toString().trim();
			e.Subject = object.getSubject().toString().trim();
			e.Object = object.getObject().toString().trim();
			e.Predicate = object.getPredicate().toString().trim();

			QuadPatterns.add(e);
			//System.out.println(e.InputGraph +e.Subject+e.Object+e.Predicate);
		}
	}

	private boolean ifInputGraphExistsinInputOpList(String inputGraph)
	{
		boolean ifExists = false;
		for(InputOp inOp : this.InputOperatorList)
		{
			if(inOp.quad.InputGraph.trim().equals(inputGraph))
			{
				return true;
			}
		}
		return ifExists;
	}

	private void makeInputs()
	{
		int i = 0;
		String inputText;
		for(Quad object: context.engine().getQuads())
		{	
			QuadPattern e = new QuadPattern();
			e.InputGraph = object.getGraph().toString().trim();
			e.Subject = object.getSubject().toString().trim();
			e.Object = object.getObject().toString().trim();
			e.Predicate = object.getPredicate().toString().trim();

			InputOp inOp = new InputOp();
			inOp.quad = e;

			//if(!this.InputOperatorList.contains(inOp.quad.InputGraph))
			if(!ifInputGraphExistsinInputOpList(e.InputGraph.trim()))
			{	
				i = i+1;
				inOp.InputOperatorVariableName= "$in"+String.valueOf(i);
				inOp.inputOperatorText  = " $in"+i+":= file_source() using (filename = "+inOp.quad.InputGraph+") with (Subj string, Pred string, Obj string);\n";
				this.PipeFlowQuery =  this.PipeFlowQuery + inOp.inputOperatorText;
				//System.out.println(inOp.inputOperatorText);
				this.InputOperatorList.add(inOp);
			}
		}
	}

	private boolean ifQuadPatternHasStatic(String inputGraph)
	{
		boolean ifExists = false;
		for(InputOp inOp : this.InputOperatorList)
		{
			if(inOp.quad.InputGraph.trim().equals(inputGraph))
			{
				return true;
			}
		}
		return ifExists;
	}
	private boolean filterAlreadyExist(String fby)
	{
		boolean ifExists = false;
		//fby = fby.trim();
		for(filterOp fOp : this.FilterOperatorList)
		{
			if(fOp.filterByValue.equals(fby))
			{
				return true;
			}
		}
		return ifExists;
	}
	private void makeFilters()
	{
		int i = 0;
		for(QuadPattern Q : QuadPatterns)
		{
			i = i +1;
			if(!Q.Subject.contains("?"))
			{	
				String filter = 	" $f := filter($in) by subj =="+"\""+Q.Subject+"\";\n";
				//System.out.println(filter);
				filterOp fOp = new filterOp();

				fOp.filterBy = "Subject";
				fOp.filterCommandText = filter;
				fOp.filterByValue = Q.Subject.trim();
				for(InputOp inOp : this.InputOperatorList )
				{ 
					if(inOp.quad.InputGraph.equals(Q.InputGraph))
					{ 	
						fOp.inOp = inOp;
					}
				}

				if(!filterAlreadyExist(fOp.filterByValue))
				{ 

					fOp.filterVariableName = "$f"+i;
					fOp.filterCommandText=fOp.filterCommandText.replace("$in", fOp.inOp.InputOperatorVariableName);
					fOp.filterCommandText=fOp.filterCommandText.replace("$f", fOp.filterVariableName);
					this.PipeFlowQuery = this.PipeFlowQuery + fOp.filterCommandText;
					fOp.QuadList.add(Q);
					this.FilterOperatorList.add(fOp);
					//System.out.println(fOp.filterCommandText);
				}
				else
				{
					fOp.QuadList.add(Q);
				} 
			}
			else if(!Q.Predicate.contains("?"))
			{	
				String filter = 	" $f := filter($in) by pred =="+"\""+Q.Predicate+"\";\n";
				
				filterOp fOp = new filterOp();
				fOp.filterBy = "Predicate";
				fOp.filterCommandText = filter;
				fOp.filterByValue = Q.Predicate.trim();
				for(InputOp inOp : this.InputOperatorList )
				{ 
					if(inOp.quad.InputGraph.equals(Q.InputGraph))
					{ 	
						fOp.inOp = inOp;
					}
				}
				if(!filterAlreadyExist(fOp.filterByValue))
				{ 

					fOp.filterVariableName = "$f"+i;
					fOp.filterCommandText=fOp.filterCommandText.replace("$in", fOp.inOp.InputOperatorVariableName);
					fOp.filterCommandText=fOp.filterCommandText.replace("$f", fOp.filterVariableName);
					this.PipeFlowQuery = this.PipeFlowQuery + fOp.filterCommandText;
					fOp.QuadList.add(Q);
					this.FilterOperatorList.add(fOp);
					System.out.println(fOp.filterCommandText);
				}	
				else
				{
					fOp.QuadList.add(Q);
				} 
			}
			else if(!Q.Object.contains("?"))
			{	
				String filter = 	" $f := filter($in) by obj =="+"\""+Q.Object+"\";\n";
				//System.out.println(filter);
				filterOp fOp = new filterOp();

				fOp.filterBy = "Object";
				fOp.filterCommandText = filter;
				fOp.filterByValue = Q.Object;
				for(InputOp inOp : this.InputOperatorList )
				{ 
					if(inOp.quad.InputGraph.equals(Q.InputGraph))
					{ 	
						fOp.inOp = inOp;
					}
				}
				if(!filterAlreadyExist(fOp.filterByValue))
				{ 

					fOp.filterVariableName = "$f"+i;
					fOp.filterCommandText=fOp.filterCommandText.replace("$in", fOp.inOp.InputOperatorVariableName);
					fOp.filterCommandText=fOp.filterCommandText.replace("$f", fOp.filterVariableName);
					this.PipeFlowQuery = this.PipeFlowQuery + fOp.filterCommandText;
					fOp.QuadList.add(Q);
					this.FilterOperatorList.add(fOp);
					System.out.println(fOp.filterCommandText);
				}
				else
				{
					fOp.QuadList.add(Q);
				} 
			}
		}
	}
	private  String  mapToPipeFlow(boolean isSparqlJoin, ArrayList<String> staticSparqlQuery, String SpqrqlJoinSelectParameter, String inputVar)
	{

		String sparql_join_pipeflow = null;
		String input,filterText;

		
		String pipeFlowQuery = "";
		if(this.isSparqlJoinExist)
		{				
			sparql_join_pipeflow = "$res:= sparql_join($input) using (endpoint = \"file:"+"\""+this.EndPointConfigFile+"\" ,\n";
			sparql_join_pipeflow = sparql_join_pipeflow + " query = \"SELECT "+SpqrqlJoinSelectParameter+ " WHERE  \n";

			if(staticSparqlQuery.size() < 2 )
				staticSparqlQuery.add(" ");

			String element = staticSparqlQuery.get(0) + " . \n";
			sparql_join_pipeflow = sparql_join_pipeflow + element;
			String element2 = staticSparqlQuery.get(1)  + " \");\n";
			sparql_join_pipeflow = sparql_join_pipeflow + element2;
			sparql_join_pipeflow = sparql_join_pipeflow.replace("$input", inputVar);
			
			Sparql_Join sparqlJoinOperator = new Sparql_Join();
			sparqlJoinOperator.EndPointConfigFile = this.EndPointConfigFile;
			sparqlJoinOperator.SparqlJoinInput = inputVar;
			String outputVariable = "$res" +String.valueOf(this.sparqlJoinList.size()+1);
			sparqlJoinOperator.SparqlJoinOutputVariable = outputVariable;
			sparql_join_pipeflow = sparql_join_pipeflow.replace("$res", outputVariable);
			sparqlJoinOperator.sparqlJoinPipeflowQueryText = sparql_join_pipeflow;
			this.sparqlJoinList.add(sparqlJoinOperator);
			
			pipeFlowQuery = pipeFlowQuery +  sparql_join_pipeflow;
			return pipeFlowQuery;

		}
		else
		{
			return pipeFlowQuery;
		}
	}
	private  boolean isSparqlJoinExists(String SparqlJoinQuery)
	{
		if(SparqlJoinQuery.contains("distinct") && SparqlJoinQuery.contains("bgp"))		
		{	
			return true;
		}
		return false;
	}
	private  ArrayList<String> extractStaticQueryPart (String queryStr)
	{		
		//Pattern p = Pattern.compile("\\((.*?)\\)",Pattern.DOTALL);
		Pattern p = Pattern.compile("((.*))");
		Matcher matcher = p.matcher(queryStr);
		ArrayList<String> triples = new ArrayList<String>();  
		while(matcher.find())
		{
			//int beginIndex = matcher.group(1).indexOf("bgp\n (triple");
			//int endIndex = matcher.group().length();
			String part = matcher.group(1).trim();
			if(part.contains("triple"))
			{
				String target = "(triple ";
				String replacement = " ";
				String trip=	part.replace(target, replacement);

				if(part.contains("(bgp"))
				{
					trip=	trip.replace("(bgp", replacement);
				}

				trip = trip.replace(")", " ");
				//String oldChar = "?";
				//String newChar = ":";
				//trip =  trip.replace(oldChar, newChar);
				//trip =  trip.replace(oldChar, newChar);
				//System.out.println("triples"+trip);
				triples.add(trip);
			}
		}
		return triples;
	}
	private  void extractJoins ()
	{	
		joinsList = new ArrayList<PipeflowJoinOperator>();
		PipeflowJoinOperator newJoinOperator = null;
		int joinIndex = 0;
		for (com.hp.hpl.jena.sparql.syntax.Element i : this.context.joinGraph.getElements()) 
		{ 
			//PipeflowJoinOperator newJoinOperator = new PipeflowJoinOperator();
			//System.out.println("Element -->"+i);
			if(i.toString().contains("GRAPH"))
			{     
				Pattern p = Pattern.compile("\\{(.*?)\\}",Pattern.DOTALL);
				Matcher matcher = p.matcher(i.toString());

				int numberOfJoins = 0;
				while(matcher.find())
				{	
					if(newJoinOperator ==null)
						newJoinOperator = new PipeflowJoinOperator();

					String part = matcher.group(0).trim();
					//System.out.println(" parts -"+ part);

					// split Graph into triples	
					Pattern p2 = Pattern.compile("[^\\r\n]*",Pattern.DOTALL);
					Matcher matcher2 = p2.matcher(part);
					ArrayList<String> nestedJoins = new ArrayList<String>();  
					while(matcher2.find())
					{	
						String part2 = matcher2.group(0).trim();
						//System.out.println("nested parts -"+ part2);

						if(!part2.isEmpty() &&  !part2.equals("}"))
						{
							//System.out.println("nested parts -"+ part2);
							String[] splited = part2.split("\\s+");
							for (int index=0; index < splited.length; index++)
							{

								if(!splited[index].equals("{") && !splited[index].equals("."))
								{
									QuadPattern quad = new QuadPattern();
									quad.Subject = splited[index];
									index = index + 1;
									quad.Predicate= splited[index];
									index = index + 1;
									quad.Object = splited[index];
									index = index +1;
									if(newJoinOperator.J1 == null)
									{	
										newJoinOperator.J1= quad;
									}
									else
									{	
										newJoinOperator.J2= quad;
									}
								}	
							}

						}
					}		//End of A Graph

					if(newJoinOperator.J2 != null)
					{
						joinIndex = joinIndex +1;
						newJoinOperator.JoinVariableName = "$out"+joinIndex;
						joinsList.add(newJoinOperator);
						newJoinOperator = null;
					}
				}   			
			}   	
		}

		//set source filters to operator joins
		int index = 0;
		for( PipeflowJoinOperator op : this.joinsList )
		{
			for(filterOp fOp :FilterOperatorList)
			{
				for(QuadPattern quad : fOp.QuadList)
				{	
					if(op.J1.Subject.equals(quad.Subject) || op.J1.Predicate.equals(quad.Predicate) || op.J1.Object.equals(quad.Object)  )
					{
						op.f1 = fOp;
					}
					if(op.J2.Subject.equals(quad.Subject) || op.J2.Predicate.equals(quad.Predicate) || op.J2.Object.equals(quad.Object)  )
					{
						op.f2 = fOp;
					}
				}
			}
			index = index +1;
			this.PipeFlowQuery = this.PipeFlowQuery + op.PipeFlowRelationalHashJoinOnMatchingAttributes() +"\n";
			//System.out.println( op.PipeFlowRelationalHashJoinOnMatchingAttributes());
			//op.showJoinOperators();
			//System.out.println( op.PipeFlowRelationalHashJoinOnMatchingAttributes());
			//System.out.println(op.PipeFlowRelationalHashJoinOnMatchingAttributes());
			//System.out.println(" - "+": "+op.J1.Subject+": "+op.J1.Predicate+": "+op.J1.Object);	
			//System.out.println(" - "+op.J2.Subject+": "+op.J2.Predicate+": "+op.J2.Object);	
		}





	}
	private void setStreamWriter()
	{
		StreamWriter sWriter = new StreamWriter();
		if(this.isSparqlJoinExist)
		{
			this.PipeFlowQuery = this.PipeFlowQuery + sWriter.GetStreamWriter("", this.sparqlJoinList.get(0).SparqlJoinOutputVariable);
			
		}
		else if(!this.joinsList.isEmpty())
		{
			this.PipeFlowQuery = this.PipeFlowQuery + sWriter.GetStreamWriter("", this.joinsList.get(this.joinsList.size()-1).JoinVariableName);
		}
		else if(!this.FilterOperatorList.isEmpty())
		{
			this.PipeFlowQuery = this.PipeFlowQuery + sWriter.GetStreamWriter("", this.FilterOperatorList.get(this.FilterOperatorList.size()-1).filterVariableName);
		}
		
		
	}
	
	
	private void initialize_parser()
	{
		fetchQuadPatterns();
		makeInputs();
		makeFilters();
		extractJoins();
		extract_Sparql_Join();
		setStreamWriter();
		
		
	}
	private void extract_Sparql_Join()
	{
		this.isSparqlJoinExist =  isSparqlJoinExists( context.router(1).getOp().toString());
		if(this.isSparqlJoinExist)
		{
			ArrayList<String> staticQueryPart =   extractStaticQueryPart(context.router(1).getOp().toString());
			String sparqlprojectionvariable = extractSparqlJoinProjectVariable(staticQueryPart.get(0));
			String InputVariable = "";
			for(PipeflowJoinOperator x: this.joinsList)
			{
				InputVariable = x.JoinVariableName;
			}
			if (InputVariable.isEmpty())
			{
				for(filterOp fOp :FilterOperatorList)
				{
					InputVariable = fOp.filterVariableName;
					
				}
			}
			
			this.PipeFlowQuery = this.PipeFlowQuery+  mapToPipeFlow(isSparqlJoinExist,  staticQueryPart, sparqlprojectionvariable,InputVariable);
		}
		else
		{
			ArrayList<String> staticQueryPart = new ArrayList<String>();
			this.PipeFlowQuery = this.PipeFlowQuery+   mapToPipeFlow(isSparqlJoinExist,staticQueryPart  , " ","");
		}
		//System.out.println("--------------------------------------");

	}
	private  String extractSparqlJoinProjectVariable(String triple)
	{
		String projectionVar = "";
		triple = triple.trim();
		int beginIndex = triple.indexOf("?");
		int endIndex = triple.indexOf("<");
		//System.out.println(String.valueOf(" begindex "+beginIndex));
		//System.out.println(String.valueOf(" endindex"+endIndex));


		projectionVar = triple.substring(beginIndex, endIndex-1);
		projectionVar.replace("?", ":");
		//System.out.println("projectionVar"+triple.substring(beginIndex, endIndex-1));
		return projectionVar;
	}
	public String  parse(String cqelsQuery, ExecContext execContext , ContinuousSelect selectQuery, String InputFile, String endPointConfigFile) 
	{    	
		this.selQuery = selectQuery;
		this.context = execContext;
		this.EndPointConfigFile = endPointConfigFile;
		this.initialize_parser();
		
		System.out.println(GetLogicPlan());
		
		return this.PipeFlowQuery;           
	}
}
