package org.deri.cqels.engine;

import java.util.ArrayList;
import java.util.HashMap;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.lang.cqels.OpStream;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
/** 
 * This class is the base class to build an execution plan
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public abstract class RoutingPolicyBase implements RoutingPolicy 
{
    public ExecContext context;
    public HashMap<Integer,OpRouter> next;
    public LogicCompiler compiler;
    
	public ArrayList<ElementFilter> _filters = new ArrayList<ElementFilter>();
	public ArrayList<OpStream> _streamOps = new ArrayList<OpStream>();
    
    public RoutingPolicyBase(ExecContext ctx) 
    {
    	context = ctx;
    	next = new HashMap<Integer, OpRouter>();
    }
    public ExecContext getContext() 
    {
    	
    	return context;
    }
    
    public ArrayList<ElementFilter>   getFilters()
    {
    
    	return _filters;
    }
    
    public ArrayList<OpStream>  getstreamOps()
    {
    
    	return _streamOps;
    }
    
    
    
    protected abstract OpRouter generateRoutingPolicy(Query query);
    protected abstract OpRouter addRouter(OpRouter from, OpRouter newRouter);
}
