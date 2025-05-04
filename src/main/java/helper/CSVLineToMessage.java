/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.Object;
import java.util.ArrayList;
import java.util.List;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.file.GenericFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Unused
 * @deprecated
 * @author Arnaud Marchand
 */
public class CSVLineToMessage implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("ExcelConverter");

    String              []ColumnsArray=null;
    String              Columns="";
    String              Types="";
    String              Destination="";
    String              Delimiter=",";
    String              TargetTableName="";

    public String getColumns()
    {
        return Columns;
    }

    public void setColumns(String Columns)
    {
        this.Columns = Columns;
        ColumnsArray=this.Columns.split(",");
    }

    public String getTypes()
    {
        return Types;
    }

    public void setTypes(String Types)
    {
        this.Types = Types;
    }

    public String getDestination()
    {
        return Destination;
    }

    public void setDestination(String Destination)
    {
        this.Destination = Destination;
    }

    public String getDelimiter()
    {
        return Delimiter;
    }

    public void setDelimiter(String Delimiter)
    {
        this.Delimiter = Delimiter;
    }

    public String getTargetTableName()
    {
        return TargetTableName;
    }

    public void setTargetTableName(String TargetTableName)
    {
        this.TargetTableName = TargetTableName;
    }
    
                
    
    @Override
    public void process(Exchange exchng) throws Exception
    {
        //System.out.println("In Process>>>>>");

        
        Object[] resc=exchng.getIn().getBody(Object[].class);
        
        //java.util.ArrayList<Object> decodedcsv=(java.util.ArrayList<Object>)res;
        List<String> decodedline=new ArrayList<String>();
        for(Object str:resc)
            decodedline.add((String)str);
        //List<String> decodedline=decodeCSVLine(csv,Delimiter.charAt(0));
        StringBuilder builder=new StringBuilder();
        
        if((TargetTableName!=null)&&(TargetTableName.length()>0))
        {
            builder.append("TargetTableName=\""+TargetTableName+"\"");
        }
        if(decodedline.size()>0)
            builder.append(",");
            
        for(int i=0;i<decodedline.size();i++)
        {
            String colname="COL"+i;
            if(i<ColumnsArray.length)
            {
                colname=ColumnsArray[i];
            }
            builder.append(colname+"=\""+decodedline.get(i) +"\"");
            if(i!=decodedline.size()-1)
                builder.append(",");
        }
        
        String endpointstr=Destination;
        logger.info("Dispatch to:"+endpointstr);
        org.apache.camel.Endpoint endp=exchng.getContext().getEndpoint(endpointstr);

        Exchange exchange2 = endp.createExchange();
        exchange2.getIn().setBody(builder.toString());

        
//        Exchange exchange2 = endp.createExchange();
//        exchange2.getIn().setBody(rescsv);
//        exchange2.getIn().setHeader("FileName", filename);
//        exchange2.getIn().setHeader("CamelFileName", filename);


        endp.createProducer().process(exchange2);
    }
    
    public void printStrings(List<String> aList)
    {
        for(String str:aList)
        {
            System.out.println(">>>"+str);
        }
    }
    
    public List<String> decodeCSVLine(String aLine,char aDelimiter)
    {
        ArrayList<String> res=new ArrayList<String>();
        if((aLine==null)||(aLine.length()==0))
        {
            return res;
        }
        boolean isaquotedstring=false;
        int curpos=0;
        String curfield="";
        
        isaquotedstring=aLine.charAt(0)=='"';
        if(isaquotedstring)
            curpos=1;
            
        while(curpos<aLine.length())
        {
            if(isaquotedstring)
            {
                int nextpos=aLine.indexOf('"',curpos);
                if(nextpos<0)
                {
                    System.out.println("Unable to find end of field");
                    return res;
                }
                curfield=aLine.substring(curpos,nextpos);
                res.add(curfield);
                curpos=nextpos+2;    
                if(curpos+1<aLine.length())
                {
                    isaquotedstring=aLine.charAt(curpos)=='"';
                }
            }
            else
            {
                int nextpos=aLine.indexOf(aDelimiter,curpos);
                if(nextpos<0)
                {
                    System.out.println("Last field");
                    curfield=aLine.substring(curpos);
                    res.add(curfield);
                    return res;
                }
                curfield=aLine.substring(curpos,nextpos);
                res.add(curfield);
                curpos=nextpos+1;
                    
            }
        }
        return res;
    }
    
}
