/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package helper;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.file.GenericFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts an Excel file into a csv file
 * @author Arnaud Marchand
 */
public class ExcelConverter implements Processor
{
    static final Logger logger = LoggerFactory.getLogger("ExcelConverter");
    int                 MaxNumberOfSheetsToImport=1;
    String              FieldSeparator=",";
    String              Destination="";
    String              OutputEncoding="UTF8";

    public String getOutputEncoding()
    {
        return OutputEncoding;
    }

    public void setOutputEncoding(String OutputEncoding)
    {
        logger.info("Set Excel Encoding to:"+OutputEncoding);
        this.OutputEncoding = OutputEncoding;
    }
    
    public ExcelConverter()
    {
        logger.info("Excel converter created.");
    }

    public void setMaxNumberOfSheetsToImport(int MaxNumberOfSheetsToImport)
    {
       logger.info("Max number of sheets to import set to:"+MaxNumberOfSheetsToImport);
       this.MaxNumberOfSheetsToImport=MaxNumberOfSheetsToImport;
       
    }
    
    public void setFieldSeparator(String FieldSeparator)
    {
       logger.info("Field seperator set to:"+FieldSeparator);
       this.FieldSeparator=FieldSeparator;
       
    }
    
    public void setDestination(String Destination)
    {
       logger.info("Destination set to:"+Destination);
       this.Destination=Destination;
       
    }
    
    
    
    @Override
    public void process(Exchange exchng) throws Exception
    {
        logger.info("Converting excel.");
        Object res=exchng.getIn().getBody();
//        Workbook workbook = Workbook.getWorkbook();
        logger.info("Object:"+res);
        
        GenericFile genfile=null;
        byte[]  bytesin=null;
        
        if(res instanceof GenericFile)
            genfile=(GenericFile)res;
        else
        {
            logger.info("Body is byte[]");
            bytesin=(byte[])res;
        }
        
        
        try
        {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            //String encoding = "UTF8";
            OutputStreamWriter osw = new OutputStreamWriter(os, OutputEncoding);
            BufferedWriter bw = new BufferedWriter(osw);
            
            WorkbookSettings st=new WorkbookSettings();
            st.setEncoding("ISO-8859-1");
            
            Workbook w=null;
            if(genfile==null)
                w= Workbook.getWorkbook(new ByteArrayInputStream(bytesin));
            else
                w = Workbook.getWorkbook(new File(genfile.getAbsoluteFilePath()),st);
            
            for (int sheet = 0; sheet < w.getNumberOfSheets(); sheet++)
            {
                if(sheet==MaxNumberOfSheetsToImport)
                {
                    logger.info("Max number of sheets reached.");
                    break;
                }
                Sheet s = w.getSheet(sheet);

                //bw.write(s.getName());
                //bw.newLine();

                Cell[] row = null;

                // Gets the cells from sheet
                for (int i = 0 ; i < s.getRows() ; i++)
                {
                  row = s.getRow(i);

                  
                  
                  if (row.length > 0)
                  {
                    bw.write(row[0].getContents());
                    for (int j = 1; j < row.length; j++)
                    {
                      bw.write(FieldSeparator);
                      bw.write(row[j].getContents().replaceAll("'","<SQ>").replaceAll("\r","<CR>").replaceAll("\n","<LF>"));
                    }
                  }
                  bw.newLine();
                }
            }
            bw.flush();
            bw.close();
            String filename="NA";
            try
            {
                filename=genfile.getFileName();
            }
            catch(Exception e)
            {
                logger.info("File name not found.");
            }
            if(Destination.length()>0)
            {
                String rescsv=os.toString();
                String endpointstr=Destination;
                logger.info("Dispatch converted file:"+filename+" to:"+endpointstr);
                org.apache.camel.Endpoint endp=exchng.getContext().getEndpoint(endpointstr);

                Exchange exchange2 = endp.createExchange();
                exchange2.getIn().setBody(rescsv);
                exchange2.getIn().setHeader("FileName", filename);
                exchange2.getIn().setHeader("CamelFileName", filename);


                endp.createProducer().process(exchange2);
            }
            String resf=os.toString();
            exchng.getIn().setBody(resf);
        }
        catch(Exception e)
        {
            logger.error("Unable to open workbook. Ex="+e.getMessage(),e);
        }
    }

}
