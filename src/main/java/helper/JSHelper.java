package helper;

import camelworker.CamelWorker;
import com.equans.camel.component.pubsub.PubSubComponent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.PatternLayout;
import org.apache.camel.component.jms.JmsComponent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import jakarta.jms.JMSException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;

public class JSHelper implements Processor {

    static final Logger logger = LoggerFactory.getLogger("JSHelper");

    // We get the pubSub component here
    private PubSubComponent pubSub;

    // Request
    private int requestTimeout = 2000;
    
    // DB
    private java.sql.Connection sql;

    // Add commonServices Url
    private String commonServices = "http://127.0.0.1/CommonServices/";
    
    private String propertiesPath = "D:/ICTCS/APSB/Properties/";
    private final Map<String, Properties> propertiesFile;
    private final Map<String, Long> propertiesFileDate;
    
    private String appPropertiesPath = "D:/ICTCS/Screens/";
    private String screenLogPath = "D:/ICTCS/Screens/Logs";
    private final Map<String, Properties> appPropertiesFile;
    private final Map<String, Long> appPropertiesFileDate;  
    
    private final Map<String,Exchange> threadRequestMap=new HashMap<String,Exchange>();
    private final Map<String,org.apache.log4j.Logger> screenToLoggerMap=new HashMap<String,org.apache.log4j.Logger>();
    private final Map<String, JsonObject> contexts=new HashMap<String,JsonObject>();
    
    // To saved all statuses
    private static final HashMap<String, HashMap<String, Object>> statuses = new HashMap<String, HashMap<String, Object>>();
    // Last public for each station
    private static final HashMap<String, DateTime> lastPublishTime = new HashMap<String, DateTime>();
    // Last get status for each station
    private static final HashMap<String, DateTime> lastGetStatusTime = new HashMap<String, DateTime>();
    // Polled Events
    private static final HashMap<String, List<JSONObject>> polledEvents = new HashMap<String, List<JSONObject>>();
    // List of Poller id by queue name for handling multiple polling at same time
    public static final HashMap<String, ArrayList<Pair<String, Long>>> pollerIdsByQueue = new HashMap<String, ArrayList<Pair<String, Long>>>();
    
    
    private final StatusManager statusManager=new StatusManager();
    
    private final Map<String, CachePair> cache;
    Gson gson = new Gson();
    private Date nextCheck=new Date((Calendar.getInstance().getTimeInMillis()+(long)(60000)));
    
    private BasicDataSource basicDS=null;
        
    public JSHelper() throws JMSException {
        this.cache = new HashMap<String, CachePair>();
        this.propertiesFile = new HashMap<String, Properties>();
        this.propertiesFileDate = new HashMap<String, Long>();
        this.appPropertiesFile = new HashMap<String, Properties>();
        this.appPropertiesFileDate = new HashMap<String, Long>();
    }

    public void setStatusManagerException(String StatusManagerException)
    {
        logger.info("Setting Status Manager Exception to:"+StatusManagerException);
        statusManager.ExceptionRegex = StatusManagerException;
    }
    public Object getBean(String beanName)
    {
        ApplicationContext context=CamelWorker.context;
        return context.getBean(beanName);
    }
    
    public String getScreenLogPath()
    {
        return screenLogPath;
    }

    public void setScreenLogPath(String screenLogPath)
    {
        this.screenLogPath = screenLogPath;
    }
    
    private org.apache.log4j.Logger createOneLogger(String anApp,String anID)
    {
        String cleanname=(anApp+"_"+anID).replaceAll(" ", "");
        org.apache.log4j.Logger logger2=org.apache.log4j.Logger.getLogger(cleanname);
        String logFileName = cleanname+".log";

        logger.info("Creating log:"+this.screenLogPath+File.separator+anApp+File.separator+anID+File.separator+logFileName);

        DailyRollingFileAppender rfappender = new DailyRollingFileAppender();
        rfappender.setName("WORKLOG");
        try
        {
            rfappender.setFile(this.screenLogPath+File.separator+anApp+File.separator+anID+File.separator+logFileName, true, false,0 );
        }
        catch (IOException ex)
        {
            java.util.logging.Logger.getLogger(JSHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        Layout layout = new PatternLayout("%d %-5p - %m%n");
        rfappender.setLayout(layout);
        rfappender.activateOptions();
        logger2.addAppender(rfappender);
        logger2.info( "#####################################");
        logger2.info( cleanname+ " initialized.");
        return logger2;
    }
    
    public void setExchange(Exchange e)
    {
        logger.debug("Setting exchange:"+Thread.currentThread().getId());
        
        
        if(e.getIn()!=null)
        {
            String EventApp=(String)e.getIn().getHeader("EventApp");
            String EventAppState=(String)e.getIn().getHeader("EventAppState");
            String EventAppId=(String)e.getIn().getHeader("EventAppId");

            if((EventApp!=null)&&(!EventApp.isEmpty())
                    &&(EventAppState!=null)&&(!EventAppState.isEmpty())
                    &&(EventAppId!=null)&&(!EventAppId.isEmpty()))
            {

                synchronized(threadRequestMap)
                {
                    if(!screenToLoggerMap.containsKey(EventApp+"_"+EventAppId))
                    {
                        screenToLoggerMap.put(EventApp+"_"+EventAppId, 
                                createOneLogger(EventApp,EventAppId)
                                );
                    }
                    screenToLoggerMap.get(EventApp+"_"+EventAppId).info(">>>>>>> Entering state:"+EventAppState);
                    
//                    logger.info("Cache:"+(this.cache==null?"YES":"NO"));
                    
                    if(this.cache!=null)
                    {
                        try
                        {
//                            logger.info("EventAppState:"+EventAppState);

                            if((EventAppState.toLowerCase().indexOf("reconnected")!=0)
                                &&(e.getIn().getBody().toString().indexOf("ERROR")!=0))
                            {
//                                logger.info("Pushing into cache:"+EventAppState);

//                                logger.info("Key:"+EventApp+"_"+EventAppId);
                                this.cache(EventApp.toUpperCase()+"_"+EventAppId.toUpperCase()+"_LastKnownGoodState",EventAppState);
                                this.cache(EventApp.toUpperCase()+"_"+EventAppId.toUpperCase()+"_LastKnownGoodBody",e.getIn().getBody().toString());
                                logger.debug("LastKnown state="+EventAppState);
                                logger.debug("LastKnown body="+e.getIn().getBody().toString());
                            }
                        }
                        catch(Exception e2)
                        {
                            logger.error("Unable to set lastknow good."+e2.getMessage());
                        }
                    }
                    threadRequestMap.put(""+Thread.currentThread().getId(), e);
                }
            }
            
            try 
            {
//                logger.info("BODY="+EventApp+"_"+EventAppId);
                synchronized(contexts)
                {                    
                    JsonObject orgcontext= contexts.get(EventApp.toUpperCase()+"_"+EventAppId.toUpperCase());
                    if(orgcontext!=null)
                    {
//                        logger.info("TOTO==>"+gson.toJson(orgcontext));
                        JsonObject jobject = new JsonParser().parse(e.getIn().getBody().toString()).getAsJsonObject();
                        logger.debug("Injecting context:"+EventApp+"_"+EventAppId+" New context:"+gson.toJson(orgcontext));
                        jobject.remove("context");
                        logger.debug("Context Removed:"+EventApp+"_"+EventAppId+" New context:"+gson.toJson(jobject));
                        jobject.add("context", orgcontext);
                        jobject.add("context2", orgcontext);
                        logger.debug("Context Added:"+EventApp+"_"+EventAppId+" New context:"+gson.toJson(jobject));
                        e.getIn().setBody(gson.toJson(jobject));
//                        logger.info("TOTO2==>"+gson.toJson(jobject));
                    }
                    else
                        logger.info("Context is null.");
                }   
            } 
            catch (Exception excp) 
            {
                logger.error("Error when parsing message, error is " + excp,excp);
            }
            
        }
        
    }
    
    public void cleanExchange(Exchange e)
    {
        logger.debug("Cleaning exchange:"+Thread.currentThread().getId());
        if(e.getIn()!=null)
        {
            List<String> keys=new ArrayList<String>();
            for(String key:e.getIn().getHeaders().keySet())
            {
                keys.add(key);
            }
            for(String key:keys)
                e.getIn().removeHeader(key);
        }
    }
    
    public void resetExchange(Exchange e)
    {
        logger.debug("Removing exchange:"+Thread.currentThread().getId());
        
        synchronized(threadRequestMap)
        {
            threadRequestMap.remove(""+Thread.currentThread().getId());
        }
        
        try 
        {
//            logger.info("BODY="+e.getIn().getBody().toString());
            JsonObject jobject = new JsonParser().parse(e.getIn().getBody().toString()).getAsJsonObject();
            JsonPrimitive app = jobject.getAsJsonPrimitive("app");
            JsonPrimitive id = jobject.getAsJsonPrimitive("id");
//            logger.info("APP="+app.getAsString());
//            logger.info("ID="+id.getAsString());

            JsonObject context=null;
            try
            {
                context=jobject.getAsJsonObject("context");
            }
            catch(Exception e2)
            {
                logger.error("Unable to retrieve context. Reinializing it. Ex="+e2.getMessage());
                context=new JsonObject();
            }
            
            String EventAppState=(String)e.getIn().getHeader("EventAppState");
            
            if(EventAppState!=null)
            {
                logger.debug("State:"+EventAppState);
            }
            
            if((EventAppState!=null)&&(EventAppState.toLowerCase().indexOf("reconnected")!=0))
            {
                synchronized(contexts)
                {
                    contexts.remove(app.getAsString().toUpperCase()+"_"+id.getAsString().toUpperCase());
                    logger.debug("Pushing Context:"+app.getAsString().toUpperCase()+"_"+id.getAsString().toUpperCase()+" CONTEXT:"+gson.toJson(context));
                    contexts.put(app.getAsString().toUpperCase()+"_"+id.getAsString().toUpperCase(),context);
                }
            }
        } 
        catch (Exception excp) 
        {
            logger.error("Error when parsing message, error is " + excp,excp);
        }
        
        
    }
    
    public void setJms(JmsComponent comp) throws JMSException {
        logger.error("[JSHelper] using a legacy Jms component is deprecated");
        throw new jakarta.jms.IllegalStateException("JsHelper with JmsComponent is deprecated, please migrate to use the PubSub component");
    }

    public void setPubSub(PubSubComponent comp) throws JMSException {
        logger.info("[JSHelper] Setting PubSub connection");
        this.pubSub = comp;
    }

    public void setCommonServices(String url) {
        logger.info("[JSHelper] Setting CommonServices to " + url);
        if (!url.endsWith("/")) {
            url += "/";
        }
        this.commonServices = url;
    }

    public void setDataSource(BasicDataSource comp) throws SQLException {
        logger.info("[JSHelper] Setting DataSource ");
        this.basicDS=comp;           
        this.sql=null;
    }
        
    public void createDBConnection()
    {
        if(sql!=null)
            return;
        try
        {
            logger.info("[JSHelper] CreateDB Connection ");
            this.sql = this.basicDS.getConnection();
        }
        catch(Exception e)
        {
            logger.error("[JSHelper] Unable to create DB Connection. Ex="+e.getMessage(),e);
        }
    }
    
    public void setProperties(String path) {
        logger.info("[JSHelper] Setting Properties ");
        this.propertiesPath = path;
    }
    
    public void setAppProperties(String path) {
        logger.info("[JSHelper] Setting App Properties ");
        this.appPropertiesPath = path;
    }
     
    public void setRequestTimeout(int requestTimeout) {
        logger.info("[JSHelper] Setting requestTimeout to " + requestTimeout + " ms");
        this.requestTimeout = requestTimeout;
    }

    
    

    
    
    /**
     * **** publish methods *****
     */
    public void publish(String topic, String msg) throws JMSException {
        this.pubSub.getPubSub().publishMessageOnTopic(topic, msg);
    }
    
    /**
     * **** publish methods *****
     */
    public void publishOnQueue(String topic, String msg) throws JMSException {
        this.pubSub.getPubSub().publishMessageOnQueue(topic, msg);
    }

    /**
     * **** publish methods *****
     */
    public void publish(String topic, String msg,Map<String, String> properties) throws JMSException {
        this.pubSub.getPubSub().publishMessageOnTopic(topic, msg,properties);
    }
    
    /**
     * **** publish methods *****
     */
    public void publishOnQueue(String topic, String msg,Map<String, String> properties) throws JMSException {
        this.pubSub.getPubSub().publishMessageOnQueue(topic, msg,properties);
    }
    
    /**
     * **** DB Method *****
     */
    public String query(String query) throws Exception {
        
        String json = "{}";
        createDBConnection();
        Statement sta = null;
        try {
            sta = this.sql.createStatement();
            sta.execute(query);
            ResultSet res = null;

            try {
                res = sta.getResultSet();
                json = this.convertResultSetIntoJSON(res);
            } catch (Exception e) {
                logger.error("[query] Unable to read result set. Ex=" + e.getMessage());
            } finally {
                try {
                    res.close();
                } catch (Exception e2) {
                    logger.error("[query] Unable to close result set. Ex=" + e2.getMessage());
                }
            }
        } finally {
            try {
                sta.close();
            } catch (Exception e3) {
                logger.error("[query] Unable to close statement set. Ex=" + e3.getMessage());
                this.sql=null;
            }
        }
        return json;
    }
    
    /**
     * **** DB Method *****
     */
    public long execute(String query) throws Exception {
        long numberofupdate=0;
        createDBConnection();
        Statement sta = null;
        try {
            sta = this.sql.createStatement();
            numberofupdate=sta.executeUpdate(query);
        } finally {
            try {
                sta.close();
            } catch (Exception e3) {
                logger.error("[query] Unable to close statement set. Ex=" + e3.getMessage());
                this.sql=null;
            }
        }
        return numberofupdate;
    }
    
    
    private String convertResultSetIntoJSON(ResultSet resultSet) throws Exception {
        JSONArray jsonArray = new JSONArray();
        while (resultSet.next()) {
            int total_rows = resultSet.getMetaData().getColumnCount();
            JSONObject obj = new JSONObject();
            for (int i = 0; i < total_rows; i++) {
                String columnName = resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase();
                Object columnValue = resultSet.getObject(i + 1);
                if (columnValue == null) {
                    columnValue = "null";
                }
                if (columnValue instanceof Date) {
                    
                    
                    if(!obj.containsKey(columnName+"_TS"))
                    {
                        obj.put(columnName+"_TS", ((Date)columnValue).getTime());        
                        columnValue = columnValue.toString();
                    }

                }
                if (columnValue instanceof Time) {
                    
                    if(!obj.containsKey(columnName+"_TS"))
                    {
                        obj.put(columnName+"_TS", ((Time)columnValue).getTime());        
                        columnValue = columnValue.toString();
                    }
                }
                if (columnValue instanceof Timestamp) {
                    
                  
                    if(!obj.containsKey(columnName+"_TS"))
                    {
                        obj.put(columnName+"_TS", ((Timestamp)columnValue).getTime());                                                
                        columnValue = columnValue.toString();
                    }
                }
                obj.put(columnName, columnValue);
            }
            jsonArray.add(obj);
        }
        return jsonArray.toJSONString();
    }

    public void checkCache()
    {
        long now=Calendar.getInstance().getTimeInMillis();
        if(now>nextCheck.getTime())
        {
            logger.info("Cleaning cache.");
            try
            {
                nextCheck=new Date(now+(long)(60000*10));

                ArrayList<String> keys=new ArrayList<String>();
                for ( Map.Entry<String, CachePair> entry : cache.entrySet() ) 
                {
                    String key = entry.getKey();
                    CachePair value = entry.getValue();
                    if(value.removeTime!=null)
                    {
                        if(value.removeTime.getTime()<now)
                        {
                            logger.info("Removing key:"+key);
                            keys.add(key);
                        }
                    }
                }
                if(keys.size()>0)
                {
                    logger.info("Removing "+keys.size()+" key(s).");
                    for(String key:keys)
                    {
                        cache.remove(key);
                    }
                    logger.info("Cache size: "+cache.size());

                }
            }
            catch(Exception e)
            {
                logger.error("Unable to clean cache. Ex="+e.getMessage(),e);
            }
        }
    }
    
    /**
     * **** cache method *****
     */
    public void cache(String key, Object msg,int TimeToLive) {
        
        if(TimeToLive<=0)
        {
            cache(key,msg);
        }
        else
        {
            Calendar date = Calendar.getInstance();
            CachePair   cpair=new CachePair();
            cpair.object=msg;
            cpair.removeTime=new Date(Calendar.getInstance().getTimeInMillis()+(60000*(long)TimeToLive));
            this.cache.put(key, cpair);
        }
    }
    
    /**
     * **** cache method *****
     */
    public void cache(String key, Object msg) {
        
        CachePair   cpair=new CachePair();
        cpair.object=msg;
        this.cache.put(key, cpair);
    }

    public Object cache(String key) {
        checkCache();
        if(this.cache.containsKey(key))
            return this.cache.get(key).object;
        else
            return null;                
    }

    /**
     * ****** Http get / post method ******
     */
    public String get(String url) throws Exception {

        try {
            HttpClient client = HttpClientBuilder.create().build();

            String toRequest = url;
            if (!toRequest.startsWith("http:")) {
                toRequest = this.commonServices + url;
            }
            
            HttpGet request = new HttpGet(toRequest);

            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectionRequestTimeout(this.requestTimeout)
                    .setConnectTimeout(this.requestTimeout)
                    .setSocketTimeout(this.requestTimeout)
                    .build();
            request.setConfig(requestConfig);

            HttpResponse response = client.execute(request);

            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line = "";
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            return result.toString();
            
        } catch (Exception e) {
            logger.error("[get] error in get, exception is " + e);
            return null;
        }
    }

    public String post(String url, String data) throws Exception {

        try {
            HttpClient client = HttpClientBuilder.create().build();
            
            String toRequest = url;
            if (!toRequest.startsWith("http:")) {
                toRequest = this.commonServices + url;
            }
            
            HttpPost post = new HttpPost(toRequest);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectionRequestTimeout(this.requestTimeout)
                    .setConnectTimeout(this.requestTimeout)
                    .setSocketTimeout(this.requestTimeout)
                    .build();
            post.setConfig(requestConfig);
            post.setHeader("Content-type", "application/json");
            post.setEntity(new StringEntity(data));

            HttpResponse response = client.execute(post);
            BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
            StringBuilder result = new StringBuilder();
            String line = "";
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            return result.toString();
        } catch (Exception e) {
            logger.error("[post] error in post, exception is " + e);
            return null;
        }
    }

    /**
     * *** LOG Methods ****
     */
    
    public org.apache.log4j.Logger getScreenLogger()
    {
        synchronized(threadRequestMap)
        {
            if(threadRequestMap.containsKey(""+Thread.currentThread().getId()))
            {
                Exchange e= threadRequestMap.get(""+Thread.currentThread().getId());
                String EventApp=(String)e.getIn().getHeader("EventApp");
                String EventAppState=(String)e.getIn().getHeader("EventAppState");
                String EventAppId=(String)e.getIn().getHeader("EventAppId");
                String Key=EventApp+"_"+EventAppId;
                if(screenToLoggerMap.containsKey(Key))
                {
                    return screenToLoggerMap.get(Key);                    
                }
            }            
        } 
        return null;
    }
    
    public void info(String data) {
        
        org.apache.log4j.Logger screenlogger=getScreenLogger();
        if(screenlogger!=null)
            screenlogger.info(data);
        else
            logger.info(data);
    }

    
    
    public void debug(String data) {
        org.apache.log4j.Logger screenlogger=getScreenLogger();
        if(screenlogger!=null)
            screenlogger.debug(data);
        else
            logger.debug(data);
        
    }

    public void warn(String data) {
        
        org.apache.log4j.Logger screenlogger=getScreenLogger();
        if(screenlogger!=null)
            screenlogger.warn(data);
        else
            logger.warn(data);
    }

    public void error(String data) {
        
        org.apache.log4j.Logger screenlogger=getScreenLogger();
        if(screenlogger!=null)
            screenlogger.error(data);
        else
            logger.error(data);
    }
    
    public void info(String data, Exchange e) {
        try {
            Logger logTmp = LoggerFactory.getLogger(e.getFromRouteId());
            logTmp.info(data);
        } catch (Exception ex) {
            logger.info(data);
        }
    }
    
    public void debug(String data, Exchange e) {
        try {
            Logger logTmp = LoggerFactory.getLogger(e.getFromRouteId());
            logTmp.debug(data);
        } catch (Exception ex) {
            logger.debug(data);
        }
    }

    public void warn(String data, Exchange e) {
        try {
            Logger logTmp = LoggerFactory.getLogger(e.getFromRouteId());
            logTmp.warn(data);
        } catch (Exception ex) {
            logger.warn(data);
        }
    }

    public void error(String data, Exchange e) {
        try {
            Logger logTmp = LoggerFactory.getLogger(e.getFromRouteId());
            logTmp.error(data);
        } catch (Exception ex) {
            logger.error(data);
        }
    }

    /**
     * ********** Date Helper ***********
     */
    public DateTime parseDate(String date, String format) {
        DateTimeParser[] parsers = {DateTimeFormat.forPattern(format).getParser()};
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();
        return formatter.parseDateTime(date);
    }
    
    public DateTime parseUtcDate(String date, String format) {
        DateTimeParser[] parsers = {DateTimeFormat.forPattern(format).getParser()};
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().append(null, parsers).toFormatter();
        return formatter.parseDateTime(date).withZoneRetainFields(DateTimeZone.UTC);
    }

    public DateTime toUtc(DateTime dte) {
        return dte.toDateTime(DateTimeZone.UTC);
    }
    
    public DateTime toLocal(DateTime dte) {
        return dte.toDateTime(DateTimeZone.getDefault());
    }
    
    public String printDate(DateTime date, String format) {
        DateTimeFormatter fmt1 = DateTimeFormat.forPattern(format);	
        return fmt1.print(date);
    }
    
    /***** FILE Utility ********/
    public String readFile(String path) throws IOException {
        return FileUtils.readFileToString(new File(path));
    }
    
    
    /******** Property utility *****************/
    public String property(String property, String language) throws Exception {
        
        // We have to load the properties file or reload if change
        String key = "prop";
        if (!language.isEmpty()) {
            key += "_" + language.toLowerCase();
        }
        
        File f = new File(this.propertiesPath + key + ".properties");
        if (f.exists()) {
            if (!this.propertiesFile.containsKey(key) || this.propertiesFileDate.get(key) != f.lastModified()) {
                this.loadProp(key, f);
            }
        } 
        
        // We load the fallback mode
        File f2 = new File(this.propertiesPath + "prop.properties");
        if (f2.exists()) { // Fallback to file without language if not found
            if (!this.propertiesFile.containsKey("prop") || this.propertiesFileDate.get("prop") != f2.lastModified()) {
                this.loadProp("prop", f2);
            }
        }
        
        Properties prop = this.propertiesFile.get(key);
        if (prop != null && !"not_found".equals(prop.getProperty(property, "not_found"))) {
            return prop.getProperty(property, "not_found");  
        } else {
            prop = this.propertiesFile.get("prop");
            if (prop != null) {
                return prop.getProperty(property, "not_found");  
            } else {
                return "not_found";
            }
        }

    }

    public String property(String property) throws Exception {
        return this.property(property, "");
    }
    
    private void loadProp(String key, File f) throws Exception {
        
        Properties prop = new Properties();
        
        FileInputStream fs = new FileInputStream(f);
        prop.load(new InputStreamReader(fs, StandardCharsets.UTF_8) );
        fs.close();
        
        this.propertiesFile.put(key, prop);
        this.propertiesFileDate.put(key, f.lastModified());
    }
    
    /****************** APP PROPERTY ***************/
    public String appProperty(String app, String property, String language) throws Exception {
        
        // We have to load the properties file or reload if change
        String key = "prop";
        if (!language.isEmpty()) {
            key += "_" + language.toLowerCase();
        }
        
        File f = new File(this.appPropertiesPath + app + "/" + key + ".properties");
        if (f.exists()) {
            if (!this.appPropertiesFile.containsKey(key) || this.appPropertiesFileDate.get(key) != f.lastModified()) {
                this.loadAppProp(key, f);
            }
        } 
        else
        {
            logger.info("File1:"+f.getAbsolutePath()+" does not exist.");
        }
        
        // We load the fallback mode
        File f2 = new File(this.appPropertiesPath + app + "/" + "prop.properties");
        if (f2.exists()) { // Fallback to file without language if not found
            if (!this.appPropertiesFile.containsKey("prop") || this.appPropertiesFileDate.get("prop") != f2.lastModified()) {
                this.loadAppProp("prop", f2);
            }
        }
        else
        {
            logger.info("File2:"+f2.getAbsolutePath()+" does not exist.");
        }
        
        Properties prop = this.appPropertiesFile.get(key);
        if (prop != null && !"not_found".equals(prop.getProperty(property, "not_found"))) 
        {
            return prop.getProperty(property, "not_found (1)");  
        }
        else 
        {
            prop = this.appPropertiesFile.get("prop");
            if (prop != null) 
            {
                return prop.getProperty(property, "not_found (2)");  
            } 
            else 
            {
                return "not_found (3)";
            }
        }
    }
    
    public String appProperty(String app, String property) throws Exception {
        return this.appProperty(app, property, "");
    }
    
    private void loadAppProp(String key, File f) throws Exception {
        
        Properties prop = new Properties();
        
        FileInputStream fs = new FileInputStream(f);
        prop.load(new InputStreamReader(fs, StandardCharsets.UTF_8) );
        fs.close();
        
        this.appPropertiesFile.put(key, prop);
        this.appPropertiesFileDate.put(key, f.lastModified());
    }
    
    /***************** HASH code *******************/
    public String hashCode(String s) {
        
        if (s==null){
            return "string_null";
        }
        
        return "" + s.hashCode();
        
    }
    
    /** Screens Monitoring **/
    
    public void saveMonitoringStatus(String app,String id,String status)
    {
        logger.debug("App="+app+" Id="+id+" Mes="+status);
        
        synchronized (statuses)
        {
            if (!statuses.containsKey(app))
            {
                statuses.put(app, new HashMap<String, Object>());
            }

            HashMap<String, Object> appStatus = statuses.get(app);

            if (!appStatus.containsKey(id))
            {
                appStatus.put(id, status);
            }
            else
            {
                appStatus.put(id,status);
            } 

            // We memory the last publish time
            String key = app.toUpperCase()+ "_" + id.toUpperCase();
            if (!lastPublishTime.containsKey(key))
            {
                lastPublishTime.put(key, DateTime.now());
            }
            else
            {
                lastPublishTime.put(key,DateTime.now());
            }
        }
    }
    
    public void queueEvent(String message) throws ParseException
    {
        logger.info("Message="+message);
        try
        {
            JSONParser parser = new JSONParser();
            JSONObject item = (JSONObject)parser.parse(message);
                            
            synchronized (polledEvents)
            {
                if (!polledEvents.containsKey((String)item.get("queue")))
                {
                    polledEvents.put((String)item.get("queue"), new ArrayList<JSONObject>());
                }

                List<JSONObject> lst = polledEvents.get((String)item.get("queue"));
                item.put("ttl", DateTime.now().plusSeconds(60).getMillis());
                lst.add(item);
            }
        }
        catch (Exception e)
        {
            logger.error("[AddEvent] unable to add, error is " + e);
        }
    }
    
    public String eventsRetrieve(String id,String request) throws ParseException
    {
        logger.debug("ID="+id+" Request="+request);
        
        JSONObject jsonret = new JSONObject();
        if((id==null)||(id.compareTo("")==0))
        {
            jsonret.put("error","no_id");
        }
        else if((request==null)||(request.compareTo("")==0))
        {
            jsonret.put("error","no_request");
        }
        else
        { 
           synchronized (polledEvents)
            {
                JSONParser parser = new JSONParser();
                JSONObject requestasjson = (JSONObject)parser.parse(request);
                String uid=""+((Long)requestasjson.get("uid")).longValue();
                
                if (!polledEvents.containsKey(id))
                {
                    jsonret.put("error","unknown_id");
                    jsonret.put("consumers",rotatePollerIdsAndGetNumberOfListener(id, uid));
                    return jsonret.toJSONString();
                }

                List<JSONObject> lst = polledEvents.get(id);
                
                JSONArray eventsjson=new JSONArray();
                
                for(JSONObject obj:lst)
                {
                    eventsjson.add(obj);
                }
                
                lst.clear();
                //return Json(new { events = events, consumers = this.RotatePollerIdsAndGetNumberOfListener(id, request.uid) });
                jsonret.put("events",eventsjson);                
                jsonret.put("consumers",rotatePollerIdsAndGetNumberOfListener(id, uid));
                return jsonret.toJSONString();
                
            }
        }
        return jsonret.toJSONString();
    }
    
    public int rotatePollerIdsAndGetNumberOfListener(String queueName,String uid)
    {
        synchronized (pollerIdsByQueue)
        {
            if (!pollerIdsByQueue.containsKey(queueName))
            {
                pollerIdsByQueue.put(queueName, new ArrayList<Pair<String, Long>>());
            }
        }

        ArrayList<Pair<String, Long>> pollerIds = pollerIdsByQueue.get(queueName);

        // We add the new events
        HashMap<String,String> set = new HashMap<String,String>();

        synchronized (pollerIds)
        {
            pollerIds.add(new Pair<String, Long>(uid, DateTime.now().getMillis()));

            long previousTick = DateTime.now().plusSeconds(-10).getMillis();

            // We clean previous ticks
            ArrayList<Pair<String, Long>> lstTodelete = new ArrayList<Pair<String, Long>>();
            
            for (Pair<String, Long> el : pollerIds)
            {
                if (el.getValue() <= previousTick)
                {
                    lstTodelete.add(el);
                }
            }
            for (Pair<String, Long> el : lstTodelete)
            {
                pollerIds.remove(el);
            }

            // We count the number of differents listener

            for (Pair<String, Long> el : pollerIds)
            {
                set.put(el.getKey(),el.getKey());
            }
        }

        return set.size();
    }
    
    public String monitoringStatuses(String id)
    {
        logger.debug("ID="+id+" App="+id);
        if (id==null || id.compareTo("")==0)
            {
                logger.info("No App");
                JSONObject jsonret = new JSONObject();
                jsonret.put("status","no_app");
                return jsonret.toJSONString();
                
            }

            if (!statuses.containsKey(id))
            {
                logger.info("No ID");
                JSONObject jsonret = new JSONObject();
                jsonret.put("status","app_not_found");
                return jsonret.toJSONString();
            }

            synchronized (statuses)
            {
                JSONArray status = new JSONArray();
                for (Entry<String, Object> el : statuses.get(id).entrySet())
                {
                    String key = id.toUpperCase() + "_" + el.getKey().toUpperCase();

                    JSONObject item=new JSONObject();
                    
                    item.put("id",  el.getKey());
                    item.put("status", EnhanceStatusObject(el.getValue(), key));

                    // We compute the last get status time in secondes
                    item.put("lastStatusPollAgo",9999999);
                    if (lastGetStatusTime.containsKey(key))
                    {
                        Period diff=new Period(DateTime.now(),lastGetStatusTime.get(key));
                        item.put("lastStatusPollAgo", diff.toStandardSeconds());
                    }

                    status.add(item);
                }
                JSONObject res=new JSONObject();
                res.put("app", id);
                res.put("statuses", status);
                return res.toJSONString();
            }
    }
    
    private JSONObject EnhanceStatusObject(Object data, String key)
    {
        JSONObject myobject;
        try
        {
            JSONParser parser = new JSONParser();
            myobject = (JSONObject)parser.parse((String)data);



            int delay = 999999;
            synchronized (lastPublishTime)
            {
                if (lastPublishTime.containsKey(key))
                {
                    delay = new Period(lastPublishTime.get(key),DateTime.now()).toStandardSeconds().getSeconds();
                }
            }


            myobject.remove("lastPublishDoneAgo");
            myobject.put("lastPublishDoneAgo", delay);
        }
        catch(Exception e)
        {
             myobject=new JSONObject();
        }
        return myobject;

    }
    
    /**
     * ***** PROCESS EXCHANGE METHOD *******
     */
    public void process(Exchange exchng) throws Exception {
        exchng.getIn().setHeader("jsHelper", this);
    }

    public boolean resolveStatus(String aStatus)
    {
        return statusManager.resolveStatus(aStatus);
    }

    public void feedWithStatus(String aStatus)
    {
        statusManager.feedWithStatus(aStatus);
    }

    
}


class CachePair
{
    Date removeTime=null;
    Object object;
}