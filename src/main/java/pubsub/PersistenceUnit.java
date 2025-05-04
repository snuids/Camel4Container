/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package pubsub;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import jakarta.jms.BytesMessage;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Arnaud Marchand
 */
public class PersistenceUnit implements Processor
{

    static final Logger logger = LoggerFactory.getLogger("FileAppender");
    String directory;
    SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.S");
    SimpleDateFormat fileformatter = new SimpleDateFormat("yyyyMMdd_HH");
    FileWriter filewriter;
    File curfile;
    public long messagesWrittenToDisk = 0;
    public final Map<String, Long> messagesPerTopic = new HashMap<String, Long>();
    public HashMap<String,PersistentMessageListener> listenersPerTopic=new HashMap<String, PersistentMessageListener>();
    public Dispatcher owner;
    
    public String getFileForDate(Date aDate)
    {
        File dir = new File(directory);
        String filename = dir.getAbsolutePath() + File.separator + "Persistence" + fileformatter.format(aDate) + ".txt";
        return filename;
    }

    public String getDirectory()
    {
        return directory;
    }

    public void setDirectory(String Directory)
    {
        this.directory = Directory;
    }

    @Override
    public void process(Exchange exchng) throws Exception
    {
        File dir = new File(directory);

        if (!dir.exists())
        {
            dir.mkdir();
        }

        if (exchng.getIn().hasHeaders())
        {
            //for(Entry<String,Object> obj:exchng.getIn().getHeaders().entrySet())
            //{
            //    logger.info("coucou:"+obj.getKey());
            //}

            java.lang.Long timestamp = (java.lang.Long) exchng.getIn().getHeader("OriginalJMSTimestamp");
            if (timestamp == null)
            {
                timestamp = (java.lang.Long) exchng.getIn().getHeader("JMSTimestamp");
            }

            if (timestamp != null)
            {
                String topic = (String) exchng.getIn().getHeader("Topic");
                if (topic == null)
                {
                    topic = "NA";
                }

                String pubsubid = "NA";
                try
                {
                    Long resl = (Long) exchng.getIn().getHeader("PubSubID");
                    if (resl != null)
                    {
                        pubsubid = String.valueOf(resl);
                    }
                }
                catch (Exception e)
                {
                    logger.warn("Unable to retrieve pubsubID. Ex="+e.getMessage());
                }
                StringBuilder build;
                build = new StringBuilder();
                Date date = new Date(timestamp.longValue());
                build.append(pubsubid);
                build.append('\t');
                build.append(formatter.format(date));
                build.append('\t');
                build.append(topic);
                build.append('\t');
                build.append(exchng.getIn().getBody());
                //logger.info("MES:"+build.toString());

                synchronized (messagesPerTopic)
                {
                    if (!messagesPerTopic.containsKey(topic))
                    {
                        messagesPerTopic.put(topic, Long.valueOf(1));
                    }
                    else
                    {
                        Long res = messagesPerTopic.get(topic);
                        messagesPerTopic.put(topic, res + 1);

                    }
                }

                String filename = "Persistence" + fileformatter.format(date) + ".txt";
                String fullfilename = dir.getAbsolutePath() + File.separator + filename;

                try
                {
                    if (curfile == null)
                    {
                        logger.info("Opening file:" + fullfilename);
                        curfile = new File(fullfilename);
                        
                        filewriter = new FileWriter(curfile, true);
                    }
                    else
                    {
                        if (curfile.getName().compareTo(filename) == 0)
                        {
                            //logger.info("Append file:"+filename);
                        }
                        else
                        {
                            logger.info("Closing previous file.");
                            filewriter.flush();
                            filewriter.close();
                            logger.info("Opening file:" + fullfilename);
                            curfile = new File(fullfilename);
                            filewriter = new FileWriter(curfile, true);
                        }
                    }
                    filewriter.append(build.toString().replaceAll("\r", "[(CR)]").replaceAll("\n", "[(LF)]").replaceAll("'", "[(SQ)]"));
                    filewriter.append("\r\n");
                    filewriter.flush();

                    messagesWrittenToDisk++;
                }
                catch (Exception e)
                {
                    logger.error("Unable to append message to file.");
                }

            }
        }
    }

    
    public void onMessage(Message msg,String topic)
    {
        try
        {

            if(owner!=null)
            {
                try
                {
                    owner.persistentMessageReceived(msg,topic);
                }
                catch(Exception e)
                {
                    logger.info("Unable to forward message to Dispatcher. Ex="+e.getMessage());
                }
            }

            File dir = new File(directory);

            if (!dir.exists())
            {
                dir.mkdir();
            }

            String MessageAsText="";

            if(msg instanceof BytesMessage)
            {
                BytesMessage byteMessage=(BytesMessage)msg;
                byte[] byteArr = new byte[(int)byteMessage.getBodyLength()];
                byteMessage.readBytes(byteArr); 
                MessageAsText = new String(byteArr);//, "UTF-16");  
            }
            else if(msg instanceof TextMessage)
            {
                MessageAsText=((TextMessage)msg).getText();
            }
            else if (!(msg instanceof TextMessage)) {
                return;
            }

            Message mes = msg;


            long timestamp;
            try
            {
                timestamp = mes.getLongProperty("OriginalJMSTimestamp");
            }
            catch (Exception e)
            {
                try
                {
                    timestamp= mes.getLongProperty("JMSTimestamp");
                }
                catch (Exception e2)
                {
                    logger.info("No timestamp defined.");
                    return;
                }
            }


            String pubsubid = "NA";
            try
            {
                long resl = mes.getLongProperty("PubSubID");
                pubsubid = "" + resl;
            }
            catch (Exception e)
            {
                logger.warn("Unable to retrieve PubSubID. Ex="+e.getMessage());
            }
            StringBuilder build;
            build = new StringBuilder();
            Date date = new Date(timestamp);
            build.append(pubsubid);
            build.append('\t');
            build.append(formatter.format(date));
            build.append('\t');
            build.append(topic);
            build.append('\t');
            build.append(MessageAsText);

            synchronized (messagesPerTopic)
            {
                if (!messagesPerTopic.containsKey(topic))
                {
                    messagesPerTopic.put(topic, Long.valueOf(1));
                }
                else
                {
                    Long res = messagesPerTopic.get(topic);
                    messagesPerTopic.put(topic, res + 1);

                }
            }

            String filename = "Persistence" + fileformatter.format(date) + ".txt";
            String fullfilename = dir.getAbsolutePath() + File.separator + filename;

            try
            {
                if (curfile == null)
                {
                    logger.info("Opening file:" + fullfilename);
                    curfile = new File(fullfilename);
                    filewriter = new FileWriter(curfile, true);
                }
                else
                {
                    if (curfile.getName().compareTo(filename) == 0)
                    {
                        //logger.info("Append file:"+filename);
                    }
                    else
                    {
                        logger.info("Closing previous file.");
                        filewriter.flush();
                        filewriter.close();
                        logger.info("Opening file:" + fullfilename);
                        curfile = new File(fullfilename);
                        filewriter = new FileWriter(curfile, true);
                    }
                }
                filewriter.append(build.toString().replaceAll("\r", "[(CR)]").replaceAll("\n", "[(LF)]"));
                filewriter.append("\r\n");
                filewriter.flush();

                messagesWrittenToDisk++;
            }
            catch (Exception e)
            {
                logger.error("Unable to append message to file.");
            }
        }
        catch(Exception e2)
        {
            logger.error("Unable to append message to file. Ex="+e2.getMessage(),e2);
        }
    }
}
