package com.equans.camel.component.pubsub;

import com.equans.pubsub.client.PubSubClient;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.apache.camel.util.CaseInsensitiveMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubProducer extends DefaultProducer {
    
    private static final Logger LOG = LoggerFactory.getLogger(PubSubEndPoint.class);
    
    private PubSubClient pubSub;
    private String uri;
    private Map<String, String> statusValues;
    private String headersPrefix = "";

    public PubSubProducer(PubSubClient pubSub, String uri, Map<String, String> statusValues, String prefix, Endpoint endpoint) throws UnsupportedEncodingException {
        super(endpoint);
        
        this.pubSub = pubSub;
        this.statusValues = statusValues;
         
        int indexofslash = uri.indexOf("/");
        this.uri = uri.substring(indexofslash+2);
        this.uri=java.net.URLDecoder.decode(this.uri, "UTF-8");
        
        this.headersPrefix = prefix.toLowerCase();        
    }


    @Override
    public void process(Exchange exchng) throws Exception {
        if (uri.startsWith("topic:")) {
            
            doTopic(exchng);
            
        } else if (uri.startsWith("queue:")) {
            
            doQueue(exchng);
            
        } else if (uri.startsWith("status:")) {
            
            doStatus(exchng);
            
        } else if (uri.startsWith("getstatus")) {
            
            doGetStatus(exchng);
            
        } else {
            
            doTopic(exchng);
        }
    }
    
    private void doStatus(Exchange exchng) {
        String rem = uri.replace("status:", "");
            
        if (rem.startsWith("delete:")) {
            String rembis = rem.replace("delete:", "");
            synchronized(this.statusValues) {
                this.statusValues.remove(rembis);
            }
        } else if (rem.startsWith("add:")) {
            String rembis = rem.replace("add:", "");
            synchronized(this.statusValues) {
                this.statusValues.put(rembis, exchng.getIn().getBody(String.class));
            }
        }else if (rem.startsWith("increment:")) {
            String rembis = rem.replace("increment:", "");
            synchronized(this.statusValues) {
                if(!this.statusValues.containsKey(rembis))
                    this.statusValues.put(rembis, "1");
                else
                {
                    String res=this.statusValues.get(rembis);
                    this.statusValues.put(rembis, ""+(Long.parseLong(res)+1));
                }                
            }
        }
        else if (rem.startsWith("reset:")) {
            String rembis = rem.replace("reset:", "");
            synchronized(this.statusValues) {
                this.statusValues.put(rembis, "0");
            }
        } else if (rem.startsWith("global")) {
            this.pubSub.setStatus(exchng.getIn().getBody(String.class));
        }
        else {
            synchronized(this.statusValues) {
                this.statusValues.put(rem, exchng.getIn().getBody(String.class));
            }
        }
    }
    
    private void doGetStatus(Exchange exchng) {
        
        exchng.getIn().getHeaders().put("IsLocalMaster", this.pubSub.getCurrentState()==PubSubClient.STATE.MASTER);
    }
    
    private void doQueue(Exchange exchng) {
        this.pubSub.publishMessageOnQueue(uri.replace("queue:", ""), exchng.getIn().getBody(), getPropertiesJMS((CaseInsensitiveMap)exchng.getIn().getHeaders()));
    }
    
    private void doTopic(Exchange exchng) {
        this.pubSub.publishMessageOnTopic(uri.replace("topic:", ""), exchng.getIn().getBody(), getPropertiesJMS((CaseInsensitiveMap)exchng.getIn().getHeaders()));
    }
    
    private Map<String,String> getPropertiesJMS(CaseInsensitiveMap headers){
        
        Map<String, String> ret = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
        
        if (this.headersPrefix.isEmpty()){
            for (Map.Entry<String,Object> entry : headers.entrySet()) {
                try {
                    if (entry.getValue() != null) {
                        ret.put(entry.getKey(), entry.getValue().toString());
                    }
                } catch (Exception e) {
                    LOG.error("Error copying headers", e);
                }
            }
        } else {
            for (Map.Entry<String,Object> entry : headers.entrySet()) {
                if (entry.getKey().startsWith(this.headersPrefix)) {
                    try {
                        if (entry.getValue() != null) {
                            ret.put(entry.getKey().replace(this.headersPrefix, ""), entry.getValue().toString());
                        }
                    } catch (Exception e) {
                        LOG.error("Error copying headers", e);
                    }
                
                }
            }
        }
             
        return ret;
    }

    
}
