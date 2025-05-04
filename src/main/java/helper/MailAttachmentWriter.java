package helper;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.activation.DataHandler;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

import org.apache.camel.attachment.AttachmentMessage;
import java.util.Base64; // Utilisation de la classe Java 8+ pour le décodage Base64

/**
 * Converts a mail attachment to a stream
 * Adds AttachmentName and AttachmentExtension headers
 * 
 * @author Arnaud Marchand
 */
public class MailAttachmentWriter implements Processor {
    
    static final Logger logger = LoggerFactory.getLogger("MailAttachmentWriter");
    
    public MailAttachmentWriter() {
        logger.info("Mail attachment writer created.");
    }

    @Override
    public void process(Exchange exchange) throws Exception {        
        AttachmentMessage attachmentsMessage = (AttachmentMessage) exchange.getIn();
        
        Map<String, DataHandler> attachments = attachmentsMessage.getAttachments();
        
        if (attachments.size() > 1) {
            logger.error("Unexpected number of attachments:" + attachments.size());
        }
        
        if (attachments.size() > 0) {
            for (String name : attachments.keySet()) {
                try {
                    DataHandler dh = attachments.get(name);
                    String filename = dh.getName();

                    InputStream resobj = dh.getInputStream();
                    
                    logger.info("Attachment type: " + resobj.getClass() + " name: " + name + " filename: " + filename);
                    
                    // Vérification du nom du fichier pour les encodages UTF-8
                    if (filename.indexOf("utf-8") >= 0) {
                        logger.info("setting filename to: " + name);
                        filename = name;
                        if (filename.indexOf("utf-8") >= 0) {
                            filename = "DUMMY.pdf";
                            logger.info("setting filename to: " + filename);
                        }
                    }
                    
                    // Vérification et traitement du flux Base64
                    if (isBase64Encoded(resobj)) {
                        logger.info("Converting Base64 attachment");
                        byte[] decodedBytes = decodeBase64(resobj);
                        exchange.getIn().setBody(new ByteArrayInputStream(decodedBytes));
                    } else {
                        exchange.getIn().setBody(resobj);
                    }

                    // Extraction du nom et de l'extension du fichier
                    int lastindex = filename.lastIndexOf('.');
                    if (lastindex < 0) {
                        exchange.getIn().setHeader("AttachmentName", filename);
                        exchange.getIn().setHeader("AttachmentExtension", "None");
                    } else {
                        exchange.getIn().setHeader("AttachmentName", filename.substring(0, lastindex));
                        exchange.getIn().setHeader("AttachmentExtension", filename.substring(lastindex + 1));
                    }
                    
                } catch (Exception e) {
                    logger.error("ERROR: Unable to forward attachment. Ex=" + e.getMessage(), e);
                }
            }
        }
    }

    /**
     * Verifies if the input stream is Base64 encoded.
     * @param inputStream the input stream to check.
     * @return true if the stream seems to be Base64 encoded.
     */
    private boolean isBase64Encoded(InputStream inputStream) {
        // Essaye de décoder une partie des données en Base64 et vérifie si cela réussit
        try {
            byte[] data = inputStream.readAllBytes();
            // Tentative de décodage en Base64
            Base64.getDecoder().decode(data); // Si cela échoue, une exception sera levée
            return true;
        } catch (IllegalArgumentException e) {
            // Si une exception est levée, ce n'est pas Base64 valide
            return false;
        } catch (Exception e) {
            logger.error("Error checking if input stream is Base64 encoded: " + e.getMessage());
            return false;
        }
    }

    /**
     * Decodes a Base64 encoded InputStream into a byte array.
     * @param inputStream the input stream to decode.
     * @return decoded byte array.
     */
    private byte[] decodeBase64(InputStream inputStream) throws Exception {
        byte[] data = inputStream.readAllBytes();
        return Base64.getDecoder().decode(data);
    }
}
