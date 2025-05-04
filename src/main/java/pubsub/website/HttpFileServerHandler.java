package pubsub.website;

import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpHeaderNames;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import org.jboss.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import org.jboss.netty.handler.codec.http.multipart.InterfaceHttpData;
import org.jboss.netty.handler.codec.http.multipart.MemoryAttribute;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedFile;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pubsub.Dispatcher;

import jakarta.activation.MimeType;
import jakarta.activation.MimeTypeParseException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.GET;
import static org.jboss.netty.handler.codec.http.HttpMethod.POST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 *
 * @author Arnaud Marchand
 */
public class HttpFileServerHandler extends SimpleChannelUpstreamHandler {

    static final Logger logger = LoggerFactory.getLogger("HttpFileServerHandler");
    public static Dispatcher pubSubDispatcher;
    public static HttpFileServer httpFileServer;

    public static final String HTTP_DATE_FORMAT = "EEE, dd MMM yyyy HH:mm:ss zzz";
    public static final String HTTP_DATE_GMT_TIMEZONE = "GMT";
    public static final int HTTP_CACHE_SECONDS = 60;
    

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        
        HttpRequest request = (HttpRequest) e.getMessage();
        
        
        if ((request.getMethod() != GET) && (request.getMethod() != POST)) {
            sendError(ctx, METHOD_NOT_ALLOWED);
            return;
        }
        logger.info("Request:"+request.getUri());

        if (request.getUri().contains("PUBSUBCMD")) {
            dynamicMessageReceived(ctx, e);
            logger.info("Done PUBSUBCMD.");
            return;
        }

        if (request.getUri().contains("SHOWROUTE")) {
            dynamicRouteMessageReceived(ctx, e);
            logger.info("Done SHOWROUTE.");

            return;
        }

        if (request.getUri().contains("GETCONFIGURATION")) {
            dynamicGetConfigurationMessageReceived(ctx, e);
            logger.info("Done GETCONFIGURATION.");
            return;
        }
                
        if (request.getUri().contains("UPDATECONFIGURATION")) {
            dynamicUpdateConfigurationMessageReceived(ctx, e);
            logger.info("Done UPDATECONFIGURATION.");
            return;
        }

        if (request.getUri().contains("GETSINGLETONASJSON")) {
            dynamicGetSingletonMessageReceived(ctx, e, true);
            logger.info("Done GETSINGLETONASJSON.");
            
            return;
        }

        if (request.getUri().contains("GETSINGLETON")) {
            dynamicGetSingletonMessageReceived(ctx, e, false);
            logger.info("Done GETSINGLETON.");
            return;
        }

        if (request.getUri().contains("COMMANDROUTE")) {
            dynamicCommandRouteMessageReceived(ctx, e);
            logger.info("Done COMMANDROUTE.");
            return;
        }

        if (request.getUri().contains("SETLICENCE")) {
            dynamicSetLicenceMessageReceived(ctx, e);
            logger.info("Done SETLICENCE.");
            return;
        }

        if (request.getUri().contains("LICENCE")) {
            dynamicLicenceMessageReceived(ctx, e);
            logger.info("Done LICENCE.");
            return;
        }

        final String path = sanitizeUri(request.getUri());

        if (path == null) {
            sendError(ctx, FORBIDDEN);
            return;
        }

        File file = new File(path);
        if (file.isHidden() || !file.exists()) {
            sendError(ctx, NOT_FOUND);
            return;
        }
        if (!file.isFile()) {
            sendError(ctx, FORBIDDEN);
            return;
        }
        
        logger.info("Serving file:"+file.getAbsolutePath());
        
        RandomAccessFile raf;
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException fnfe) {
            sendError(ctx, NOT_FOUND);
            return;
        }
        
        logger.info("Read size:"+raf.length());
                
        long fileLength = raf.length();
        
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        HttpHeaders.setContentLength(response, fileLength);
        HttpHeaders.setHeader(response, "Access-Control-Allow-Origin", "*");
        setContentTypeHeader(response, file);
        setDateAndCacheHeaders(response, file);
    
        Channel ch = e.getChannel();

        // Write the initial line and the header.
        ch.write(response);

        // Write the content.
        ChannelFuture writeFuture;
        if (ch.getPipeline().get(SslHandler.class) != null) {
            // Cannot use zero-copy with HTTPS.
            writeFuture = ch.write(new ChunkedFile(raf, 0, fileLength, 8192));
        } else {
            // No encryption - use zero-copy.
            final FileRegion region
                    = new DefaultFileRegion(raf.getChannel(), 0, fileLength);
            writeFuture = ch.write(region);
            writeFuture.addListener(new ChannelFutureProgressListener() {
                public void operationComplete(ChannelFuture future) {
                    region.releaseExternalResources();
                }

                public void operationProgressed(
                        ChannelFuture future, long amount, long current, long total) {
                    System.out.printf("%s: %d / %d (+%d)%n", path, current, total, amount);
                }
            });
        }

        // Decide whether to close the connection or not.
        if (!isKeepAlive(request)) {
            // Close the connection when the whole content is written out.
            writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
        logger.info("Done.Serving File..");
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Channel ch = e.getChannel();
        Throwable cause = e.getCause();
        if (cause instanceof TooLongFrameException) {
            sendError(ctx, BAD_REQUEST);
            return;
        }

        cause.printStackTrace();
        if (ch.isConnected()) {
            sendError(ctx, INTERNAL_SERVER_ERROR);
        }
    }

    private static String sanitizeUri(String uri) {
        // Decode the path.
        try {
            uri = URLDecoder.decode(uri, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            try {
                uri = URLDecoder.decode(uri, "ISO-8859-1");
            } catch (UnsupportedEncodingException e1) {
                throw new Error();
            }
        }

        // Convert file separators.
        uri = uri.replace('/', File.separatorChar);

        // Simplistic dumb security check.
        // You will have to do something serious in the production environment.
        if (uri.contains(File.separator + ".")
                || uri.contains("." + File.separator)
                || uri.startsWith(".") || uri.endsWith(".")) {
            return null;
        }

        // Convert to absolute path.
        String res = System.getProperty("user.dir") + File.separator + uri;
        res = res.replaceAll("//", "/");
        return res;
    }

    private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);

        HttpHeaders.setHeader(response, CONTENT_TYPE, "text/plain; charset=UTF-8");
        
        response.setContent(ChannelBuffers.copiedBuffer(
                "Failure: " + status.toString() + "\r\n",
                CharsetUtil.UTF_8));

        // Close the connection as soon as the error message is sent.
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    private static void sendNotModified(ChannelHandlerContext ctx) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, HttpResponseStatus.NOT_MODIFIED);
        setDateHeader(response);
        ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
    }

    private static void setDateHeader(HttpResponse response) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));
        Calendar time = new GregorianCalendar();
        response.headers().set(HttpHeaders.Names.DATE, dateFormatter.format(time.getTime()));
    }

    private static void setDateAndCacheHeaders(HttpResponse response, File fileToCache) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(HTTP_DATE_FORMAT, Locale.US);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(HTTP_DATE_GMT_TIMEZONE));
        Calendar time = new GregorianCalendar();
        response.headers().set(HttpHeaders.Names.DATE, dateFormatter.format(time.getTime()));
        time.add(Calendar.SECOND, HTTP_CACHE_SECONDS);
        response.headers().set(HttpHeaders.Names.EXPIRES, dateFormatter.format(time.getTime()));
        response.headers().set(HttpHeaders.Names.CACHE_CONTROL, "private, max-age=" + HTTP_CACHE_SECONDS);
        response.headers().set(HttpHeaders.Names.LAST_MODIFIED, dateFormatter.format(new Date(fileToCache.lastModified())));
    }


    private static void setContentTypeHeader(HttpResponse response, File file) {
        try {
            // Utiliser Files.probeContentType pour obtenir le type MIME du fichier
            Path path = Paths.get(file.getPath());
            String mimeTypeString = Files.probeContentType(path);

            if (mimeTypeString == null) {
                logger.warn("Impossible de détecter le type MIME pour le fichier: " + file.getName());
                mimeTypeString = "application/octet-stream"; // Type MIME par défaut
            }

            // Utilisation de Jakarta MimeType pour gérer les types MIME de manière plus détaillée
            MimeType mimeType = new MimeType(mimeTypeString);
            String mimetype = mimeType.getBaseType();

            // Appliquer des règles personnalisées si le fichier est un .htm, .js ou .css
            if (mimetype.startsWith("application")) {
                if (file.getName().indexOf(".htm") > 0) {
                    mimetype = "text/html";
                } else if (file.getName().indexOf(".js") > 0) {
                    mimetype = "text/javascript";
                } else if (file.getName().indexOf(".css") > 0) {
                    mimetype = "text/css";
                }
            }

            // Définir l'en-tête Content-Type dans la réponse HTTP
            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, mimetype);
            logger.info("MIME type: " + mimetype);
        } catch (IOException | jakarta.activation.MimeTypeParseException e) {
            logger.error("Error setting MIME type header for file: " + file.getName(), e);
        }
    }
    
    
    public void dynamicMessageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        httpFileServer.checkJMSConnection();

        HttpRequest request = (HttpRequest) e.getMessage();

        String url = request.getUri();
        long ID = 0;
        if (url.indexOf("?") > 0) {
            String params = url.split("\\?")[1];
            for (String param : params.split("&")) {
                String[] cols = param.split("=");
                if (cols.length > 1) {
                    if (cols[0].compareTo("ID") == 0) {
                        ID = Long.parseLong(cols[1]);
                    }
                }
            }
        }

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.setContent(ChannelBuffers.copiedBuffer(httpFileServer.getJSONString(ID), CharsetUtil.UTF_8));
        response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
        ChannelFuture future = e.getChannel().write(response);
        future.addListener(ChannelFutureListener.CLOSE);
    }

    private void dynamicRouteMessageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        logger.debug("Route request received.");
        try {
            // HttpRequest request = (HttpRequest) e.getMessage();

            StringBuilder resstr = new StringBuilder();

            if (httpFileServer.getJmxSupervisor() == null) {
                logger.error("JMX Supervisor not defined in web server. Check your pubsub.xml");
            }
            resstr.append(httpFileServer.getJmxSupervisor().toJson());
            //logger.info(resstr.toString());

            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.setContent(ChannelBuffers.copiedBuffer(resstr.toString(), CharsetUtil.UTF_8));

            response.headers().set("Access-Control-Allow-Origin", "*");
            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
            
            ChannelFuture future = e.getChannel().write(response);
            future.addListener(ChannelFutureListener.CLOSE);
        } catch (Exception e2) {
            logger.error("Error while computing route answer. Ex=" + e2);
        }
        logger.debug("Route request sent.");
    }

    private void dynamicCommandRouteMessageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        logger.info("Route command request received.");
        try {
            HttpRequest request = (HttpRequest) e.getMessage();
            StringBuilder resstr = new StringBuilder();

            if (httpFileServer.getJmxSupervisor() == null) {
                logger.error("JMX Supervisor not defined in web server. Check your pubsub.xml");
            }
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
            Map<String, List<String>> params = queryStringDecoder.getParameters();
            String command = params.get("command").get(0);
            if (("reset".compareTo(command) == 0) || ("resume".compareTo(command) == 0) || ("suspend".compareTo(command) == 0)) {
                resstr.append(httpFileServer.getJmxSupervisor().callJMXMethod(command, params.get("client").get(0), params.get("route").get(0)));
            }

            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.setContent(ChannelBuffers.copiedBuffer(resstr.toString(), CharsetUtil.UTF_8));
            response.headers().set("Access-Control-Allow-Origin", "*");
            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

            setContentLength(response, resstr.length());
            ChannelFuture future = e.getChannel().write(response);
            future.addListener(ChannelFutureListener.CLOSE);
        } catch (Exception e2) {
            logger.error("Error while route command answer. Ex=" + e2);
        }
        logger.info("Route request sent.");
    }

    private void dynamicLicenceMessageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        logger.info("Licence request received.");
        try {
            HttpRequest request = (HttpRequest) e.getMessage();

            //QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
            //Map<String, List<String>> params = queryStringDecoder.getParameters();
            StringBuilder resstr = new StringBuilder();

            resstr.append("<html>");
            resstr.append("<body>");
            resstr.append("<H1>Licence Server " + pubSubDispatcher.getServerID() + "</H1>");
            resstr.append("<H2>Server ID String</H2>");
            resstr.append("<textarea rows='6' style='width:100%'>");
            resstr.append(pubSubDispatcher.publicSleutel);
            resstr.append("</textarea>");
            resstr.append("<H2>Server Licence</H2>");
            resstr.append("<form name='licenceform' action='/SETLICENCE' method='POST'>");
            resstr.append("<textarea id='newlic' name='newlicname' rows='6' style='width:100%'>");
            resstr.append("</textarea><br/>");
            resstr.append("<input type='submit' value='Install' />");
            resstr.append("</form>");
            resstr.append("</body>");
            resstr.append("</html>");
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.setContent(ChannelBuffers.copiedBuffer(resstr.toString(), CharsetUtil.UTF_8));
            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
            setContentLength(response, resstr.length());
            ChannelFuture future = e.getChannel().write(response);
            future.addListener(ChannelFutureListener.CLOSE);
        } catch (Exception e2) {
            logger.error("Unable to prepare licence. Ex=" + e2);
        }
        logger.info("Licence request received.");
    }

    private void dynamicSetLicenceMessageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        logger.info("Set licence request received.");
        try {
            HttpRequest request = (HttpRequest) e.getMessage();

            HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(new DefaultHttpDataFactory(false), request);
            InterfaceHttpData resform = decoder.getBodyHttpData("newlicname");

            MemoryAttribute attribute = (MemoryAttribute) resform;
            String value = attribute.getValue();

            String result = httpFileServer.getPubSubDispatcher().validateSleutel(value);

            StringBuilder resstr = new StringBuilder();

            resstr.append("<html>");
            resstr.append("<body>");
            resstr.append("<H1>Install Licence Of Server " + pubSubDispatcher.getServerID() + "</H1>");
            resstr.append("<H3>" + result + "</H3>");
            resstr.append("</body>");
            resstr.append("</html>");
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.setContent(ChannelBuffers.copiedBuffer(resstr.toString(), CharsetUtil.UTF_8));

            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
            setContentLength(response, resstr.length());
            ChannelFuture future = e.getChannel().write(response);
            future.addListener(ChannelFutureListener.CLOSE);
        } catch (Exception e2) {
            logger.error("Unable to prepare licence. Ex=" + e2);
        }
        logger.info("Licence request received.");
    }

    private void dynamicGetConfigurationMessageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        logger.debug("GetConfiguration request received.");
        try {


            StringBuilder resstr = new StringBuilder();

            if (httpFileServer.getPubSubDispatcher() == null) {
                logger.error("Dispatcher not defined in web server. Check your pubsub.xml");
            }
            resstr.append(httpFileServer.getPubSubDispatcher().getConfigurationAsJson());


            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.headers().set("Access-Control-Allow-Origin", "*");
            response.setContent(ChannelBuffers.copiedBuffer(resstr.toString(), CharsetUtil.UTF_8));

            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
            setContentLength(response, resstr.length());
            ChannelFuture future = e.getChannel().write(response);
            future.addListener(ChannelFutureListener.CLOSE);
            
        } catch (Exception e2) {
            logger.error("Error while computing route answer. Ex=" + e2);
        }
        logger.debug("GetConfiguration request sent.");
    }
    
    private void dynamicUpdateConfigurationMessageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        logger.debug("UpdateConfiguration request received.");
        try {
            StringBuilder resstr = new StringBuilder();

            if (httpFileServer.getPubSubDispatcher() == null) {
                logger.error("Dispatcher not defined in web server. Check your pubsub.xml");
                resstr.append("error:no_pub_sub_defined");
            } else {
                try {
                    // We update the pubsub
                    httpFileServer.getPubSubDispatcher().applyNewConfiguration();
                    
                    // We update the vizualizer
                    httpFileServer.prepareVisualizerListener();
                    
                    // We send the update notification
                    httpFileServer.getPubSubDispatcher().sendUpdateNotification();
                    resstr.append("done");
                } catch (Exception ex) {
                    resstr.append("error:" + ex);
                }
            }
            
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.headers().set("Access-Control-Allow-Origin", "*");
            response.setContent(ChannelBuffers.copiedBuffer(resstr.toString(), CharsetUtil.UTF_8));

            response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
            setContentLength(response, resstr.length());
            ChannelFuture future = e.getChannel().write(response);
            future.addListener(ChannelFutureListener.CLOSE);
            
        } catch (Exception e2) {
            logger.error("Error while computing route answer. Ex=" + e2);
        }
        logger.debug("UpdateConfiguration request sent.");
    }

    private void DeserializeOneLine(String theline, HashMap res) throws Exception {
        String[] splitequal = theline.split("=");
        String[] splitpipe = splitequal[0].split("\\|");
        if (splitpipe.length == 1) {
            int index1 = theline.indexOf("=");
            int index2 = theline.indexOf("=", index1 + 1);

            if (splitpipe[0].equals("string")) {

                res.put(splitequal[1], theline.substring(index2 + 1));
            } else if (splitpipe[0].equals("float")) {
                try {
                    res.put(splitequal[1], new Float(theline.substring(index2 + 1)));
                } catch (Exception eeee) {
                    throw new Exception("Unable to decode float field=" + splitequal[1] + " value=" + theline.substring(index2 + 1));
                }

            } else if (splitpipe[0].equals("long")) {
                try {
                    res.put(splitequal[1], Long.valueOf(theline.substring(index2 + 1)));
                } catch (Exception eeee) {
                    throw new Exception("Unable to decode long field=" + splitequal[1] + " value=" + theline.substring(index2 + 1));
                }

            } else if (splitpipe[0].equals("int")) {
                try {
                    res.put(splitequal[1], Integer.valueOf(theline.substring(index2 + 1)));
                } catch (Exception eeee) {
                    throw new Exception("Unable to decode int field=" + splitequal[1] + " value=" + theline.substring(index2 + 1));
                }

            } else if (splitpipe[0].equals("bool")) {
                res.put(splitequal[1], Boolean.valueOf(theline.substring(index2 + 1)));
            }
        } else {
            HashMap next;
            if (res.get(splitpipe[0]) == null) {
                next = new HashMap();
                res.put(splitpipe[0], next);
            } else {
                next = (HashMap) res.get(splitpipe[0]);
            }

            int index = theline.indexOf('|');
            DeserializeOneLine(theline.substring(index + 1), next);
        }
    }

    private HashMap Deserialize(String mes) {
        HashMap res = new HashMap();
        String[] split = mes.replace("\r", "").split("\n");
        for (int i = 0; i < split.length; i++) {
            if (split[i].startsWith("Main")) {
                try {
                    DeserializeOneLine(split[i].substring(5), res);
                } catch (Exception ex) {
                    java.util.logging.Logger.getLogger(HttpFileServerHandler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return res;
    }

    private void dynamicGetSingletonMessageReceived(ChannelHandlerContext ctx, MessageEvent e, Boolean useJson) {
        logger.info("GetSingleton request received.");
        try {
            HttpRequest request = (HttpRequest) e.getMessage();

            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
            Map<String, List<String>> params = queryStringDecoder.getParameters();
            String singleton = params.get("Singleton").get(0);

            StringBuilder resstr = new StringBuilder();

            if (httpFileServer.getPubSubDispatcher() == null) {
                logger.error("Dispatcher not defined in web server. Check your pubsub.xml");
            }

            try {
                String content = httpFileServer.getPubSubDispatcher().getSingletonBean().getSingletonContent(singleton + ".txt");
                logger.info("Loading singleton.");

                try {
                    if (useJson) {
                        Gson gson = new Gson();
                        content = gson.toJson(Deserialize(content));
                    }
                } catch (Exception excp) {
                    logger.error("Error when gson, error is " + excp);
                }
                resstr.append(content);
            } catch (Exception e3) {
                logger.error("Unable to retrieve singleton. Ex=" + e3.getMessage(), e3);
            }

            logger.info("Creating response.");

            
            HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
            response.headers().set("Access-Control-Allow-Origin", "*");
            ChannelBuffer buf = ChannelBuffers.copiedBuffer(resstr.toString(), CharsetUtil.UTF_8);
            response.setContent(buf);

            if (useJson) {
                response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=UTF-8");
            } else {
                response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
            }
            //setContentLength(response, buf.);
            
            
            ChannelFuture future = e.getChannel().write(response);
            future.addListener(ChannelFutureListener.CLOSE);
        } catch (Exception e2) {
            logger.error("Error while computing singleton request answer. Ex=" + e2);
        }

        logger.info("Singeleton request sent.");
    }
}
