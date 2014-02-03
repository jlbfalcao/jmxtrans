package com.googlecode.jmxtrans.model.output;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.googlecode.jmxtrans.model.Query;
import com.googlecode.jmxtrans.model.Result;
import com.googlecode.jmxtrans.util.BaseOutputWriter;
import com.googlecode.jmxtrans.util.ValidationException;
import net.intelie.gozirra.Client;
import net.intelie.gozirra.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Basic filter good for testing that just outputs the Result objects using
 * System.out.
 *
 * @author jon
 */
public class StompOutWriter extends BaseOutputWriter {

    private static final Logger log = LoggerFactory.getLogger(StompOutWriter.class);

    private final Gson gson;
    private Client c;
    private Boolean debug = false;
    private String host = null;
    private Integer port = 61613;
    private String user = null;
    private String password = null;
    private String queue = "events";

    public StompOutWriter() {
        gson = new GsonBuilder().create();
    }

    /**
     * nothing to validate
     */
    public void validateSetup(Query query) throws ValidationException {

        if ("true".equals(this.getSettings().get("debug"))) {
            debug = true;
        }
        String p = (String) this.getSettings().get("port");
        if (p != null) {
            port = Integer.parseInt(p);
        }
        String q = (String) this.getSettings().get("queue");
        if (q != null) {
            queue = q;
        }
        host = (String) this.getSettings().get("host");
        user = (String) this.getSettings().get("user");
        password = (String) this.getSettings().get("password");
        connect();
    }

    public void doWrite(Query query) {

        Map<String, Object> json = new HashMap<String, Object>();

        // header
        HashMap headers = new HashMap();
        headers.put("eventtype", query.getResultAlias());
        headers.put("timestamp", new Date().getTime());

        for (Result r : query.getResults()) {
            log.debug(r.toString());

            for (Map.Entry<String, Object> v : r.getValues().entrySet()) {
                String key = r.getAttributeName() + "_" + v.getKey();
                Object vl = v.getValue();
                json.put(key, v.getValue());
            }
        }
        json.put("host", query.getServer().getHost());

        checkAndConnect();
        c.send("/queue/" + queue, gson.toJson(json), headers);

        // debug
        log.info(String.format("headers %s", gson.toJson(headers)));
        log.info(String.format("body: %s", gson.toJson(json)));
    }

    private void checkAndConnect() {
        if (!c.isConnected()) {
            connect();
        }
    }

    private void connect() {
        log.info(String.format("connect: %s %d %s", host, port, user));
        try {
            c = new Client(host, port, user, password);
        } catch (LoginException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
