package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

/**
 * Created by Nikhil on 4/24/15.
 */
public class LoadCarrier implements Serializable {

    public final HashMap<String, String> map;
    public HashMap<String, String> queryMap;
    public String messageType;
    public String owner, queryKey;
    public int timeToLive;
    public int whoToSendBack;
    public String temp;
    public String uuid;

    public LoadCarrier(HashMap map, String action, String uuid){

        this.map = map;
        this.uuid = uuid;

        if(action.equals("wakeup"))
            this.messageType = "11";
    }
    /*
    This constructor is for insertion purposes only
     */
    public LoadCarrier(HashMap map, String action, int whoToSendBack, String uuid){

        this.timeToLive = 4;
        this.whoToSendBack = whoToSendBack;
        this.map = map;
        this.uuid = uuid;

        if(action.equals("insert"))
            this.messageType = "56";
    }

    public LoadCarrier(String action, int timeToLive, String owner, int whoToSendBack){

        this.timeToLive = timeToLive;
        this.owner = owner;
        this.whoToSendBack = whoToSendBack;
        this.map = null;

        if(action.equals("recoveryQuery"))
            this.messageType = "57";
    }

    public LoadCarrier(String action, int whoToSendBack, int timeToLive, String uuid){

        this.timeToLive = timeToLive;
        this.whoToSendBack = whoToSendBack;
        this.map = null;
        this.queryMap = new HashMap<>();
        this.uuid = uuid;

        if(action.equals("*query"))
            this.messageType = "58";
    }

    public LoadCarrier(String action, int whoToSendBack, String queryKey, String uuid){

        this.timeToLive = 4;
        this.whoToSendBack = whoToSendBack;
        this.queryKey = queryKey;
        this.uuid = uuid;
        map = null;

        if(action.equals("query"))
            this.messageType = "59";

        if(action.equals("delete"))
            this.messageType = "60";
    }

    public LoadCarrier(String action, int timeToLive, String uuid){

        this.timeToLive = timeToLive;
        map = null;
        this.uuid = uuid;
        this.temp = "Ack";

        if(action.equals("alive"))
            this.messageType = "1";
    }

    public String toString(){ return messageType; }
}