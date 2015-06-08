package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by Nikhil on 5/7/15.
 */
public class LockFace {

    private Boolean lock;

    public LockFace(){
        this.lock = false;
    }

    public void openLock(){
        this.lock = true;
    }

    public Boolean getLock(){
        return this.lock;
    }

    public void setLock(){
        this.lock = false;
    }
}
