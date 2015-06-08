package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by Nikhil on 4/26/15.
 */
public class OnStartQueryTask implements Runnable {

    @Override
    public void run() {

        int index = (TiredOfClasses.myPosition() - 2) % 5;
        int start = (index < 0)?(5 + index):index;
        int end = 0;

        while(end <= 2){
            query(start, end);
            start = (start + 1) % 5;
            end++;
        }
    }

    public void query(final int node, final int end){

        if(end == 0)
            new Sockets(node, new LoadCarrier("recoveryQuery", 3,
                    TiredOfClasses.ports[node], TiredOfClasses.myPosition), Sockets.pingOn);
        else
            new Sockets(node, new LoadCarrier("recoveryQuery", 4,
                    TiredOfClasses.ports[node], TiredOfClasses.myPosition), Sockets.pingOn);

    }

}
