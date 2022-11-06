package kvpaxos;

import paxos.Paxos;
import paxos.State;
// You are allowed to call Paxos.Status to check if agreement was made.
import paxos.Paxos.retStatus;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements KVPaxosRMI {

    public static final String GET = "GET";
    public static final String PUT = "PUT";

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    Map<String, Integer> kvStore;
    Integer logSequence;

    // TODO: Duplicate detection (piggyback??)
    public Server(String[] servers, int[] ports, int me) {
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);
        // Your initialization code here
        this.kvStore = new HashMap<>();
        this.logSequence = 0;

        try {
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Op waitTillDecided(int logSequence) {
        int timeout = 10;
        while (true) {
            retStatus ret = this.px.Status(logSequence);
            if (ret.state == State.Decided)
                return (Op) ret.v;
            try {
                Thread.sleep(timeout);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (timeout < 1000) {
                timeout = timeout * 2;
            }
        }
    }

    // RMI handlers
    // TODO: timeout to wait for consensus to be arrived at??
    public Response Get(Request request) {
        // Your code here

        for (; logSequence <= px.Max(); logSequence++) {
            retStatus ret = px.Status(logSequence);

            Op updateOp = (Op) ret.v;
            if (updateOp.getOp().equalsIgnoreCase(PUT)) {
                this.kvStore.put(updateOp.getKey(), updateOp.getValue());
            }
            px.Done(logSequence);
        }

        Boolean updated = false;
        Op op = new Op(GET, logSequence, request.getKey(), null);
        while (!updated) {
            px.Start(logSequence, op);
            Op decidedOp = this.waitTillDecided(logSequence);

            if (!this.isOpEqual(decidedOp, op)) {
                if (decidedOp.getOp().equalsIgnoreCase(PUT)) {
                    this.kvStore.put(decidedOp.getKey(), decidedOp.getValue());
                }
                px.Done(logSequence);
            } else {
                updated = true;
            }
            px.Done(logSequence);
            logSequence += 1;
        }
        return new Response(this.kvStore.get(request.getKey()));

    }

    // TODO: timeout to wait for consensus to be arrived at??
    public Response Put(Request request) {
        // Your code here
        for (; logSequence <= px.Max(); logSequence++) {
            retStatus ret = px.Status(logSequence);

            Op updateOp = (Op) ret.v;
            if (updateOp.getOp().equalsIgnoreCase(PUT)) {
                this.kvStore.put(updateOp.getKey(), updateOp.getValue());
            }
            px.Done(logSequence);
        }

        Boolean updated = false;
        Op op = new Op(PUT, logSequence, request.getKey(), request.getValue());
        while (!updated) {
            px.Start(logSequence, op);
            Op decidedOp = this.waitTillDecided(logSequence);

            if (!this.isOpEqual(decidedOp, op)) {
                if (decidedOp.getOp().equalsIgnoreCase(PUT)) {
                    this.kvStore.put(decidedOp.getKey(), decidedOp.getValue());
                }
                px.Done(logSequence);
            } else {
                this.kvStore.put(op.getKey(), op.getValue());
                updated = true;
            }
            px.Done(logSequence);
            logSequence += 1;
        }
        return new Response(true);
    }

    private boolean isOpEqual(Op op1, Op op2) {
        return op1.getOp().equalsIgnoreCase(op2.getOp())
                && op1.getClientSeq() == op2.getClientSeq()
                && op1.getKey().equalsIgnoreCase(op2.getKey())
                && op1.getValue() == op2.getValue();
    }
}
