package paxos;

import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable {

    // Constant Strings
    public static final String PREPARE_OKAY = "PREPARE_OKAY";
    public static final String PREPARE_REJECT = "PREPARE_REJECT";
    public static final String ACCEPT_OKAY = "ACCEPT_OKAY";
    public static final String ACCEPT_REJECT = "ACCEPT_REJECT";
    public static final String DECIDED_OKAY = "DECIDED_OKAY";

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    int hopefullyConcurrentSequence;
    Object hopefullyConcurrentValue;

    // sequence -> state
    HashMap<Integer, State> seqStateMap = new HashMap<>();

    // sequence -> decided object
    HashMap<Integer, Object> seqDecidedValueMap = new HashMap<>();

    // sequence -> highest prepared
    HashMap<Integer, Integer> seqHighestPreparedMap = new HashMap<>();

    // sequence -> highest accepted ID
    // sequence -> highest accepted object
    HashMap<Integer, Integer> seqHighestAcceptedIdMap = new HashMap<>();
    HashMap<Integer, Object> seqHighestAcceptedObjectMap = new HashMap<>();

    List<Integer> doneArray;

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports) {

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);
        this.doneArray = new ArrayList<Integer>();
        for (int i = 0; i < peers.length; ++i)
            this.doneArray.add(-1);

        // Your initialization code here

        // register peers, do not modify this part
        try {
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id) {
        Response callReply = null;

        PaxosRMI stub;
        try {
            Registry registry = LocateRegistry.getRegistry(this.ports[id]);
            stub = (PaxosRMI) registry.lookup("Paxos");
            if (rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if (rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if (rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch (Exception e) {
            return null;
        }
        return callReply;
    }

    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value) {
        // Your code here

        if (!this.seqHighestPreparedMap.containsKey(seq)) {
            this.seqStateMap.put(seq, State.Pending);
            this.seqDecidedValueMap.put(seq, null);
            this.seqHighestPreparedMap.put(seq, 0);
            this.seqHighestAcceptedIdMap.put(seq, null);
            this.seqHighestAcceptedObjectMap.put(seq, null);
        }

        freeTillMin();

        Thread t_seq = new Thread(this);

        this.mutex.lock();
        this.hopefullyConcurrentSequence = seq;
        this.hopefullyConcurrentValue = value;

        t_seq.run();
    }

    @Override
    public void run() {
        // Your code here
        Integer sequence = this.hopefullyConcurrentSequence;
        Object value = this.hopefullyConcurrentValue;
        this.mutex.unlock();

        while (this.seqStateMap.get(sequence) != State.Decided) {

            Integer proposalId = (this.seqHighestPreparedMap.get(sequence)
                    - (this.seqHighestPreparedMap.get(sequence) % this.peers.length))
                    + this.peers.length + this.me;
            this.seqHighestPreparedMap.put(sequence, proposalId);

            Request request = new Request(sequence, proposalId, value);
            Integer okayed = 1;
            Integer highestAcceptedId = -1;
            Object highestAcceptedValue = value;
            Integer majority = (int) (this.peers.length / 2) + 1;
            for (int i = 0; i < this.peers.length; i++) {
                if (i == this.me)
                    continue;
                Response response = this.Call("Prepare", request, i);
                if (response == null)
                    continue;
                if (response.getResponseType().equalsIgnoreCase(PREPARE_OKAY)) {
                    this.doneArray.set(i, response.getDone());
                    okayed++;
                    Optional<Integer> optResponseHighestAcceptedId = Optional
                            .ofNullable(response.getAcceptedProposalId());
                    if (optResponseHighestAcceptedId.isPresent()) {
                        Integer responseHighestAcceptedId = optResponseHighestAcceptedId.get();
                        if (responseHighestAcceptedId > highestAcceptedId) {
                            highestAcceptedId = responseHighestAcceptedId;
                            highestAcceptedValue = response.getAcceptedValue();
                        }
                    }
                }

                if (okayed >= majority) {
                    break;
                }
            }

            if (okayed < majority) // did not get majority, try proposing again
                continue;
            okayed = 1;
            request = new Request(sequence, proposalId, highestAcceptedValue);
            for (int i = 0; i < this.peers.length; i++) {
                if (i == this.me)
                    continue;
                Response response = this.Call("Accept", request, i);
                if (response == null)
                    continue;
                this.doneArray.set(i, response.getDone());
                if (response.getResponseType().equalsIgnoreCase(ACCEPT_OKAY)) {
                    okayed++;
                }
                if (okayed >= majority)
                    break;
            }

            if (okayed < majority)
                continue;

            this.seqDecidedValueMap.put(sequence, highestAcceptedValue);
            this.seqStateMap.put(sequence, State.Decided);

            for (int i = 0; i < this.peers.length; i++) {
                if (i == this.me)
                    continue;
                Response response = this.Call("Decide", request, i);
                if (response != null)
                    this.doneArray.set(i, response.getDone());
            }
        }
    }

    // RMI handler
    public Response Prepare(Request request) {
        // your code here

        int sequence = request.getSequence();

        // If sequence not seen before - initialise the maps
        if (!this.seqHighestPreparedMap.containsKey(sequence)) {
            this.seqStateMap.put(sequence, State.Pending);
            this.seqDecidedValueMap.put(sequence, null);
            this.seqHighestPreparedMap.put(sequence, -1);
            this.seqHighestAcceptedIdMap.put(sequence, null);
            this.seqHighestAcceptedObjectMap.put(sequence, null);

        }

        // If the request proposal ID is equal to the highest seen so far -> we will
        // accept it since it is our own
        // If request proposal ID is greater than any seen so far -> send a
        // prepare_okay, accepted_proposal_id, accepted_value
        if (request.getProposalId() >= this.seqHighestPreparedMap.get(sequence)) {
            this.seqHighestPreparedMap.put(sequence, request.getProposalId());
            return new Response(
                    PREPARE_OKAY,
                    this.seqHighestAcceptedIdMap.get(sequence),
                    this.seqHighestAcceptedObjectMap.get(sequence),
                    this.doneArray.get(this.me));
        }

        // If request proposal ID is lesser than maximum seen so far -> prepare reject
        else {
            return new Response(
                    PREPARE_REJECT,
                    this.doneArray.get(this.me));
        }
    }

    public Response Accept(Request request) {
        // your code here

        // If request proposal ID is greater than or equal to any seen so far ->
        // accept_okay
        int sequence = request.getSequence();
        int requestProposalId = request.getProposalId();

        if (requestProposalId == this.seqHighestPreparedMap.get(sequence)) {
            this.seqHighestPreparedMap.put(sequence, requestProposalId);
            this.seqHighestAcceptedIdMap.put(sequence, requestProposalId);
            this.seqHighestAcceptedObjectMap.put(sequence, request.getValue());
            return new Response(
                    ACCEPT_OKAY,
                    this.doneArray.get(this.me));
        }
        // If request proposal ID is lesser than any seen so far -> accept_reject
        else {
            return new Response(
                    ACCEPT_REJECT,
                    this.doneArray.get(this.me));
        }

    }

    public Response Decide(Request request) {
        // your code here
        this.seqDecidedValueMap.put(request.getSequence(), request.getValue());
        this.seqStateMap.put(request.getSequence(), State.Decided);

        return new Response(
                DECIDED_OKAY,
                this.doneArray.get(this.me));
    }

    /*
     * Function that removes unnecessary mappings corresponding to sequences that
     * are below the min value
     */
    private void freeTillMin() {
        int min = Min();

        this.seqStateMap.keySet().stream().filter(
                key -> key < min).forEach(
                        key -> {
                            seqStateMap.remove(key);
                            seqDecidedValueMap.remove(key);
                            seqHighestPreparedMap.remove(key);
                            seqHighestAcceptedIdMap.remove(key);
                            seqHighestAcceptedObjectMap.remove(key);
                        });
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        doneArray.set(this.me, seq);
    }

    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max() {
        // Your code here
        if (seqStateMap.keySet().size() == 0) {
            return -1;
        }
        return Collections.max(this.seqStateMap.keySet());
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().
     * 
     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.
     * 
     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.
     * 
     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min() {
        // Your code here
        return Collections.min(this.doneArray) + 1;
    }

    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq) {
        // Your code here
        if (seq < this.Min())
            return new retStatus(State.Forgotten, null);

        return new retStatus(this.seqStateMap.get(seq), this.seqDecidedValueMap.get(seq));
    }

    /**
     * helper class for Status() return
     */
    public class retStatus {
        public State state;
        public Object v;

        public retStatus(State state, Object v) {
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill() {
        this.dead.getAndSet(true);
        if (this.registry != null) {
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch (Exception e) {
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead() {
        return this.dead.get();
    }

    public void setUnreliable() {
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable() {
        return this.unreliable.get();
    }
}
