package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;

    // your data here
    String responseType;
    Integer acceptedProposalId;
    Object acceptedValue;
    Integer done;

    public Response(String responseType, Integer done) {
        this.responseType = responseType;
        this.acceptedProposalId = null;
        this.acceptedValue = null;
        this.done = done;
    }

    public Response(String responseType, Integer proposalId, Object value, Integer done) {
        this.responseType = responseType;
        this.acceptedProposalId = proposalId;
        this.acceptedValue = value;
        this.done = done;
    }

    // TODO: make this code nice
    public String getResponseType() {
        return this.responseType;
    }

    public Integer getAcceptedProposalId() {
        return this.acceptedProposalId;
    }

    public Object getAcceptedValue() {
        return this.acceptedValue;
    }

    public Integer getDone() {
        return this.done;
    }

    // Your constructor and methods here
}
