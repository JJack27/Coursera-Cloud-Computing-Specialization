/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include <stdlib.h>
#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
        #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
        #endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
        #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
        #endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
    #ifdef DEBUGLOG
        static char s[1024];
    #endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
        #ifdef DEBUGLOG
            log->LOG(&memberNode->addr, "Starting up group...");
        #endif
        memberNode->inGroup = true;
    } else {
        size_t msgsize = sizeof(MessageHdr) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        // {msyType, address, heartbeat}
        msg->msgType = JOINREQ;
        msg->source = &memberNode->addr;
        msg->heartbeat = memberNode->heartbeat;

        #ifdef DEBUGLOG
            sprintf(s, "Trying to join...");
            log->LOG(&memberNode->addr, s);
        #endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
    #ifdef DEBUGLOG
        static char s[1024];
        sprintf(s, "Finish Up this node\t");
        log->LOG((Address *) &(memberNode->addr), s);
    #endif
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        #ifdef DEBUGLOG
            static char s[1024];
            sprintf(s, "Not in group\t");
            log->LOG((Address *) &(memberNode->addr), s);
        #endif
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	*/
    MessageHdr* msg = (MessageHdr*) data;
    switch (msg->msgType){
        case JOINREQ:{
            Address * srcAddr = msg->source;

            // add the source node into the membership_list
            MemberListEntry entry = parseRequest(msg);
            memberNode->memberList.push_back(entry);

            // printing log
            /*
            #ifdef DEBUGLOG
                char s[1024];
                sprintf(s, "Accept to memberlist (%d:%d), my_hb=%ld", 
                    entry.getid(), entry.getport(), msg->heartbeat);
                log->LOG((Address *) &(memberNode->addr), s);
            #endif
            */
            log->logNodeAdd(&memberNode->addr, msg->source);

            // Send JOINREP back to the source
            MessageHdr *res = nullptr;
            size_t msgsize = sizeof(MessageHdr) + 1;
            res = (MessageHdr *) malloc(msgsize * sizeof(char));

            res->msgType = JOINREP;
            res->source = &memberNode->addr;
            res->heartbeat = memberNode->heartbeat;
            res->memberList = &memberNode->memberList;

            // send JOINREQ message to introducer member
            emulNet->ENsend(&memberNode->addr, srcAddr, (char *)res, msgsize);
            free(res);
            break;
        }
        case JOINREP:{
            // add source node to the memberList
            MemberListEntry entry = parseRequest(msg);
            memberNode->memberList.push_back(entry);
            
            // update the status
            memberNode->inGroup = true;

            // update the node's member list
            updateMemberList(msg->memberList);

            // printing log
            log->logNodeAdd(&memberNode->addr, msg->source);

            break;
        }
        case HEARTBEAT:{
            MemberListEntry entry = parseRequest(msg);
            vector<MemberListEntry> temp;
            temp.push_back(entry);
            updateMemberList(&temp);
            updateMemberList(msg->memberList);
            break;
        } 
        default:{
            return false;
        }
    }
    
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    memberNode->heartbeat = memberNode->heartbeat + 1;

    // check if any of the member is died
    for (int i = memberNode->memberList.size()-1 ; i >= 0; i--) {
        if(par->getcurrtime() - memberNode->memberList[i].timestamp >= TREMOVE) {
            Address* removed_addr = buildAddr(memberNode->memberList[i].id, memberNode->memberList[i].port);
            log->logNodeRemove(&memberNode->addr, removed_addr);
            memberNode->memberList.erase(memberNode->memberList.begin()+i);
            delete removed_addr;
        }
    }

    // ramdonly select log2(n) nodes to send heartbeat
    int n = memberNode->memberList.size(); 
    int numSelected = n==1 ? 1 : log2(n);
    vector<int> selected;
    
    while(selected.size() < numSelected){
        int pick = rand() % memberNode->memberList.size();
        if(std::find(selected.begin(), selected.end(), pick) == selected.end()
            && pick != memberNode->addr.addr[0] - 48){
            selected.push_back(pick);
        }
    }
    
    // send heartbeats and memberlist to the selected peers
    for(auto peerIdx : selected){
        MemberListEntry toEntry = memberNode->memberList[peerIdx];
        Address * toAddr = new Address();
        toAddr->addr[0] = toEntry.getid();
		toAddr->addr[4] = toEntry.getport();

        // initialize message
        MessageHdr *msg = nullptr;
        size_t msgsize = sizeof(MessageHdr) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        msg->msgType = HEARTBEAT;
        msg->source = &memberNode->addr;
        msg->heartbeat = memberNode->heartbeat;
        msg->memberList = &memberNode->memberList;

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, toAddr, (char *)msg, msgsize);
        free(msg);
        delete toAddr;
    }
    return;
}



/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}


/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

/**
 * FUNCTION NAME: parseRequest
 *
 * DESCRIPTION: Parsing request to a MemberListEntry
 */
MemberListEntry MP1Node::parseRequest(MessageHdr * msg){
    int id = 0;
    short port = 0;
    long heartbeat = msg->heartbeat;
    long timestamp = par->getcurrtime();
    memcpy(&id, &msg->source->addr[0], sizeof(int));
	memcpy(&port, &msg->source->addr[4], sizeof(short));
    MemberListEntry entry = MemberListEntry(id, port, heartbeat, timestamp);
    return entry;
}

/**
 * FUNCTION NAME: updateMemberList
 *
 * DESCRIPTION: Sync my member list with the comming list
 */
void MP1Node::updateMemberList(vector<MemberListEntry> * commingList){
    // update my member list
    for(int i = 0; i < memberNode->memberList.size(); i++){
        for(int j = 0; j < commingList->size(); j++){
            if(memberNode->memberList[i].getid() == (*commingList)[j].getid()
            && memberNode->memberList[i].getport() == (*commingList)[j].getport()
            && memberNode->memberList[i].getheartbeat() < (*commingList)[j].getheartbeat()){
                memberNode->memberList[i].settimestamp(par->getcurrtime());
                memberNode->memberList[i].setheartbeat((*commingList)[j].getheartbeat());
            }
        }
    }

    
    // add new member into memberlist
    for(int i = 0; i < commingList->size(); i++){
        if((*commingList)[i].getid() == memberNode->addr.addr[0]
            && (*commingList)[i].getport() == memberNode->addr.addr[4]){
            continue;
        }
        bool inList = false;
        for(int j = 0; j < memberNode->memberList.size(); j++){
            if(memberNode->memberList[j].getid() == (*commingList)[i].getid()
                && memberNode->memberList[j].getport() == (*commingList)[i].getport()
                ){
                inList = true;
                break;
            }else if (par->getcurrtime() - (*commingList)[i].gettimestamp() >= TREMOVE){
                inList = true;
                break;
            }
        }

        if(!inList){
            memberNode->memberList.push_back((*commingList)[i]);

            // print graded log
            Address * addr = new Address();
            memcpy(&addr->addr[0], &(*commingList)[i].id, sizeof(int));
	        memcpy(&addr->addr[4], &(*commingList)[i].port, sizeof(short));
            log->logNodeAdd(&memberNode->addr, addr);
            delete addr;
        }
    }
}


Address * MP1Node::buildAddr(int id, short port){
    Address *addr = new Address();
    std::memcpy(&addr->addr[0], &id, sizeof(int));
	std::memcpy(&addr->addr[4], &port, sizeof(short));
    return addr;
}