package bgu.spl.mics.impl;

import bgu.spl.mics.Broadcast;
import bgu.spl.mics.Message;
import bgu.spl.mics.MessageBus;
import bgu.spl.mics.MicroService;
import bgu.spl.mics.Request;
import bgu.spl.mics.RequestCompleted;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
/**
 * This class implements {@link MessageBus}.
 * It is implemented as a thread-safe singleton.
 * It is a shared object used for communication between {@link MicroService}s.
 */
public class MessageBusImpl implements MessageBus{
	private ConcurrentHashMap<MicroService, LinkedBlockingQueue<Message>> _MSque;
	private ConcurrentHashMap<MicroService, CopyOnWriteArrayList<String>> _MSsub;
	private ConcurrentHashMap<String, RoundRobinQueue<MicroService>> _MESSAGES;
	private ConcurrentHashMap<Message, MicroService> _REQ;
	private Object _lock1 = new Object();
	private Object _lock2 = new Object();
	private Object _lock3 = new Object();



	private static class MessageBusHolder{
		private static MessageBusImpl instance = new MessageBusImpl();
	}
	private MessageBusImpl(){
		_MSque=new ConcurrentHashMap<>();
		_MSsub=new ConcurrentHashMap<>();
		_MESSAGES=new ConcurrentHashMap<>();
		_REQ=new ConcurrentHashMap<>();

	}

	public static MessageBusImpl getInstance(){
		return MessageBusHolder.instance;
	}

	public  void subscribeRequest(Class<? extends Request> type, MicroService m) {
		if(!_MSsub.containsKey(m))
			_MSsub.put(m,new CopyOnWriteArrayList<String>());
		AddSubscribe(type,m);
	}

	public  void subscribeBroadcast(Class<? extends Broadcast> type, MicroService m) {
		if(!_MSsub.containsKey(m))
			_MSsub.put(m,new CopyOnWriteArrayList<String>());
		AddSubscribe(type,m);
	}

	public <T> void complete(Request<T> r, T result) { 
		RequestCompleted<T> requestCompleted = new RequestCompleted<T>(r,result);
		MicroService ms = _REQ.get(r); // get the requester, using the request as the key
		if(ms!=null){
			BlockingQueue<Message> que= _MSque.get(ms); 
			synchronized (_lock3) {// locks the MS's queue (for unregistering purpose)
				if(que!=null){
					que.add(requestCompleted); //adds the completed request to the sender's queue.
				}
			}
		}
		_REQ.remove(r);
	}

	public void sendBroadcast(Broadcast b) {
		String type = b.getClass().getName();
		if (_MESSAGES.containsKey(type)){// get the Specific Msg's queue.
			synchronized (_lock3) {// in case we are sending a broadcast to a unregistering MicroService
				Iterator<MicroService> it = _MESSAGES.get(type).iterator();
				while(it.hasNext()){
					try {
						MicroService ms = it.next();
						_MSque.get(ms).put(b);//adds the broadcast to the subscriber's queue.
					} catch (InterruptedException e) {
						System.out.println("thrown exception at Broadcast");
						e.printStackTrace();
					}
				}
			}
		}
	}

	public boolean sendRequest(Request<?> r, MicroService requester) {
		_REQ.put(r, requester);
		if(!_MESSAGES.containsKey(r.getClass().getName()) || _MESSAGES.get(r.getClass().getName()).isEmpty())
			return false;
		synchronized (_lock3){// in case we are sending a request to a unregistering MicroService
			_MSque.get(_MESSAGES.get(r.getClass().getName()).nextOnQueue()).add(r);
			return true;
		}
	}

	public void register(MicroService m) {
		_MSque.put(m,new LinkedBlockingQueue<Message>());
	}

	public void unregister(MicroService m) { 
		if (_MSsub.containsKey(m)){
			Iterator<String> it = _MSsub.get(m).iterator();
			synchronized(_lock1){//to prevent a situation adding a Message to the MicroService's queue while is he in the progress of unregister.
				while(it.hasNext()){
					_MESSAGES.get(it.next()).remove(m);	
				}
			}
		}
		_MSsub.remove(m);
		synchronized (_lock3){// prevents adding msgs to the MS's que 
			_MSque.remove(m);	
		}

	}

	public Message awaitMessage(MicroService m) throws InterruptedException {
		if (!_MSque.containsKey(m)) throw new IllegalArgumentException("Micro-Servise "+m.getName()+" is unregistered");
		return _MSque.get(m).take(); //Retrieves and removes the head of the queue, waiting if necessary until an element becomes available.
	}

	private void AddSubscribe(Class<?> type, MicroService m){

		_MSsub.get(m).add(type.getName());
		synchronized(_lock2){// we created the lock here to prevent a creation of 2+ round robin ques for the same msg type.
			if(!_MESSAGES.containsKey(type.getName())){ 
				_MESSAGES.put(type.getName(),new RoundRobinQueue<MicroService>());
			}
		}		
		_MESSAGES.get(type.getName()).add(m);
	}
	//test!!!!!!!!!!!!!!
}