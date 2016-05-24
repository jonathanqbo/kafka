package kafka.simple;

public class Main {

	public static void main(String[] args) {
		
		new Thread(new MyConsumer("1")::run).start();;
		new Thread(new MyConsumer("2")::run).start();;
		new Thread(new MyConsumer("3")::run).start();;
		new Thread(new MyConsumer("4")::run).start();;
		
		new Thread(new MyProducer()::run).start();;
		
	}
	
}
