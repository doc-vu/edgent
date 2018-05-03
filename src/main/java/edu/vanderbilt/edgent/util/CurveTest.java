package edu.vanderbilt.edgent.util;

import java.io.IOException;
import org.zeromq.ZAuth;
import org.zeromq.ZCert;
import org.zeromq.ZCertStore;
import org.zeromq.ZConfig;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class CurveTest {
	public static void certificateTest()
	{
		ZCert serverCert= new ZCert();
		try {
			serverCert.savePublic("/home/kharesp/testCert.pub");
			serverCert.saveSecret("/home/kharesp/testCert.txt");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void loadCertificate(){
		ZConfig config;
		try {
			config = ZConfig.load("/home/kharesp/testCert.txt");
			System.out.println(config.getValue("curve/public-key"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void testCurveAnyClient() throws IOException {
		System.out.println("testCurveAnyClient");
		// accept any client-certificate

		// Create context
		final ZContext ctx = new ZContext();
		try {
			ZAuth auth = new ZAuth(ctx, new ZCertStore.Hasher());

			// Get some indication of what the authenticator is deciding
			auth.setVerbose(true);
			// auth send the replies
			auth.replies(true);

			auth.configureCurve(ZAuth.CURVE_ALLOW_ANY);

			// We need two certificates, one for the client and one for
			// the server. The client must know the server's public key
			// to make a CURVE connection.
			ZCert clientCert = new ZCert();
			ZCert serverCert = new ZCert();

			// Create and bind server socket
			ZMQ.Socket server = ctx.createSocket(ZMQ.PUSH);
			server.setZapDomain("global".getBytes());
			server.setCurveServer(true);
			serverCert.apply(server);
			final int port = server.bindToRandomPort("tcp://*");

			// Create and connect client socket
			ZMQ.Socket client = ctx.createSocket(ZMQ.PULL);
			clientCert.apply(client);
			client.setCurveServerKey(serverCert.getPublicKey());
			boolean rc = client.connect("tcp://127.0.0.1:" + port);

			// Send a single message from server to client
			rc = server.send("Hello");

			//ZAuth.ZapReply reply = auth.nextReply();

			//System.out.println("ZAuth replied " + reply.toString());

			String message = client.recvStr();

			System.out.println("Received message:" + message);

			auth.close();
		} finally {
			ctx.close();
		}
	}
	
	public static void main(String argsp[]) throws IOException{
		certificateTest();
		loadCertificate();
	}

}
