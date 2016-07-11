package com.github.yaud.avroipc;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

/**
 * Client to get Avro messages over TCP/IP from this test server:
 * https://github.com/linkedin/goavro/blob/master/examples/net/server.go
 */
public class Client {

	/**
	 * Simple wrapper that reports EOF as soon as there's no data available
	 * without waiting for read timeout 
	 */
	private static class FastEofDetectionStream extends BufferedInputStream {
		public FastEofDetectionStream(InputStream in) {
			super(in);
		}

		@Override
		public synchronized int read(byte[] b, int off, int len) throws IOException {
			return available() == 0 ? -1 : super.read(b, off, len);
		}

		@Override
		public synchronized int read() throws IOException {
			return available() == 0 ? -1 : super.read();
		}
	}

	public static void main(String[] args) throws UnknownHostException, IOException {
		String host = args.length > 1 ? args[0] : "localhost";
		int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;

		DatumReader<SpecificRecord> reader = new SpecificDatumReader<SpecificRecord>();

		try (
			Socket s = new Socket(host, port);
			InputStream is = new FastEofDetectionStream(s.getInputStream());
			DataFileStream<SpecificRecord> stream = new DataFileStream<SpecificRecord>(is, reader);
		) {
			while (stream.hasNext()) {
				System.out.println(stream.next());
			}
		}
	}
}
