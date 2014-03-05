package org.nts.pigutils.main;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import com.hadoop.compression.lzo.LzopCodec;

/**
 * Helper class to convert from GPB base64 to GPB
 */
public class ProtoConvert {

	public static final void main(String[] args) throws Throwable {

		if (args.length > 0) {
			readFile(args[0]);
		} else {
			Base64 base64 = new Base64(0);

			final BufferedReader reader = new BufferedReader(
					new InputStreamReader(System.in));
			final BufferedOutputStream out = new BufferedOutputStream(
					System.out);

			try {

				String line = null;
				while ((line = reader.readLine()) != null) {
					final byte[] bts = removeBase64(base64, line.getBytes());
					writeInt(out, bts.length);
					out.write(bts);
				}
			} finally {
				out.close();
				reader.close();
			}
		}
	}

	private static void readFile(String file) throws IOException {
		File f = new File(file);
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(f));
		
		LzopCodec codec = new LzopCodec();
		if(codec instanceof Configurable){
			((Configurable)codec).setConf(new Configuration());
		}
		
		DataInputStream din = new DataInputStream(codec.createInputStream(in));
		try{
			while(din.available() > 0){
				final int size = din.readInt();
				byte[] arr = new byte[size];
				din.read(arr);
				System.out.println("read [" + size + "] " + arr);
			}
		}finally{
			din.close();
		}
	}

	private static final void writeInt(OutputStream out, int v)
			throws IOException {
		out.write((byte) (0xff & (v >> 24)));
		out.write((byte) (0xff & (v >> 16)));
		out.write((byte) (0xff & (v >> 8)));
		out.write((byte) (0xff & v));
	}

	private static final byte[] removeBase64(Base64 base64, byte[] bts) {
		return base64.decode(bts);
	}

	private static void printHelp() {
		System.out.println("[source-file] [output] use STDIN and STDOUT");
	}

}
