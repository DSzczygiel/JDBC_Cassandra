package pl.dszczygiel.jdbc.utils;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

public class URLParser {

	public static ParsedURL parse(String url) throws URISyntaxException {
		String host = null;
		int port = 0;
		String path = null;
				
		url = url.substring(16);		
		URI uri = new URI(url);
		host = uri.getHost();
		port = uri.getPort();
		path = uri.getPath();
		
		if((path != null) && (!path.isEmpty()))
			path = uri.getPath().substring(1);
		
		List<NameValuePair> params = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8.displayName());
		
		ParsedURL parsedURL = new ParsedURL();
		try {
			parsedURL.host = InetAddress.getByName(host);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		parsedURL.port = port;
		parsedURL.path = path;
		parsedURL.parameters = params;
		return parsedURL;
	}
	
	public static class ParsedURL{
		private InetAddress host;
		private int port;
		private String path;
		private List<NameValuePair> parameters;
		public InetAddress getHost() {
			return host;
		}
		public int getPort() {
			return port;
		}
		public String getPath() {
			return path;
		}
		public String getParameterByName(String name) {
			for(NameValuePair p : parameters) {
				if(p.getName().equals(name))
					return p.getValue();
			}
			return null;
		}
		
	}
}
