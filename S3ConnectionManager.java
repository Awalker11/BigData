package s3demo;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;



public class S3ConnectionManager {

    private AmazonS3 amazonS3;

  
	private final static String accessKey = "C277DF0D97920BA6C4B3";
	private final static String secretKey = 
"W0Y5NkM1MDhBN0Y1NDE2N0NFNDNGMDNGOUYyQUVBRUQzNTkyOTU2RUZd";
	private final static String serviceEndpoint = 
			"http://scuts3.depts.bingosoft.net:29999";
	private final static String signingRegion = "";

    public void initS3Connection() {
    	final BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey,secretKey);
    	final ClientConfiguration ccfg = new ClientConfiguration().	withUseExpectContinue(true);

    	final EndpointConfiguration endpoint = 	new EndpointConfiguration(serviceEndpoint, signingRegion);
    			
    					
        amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(ccfg)
                .withEndpointConfiguration(endpoint)
                .withPathStyleAccessEnabled(true)
                .build();
    }

    public AmazonS3 getAmazonS3() {
        initS3Connection();
        return amazonS3;
    }

    public void setAmazonS3(AmazonS3 amazonS3) {
        this.amazonS3 = amazonS3;
    }

}
