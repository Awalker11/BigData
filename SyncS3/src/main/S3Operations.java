package main;

import com.amazonaws.SdkClientException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import javax.annotation.PostConstruct;
import java.io.File;
import java.util.ArrayList;
import java.util.List;


public class S3Operations {

    @Autowired
    S3ConnectionManager s3ConnectionManager;

    private AmazonS3 s3;

    private String bucketName;
    
    private static long partSize = 5 << 20;
	private static long maxSize = 20 << 20;

    @PostConstruct
    public void postConstruct() {
        s3 = s3ConnectionManager.getAmazonS3();
        bucketName = "yuxuxu";
    }

    public List<Bucket> getS3Buckets() {
        return s3.listBuckets();
    }

    public void createFile(File file) {
        try {
        	if(file.length() >= maxSize) {
				this.multPartUpload(file.getName(), file);
			} else {
            s3.putObject(bucketName, file.getName(), file);
			}
        } catch (SdkClientException ex) {
            System.out.println("Error: " + ex.getMessage());
        }
    }

    public void deleteFile(File file) {
        try {
            s3.deleteObject(bucketName, file.getName());
        } catch (SdkClientException ex) {
            System.out.println("Error: " + ex.getMessage());
        }
    }

    public void modifyFile(File file) {
        deleteFile(file);
        createFile(file);
    }
    
    public void multPartUpload(String key, File file) {
    	ArrayList<PartETag> partETags = new ArrayList<PartETag>();
    	String uploadId = null;
		long contentLength = file.length();
		try {
			InitiateMultipartUploadRequest initRequest = 
					new InitiateMultipartUploadRequest(bucketName, key);
			uploadId = s3.initiateMultipartUpload(initRequest).getUploadId();
			long filePosition = 0;
			for (int i = 1; filePosition < contentLength; i++) {
				partSize = Math.min(partSize, contentLength - filePosition);
				UploadPartRequest uploadRequest = new UploadPartRequest()
						.withBucketName(bucketName)
						.withKey(key)
						.withUploadId(uploadId)
						.withPartNumber(i)
						.withFileOffset(filePosition)
						.withFile(file)
						.withPartSize(partSize);
				System.out.format("Uploading part %d\n", i);
				partETags.add(s3.uploadPart(uploadRequest).getPartETag());
				filePosition += partSize;
			}
			System.out.println("Completing upload");
			CompleteMultipartUploadRequest compRequest = 
					new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags);
			s3.completeMultipartUpload(compRequest);
		} catch (Exception e) {
			System.err.println(e.toString());
			if (uploadId != null && !uploadId.isEmpty()) {
				System.out.println("Aborting upload");
				s3.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
			}
			System.exit(1);
		}
	}
    
    
    
}