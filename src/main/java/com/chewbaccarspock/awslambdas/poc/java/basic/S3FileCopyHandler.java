package com.chewbaccarspock.awslambdas.poc.java.basic;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.nio.ByteBuffer;

public class S3FileCopyHandler implements RequestHandler<S3Event, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PolicyPurchaseProcessor.class);
    private static final String DESTINATION_BUCKET = "chewbaccar-spock-s3-write-bucket-for-serverless-poc"; // Replace with your destination bucket

    private final S3Client s3Client;
    private final ObjectMapper objectMapper;

    public S3FileCopyHandler() {
        try {

            LOGGER.debug("Preparing to initialize S3Client");
            s3Client = S3Client.builder().build();
            LOGGER.debug("Initialized S3Client");

            LOGGER.debug("Preparing to initialize ObjectMapper");
            objectMapper = new ObjectMapper()
                    .registerModule(new com.fasterxml.jackson.datatype.joda.JodaModule());
            LOGGER.debug("Initialized ObjectMapper");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public String handleRequest(S3Event s3Event, Context context) {

        long startTime = System.currentTimeMillis();

        try {

            String eventJson = objectMapper.writeValueAsString(s3Event);
            LOGGER.debug("Received S3 Event JSON: " + eventJson);

            // Get bucket and object key from the S3 event
            String sourceBucket = s3Event.getRecords().get(0).getS3().getBucket().getName();
            String sourceKey = s3Event.getRecords().get(0).getS3().getObject().getKey();

            LOGGER.debug("Source Bucket: " + sourceBucket);
            LOGGER.debug("Source Key: " + sourceKey);

            LOGGER.debug("Preparing to read object from the source bucket");
            // Read the object from the source bucket
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build();
            ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(getObjectRequest);
            LOGGER.debug("Read object from the source bucket");

            LOGGER.debug("Preparing to write the object to the destination bucket");
            // Write the object to the destination bucket
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(DESTINATION_BUCKET)
                    .key(sourceKey) // Maintain the same key in the destination bucket
                    .build();
            PutObjectResponse putObjectResponse = s3Client.putObject(putObjectRequest,
                    RequestBody.fromByteBuffer(ByteBuffer.wrap(objectBytes.asByteArray())));
            LOGGER.debug("Wrote the object to the destination bucket");

            LOGGER.debug("File transferred to destination bucket successfully.");
            return "File transferred successfully: " + putObjectResponse.eTag();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            long endTime = System.currentTimeMillis();
            LOGGER.debug("Execution time: " + (endTime - startTime) + "ms");
        }
    }
}
