package com.infarmbureau.ea.aws.lambda.poc.java.basic;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3TriggerEventHandler implements RequestHandler<S3Event, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3TriggerEventHandler.class);

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.joda.JodaModule());

    @Override
    public String handleRequest(S3Event s3event, Context context) {

        long startTime = System.currentTimeMillis();

        try {

            LOGGER.debug("Preparing to stringify S3 Event JSON");
            String eventJson = objectMapper.writeValueAsString(s3event);
            LOGGER.debug("Stringified S3 Event JSON: " + eventJson);

            S3EventNotificationRecord record = s3event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getUrlDecodedKey();

//            S3Client s3Client = S3Client.builder().build();
//            HeadObjectResponse headObject = getHeadObject(s3Client, srcBucket, srcKey);

            LOGGER.debug("Received notification of file " + srcKey + " uploaded to " + srcBucket);

            return "Ok";
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            long endTime = System.currentTimeMillis();
            LOGGER.debug("Execution time: " + (endTime - startTime) + "ms");
        }
    }

}
