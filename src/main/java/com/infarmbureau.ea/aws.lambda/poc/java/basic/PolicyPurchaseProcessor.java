package com.infarmbureau.ea.aws.lambda.poc.java.basic;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

public class PolicyPurchaseProcessor implements RequestHandler<SNSEvent, Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PolicyPurchaseProcessor.class);

    private static final String SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/971422680638/sqs-target-for-basic-sns-to-sqs-trigger-function-vanilla-java";
    private final SqsClient sqsClient;
    private final ObjectMapper objectMapper;
    private final Transformer transformer;
    private final DocumentBuilder docBuilder;

    public PolicyPurchaseProcessor() {

        try {

            LOGGER.debug("Preparing to initialize SqsClient");
            sqsClient = SqsClient.builder().build();
            LOGGER.debug("Initialized SqsClient");

            LOGGER.debug("Preparing to initialize ObjectMapper");
            objectMapper = new ObjectMapper()
                    .registerModule(new com.fasterxml.jackson.datatype.joda.JodaModule());
            LOGGER.debug("Initialized ObjectMapper");

            LOGGER.debug("Preparing to initialize DocumentBuilder");
            docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            LOGGER.debug("Initialized DocumentBuilder");

            LOGGER.debug("Preparing to initialize Transformer");
            transformer = TransformerFactory.newInstance().newTransformer();
            LOGGER.debug("Initialized Transformer");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public Void handleRequest(SNSEvent event, Context context) {

        long startTime = System.currentTimeMillis();

        for (SNSEvent.SNSRecord record : event.getRecords()) {
            String snsMessage = record.getSNS().getMessage();
            LOGGER.debug("JSON Message: {}", snsMessage);

            try {
                LOGGER.debug("Preparing to parse JSON");
                PolicyPurchase policyPurchase = parseJsonToPolicyPurchase(snsMessage);
                LOGGER.debug("Parsed JSON");

                if (isPrivatePassengerVehicle(policyPurchase)) {
                    LOGGER.debug("Preparing to transform to XML");
                    String xmlMessage = transformToXml(policyPurchase);
                    LOGGER.debug("Transformed to XML; XML Message: {}", xmlMessage);

                    LOGGER.debug("Preparing to publish to SQS queue");
                    publishToSQS(xmlMessage);
                    LOGGER.debug("Published to SQS queue");
                } else {
                    LOGGER.debug("Skipping non-private passenger vehicle message.");
                }
            } catch (Exception e) {
                LOGGER.debug("Error processing message: {}", e.getMessage());
                // Optionally handle invalid messages or send them to another SNS topic
            } finally {
                long endTime = System.currentTimeMillis();
                LOGGER.debug("Execution time: {}ms", endTime - startTime);
            }
        }
        return null;
    }

    private PolicyPurchase parseJsonToPolicyPurchase(String json) throws Exception {
        // Deserialize JSON to a PolicyPurchase object (use Jackson or your preferred library)
        return objectMapper.readValue(json, PolicyPurchase.class);
    }

    private boolean isPrivatePassengerVehicle(PolicyPurchase purchase) {
        return "sedan".equalsIgnoreCase(purchase.getVehicle().getType())
                || "coupe".equalsIgnoreCase(purchase.getVehicle().getType())
                || "compact".equalsIgnoreCase(purchase.getVehicle().getType())
                || "suv".equalsIgnoreCase(purchase.getVehicle().getType())
                || "minivan".equalsIgnoreCase(purchase.getVehicle().getType())
                || "small truck".equalsIgnoreCase(purchase.getVehicle().getType());
    }

    private String transformToXml(PolicyPurchase policyPurchase) throws Exception {

        Document doc = docBuilder.newDocument();

        // Root element
        Element rootElement = doc.createElement("PolicyPurchase");
        doc.appendChild(rootElement);

        // Add elements
        Element quoteId = doc.createElement("QuoteId");
        quoteId.appendChild(doc.createTextNode(policyPurchase.getQuoteId()));
        rootElement.appendChild(quoteId);

        Element policyholder = doc.createElement("Policyholder");
        rootElement.appendChild(policyholder);

        Element firstName = doc.createElement("FirstName");
        firstName.appendChild(doc.createTextNode(policyPurchase.getPolicyholder().getFirstName()));
        policyholder.appendChild(firstName);

        Element lastName = doc.createElement("LastName");
        lastName.appendChild(doc.createTextNode(policyPurchase.getPolicyholder().getLastName()));
        policyholder.appendChild(lastName);

        // Repeat for other fields as needed
        // ...

        // Convert DOM to string

        DOMSource source = new DOMSource(doc);
        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);
        transformer.transform(source, result);

        return writer.toString();
    }

    private void publishToSQS(String message) {
        // Send the message to the SQS queue
        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(SQS_QUEUE_URL)
                .messageBody(message)
                .build();

        sqsClient.sendMessage(sendMessageRequest);
    }

    // Nested Policyholder Class
    public static class PolicyPurchase {

        private String quoteId;
        private Policyholder policyholder;
        private Vehicle vehicle;

        // Getters and Setters
        public String getQuoteId() {
            return quoteId;
        }

        public void setQuoteId(String quoteId) {
            this.quoteId = quoteId;
        }

        public Policyholder getPolicyholder() {
            return policyholder;
        }

        public void setPolicyholder(Policyholder policyholder) {
            this.policyholder = policyholder;
        }

        public Vehicle getVehicle() {
            return vehicle;
        }

        public void setVehicle(Vehicle vehicle) {
            this.vehicle = vehicle;
        }

    }

    // Nested Policyholder Class
    public static class Policyholder {
        private String firstName;
        private String lastName;

        // Getters and Setters
        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

    }

    // Nested Vehicle Class
    public static class Vehicle {
        private String type;
        private String vin;

        // Getters and Setters
        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getVin() {
            return vin;
        }

        public void setVin(String vin) {
            this.vin = vin;
        }

    }

}