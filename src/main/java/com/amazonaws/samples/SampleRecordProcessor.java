package com.amazonaws.samples;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;


// Newly Added by Fab
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

//import com.google.gson.Gson;
//import com.google.gson.GsonBuilder;
//import com.google.gson.JsonArray;
//import com.google.gson.JsonDeserializationContext;
//import com.google.gson.JsonDeserializer;
//import com.google.gson.JsonElement;
//import com.google.gson.JsonObject;
//import com.google.gson.JsonParseException;
import java.util.HashMap;
import java.util.Map;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
//

/**
*
*/
public class SampleRecordProcessor implements IRecordProcessor {
    
    private static final Log LOG = LogFactory.getLog(SampleRecordProcessor.class);
    private String kinesisShardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;
    
    private final CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();

    //Added by Fabian
    private static AmazonDynamoDBClient client;

    private static final String DDB_TABLE = "Twitter_Stream";
    
    /**
* Constructor.
*/
    public SampleRecordProcessor() {
        super();
    }
    
    /**
* {@inheritDoc}
*/
    @Override
    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard: " + shardId);
        this.kinesisShardId = shardId;
    }

    /**
* {@inheritDoc}
*/
    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Processing " + records.size() + " records from " + kinesisShardId);
        
        // Process records and perform all exception handling.
        processRecordsWithRetries(records);
        
        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }
        
    }

    private void createDDBClient()  {

	client = new AmazonDynamoDBClient(new ClasspathPropertiesFileCredentialsProvider());
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        client.setRegion(usEast1);

    }


   private class TwitterMainResponse {

	    private String created_at = "none";
	    private String id = "none";
	    private String text = "none";
	    private TwitterUserResponse user; 


	    //@Override
	    private String getId() {
        	return id;
   	    }

            private String getTimeStamp() {
                return created_at;
            }

            private String getText() {
                return text;
            }

            private String getLocation() {
                return user.getLocation();
            }

	    private String getName() {
                return user.getName();
            }



   }


    private class TwitterUserResponse {
		private String name = "none";
		private String location = "none";

	    public String getName() {
	        return name;
     	    }

            public String getLocation() {
                return location;
            }

   }

    private void processRecordsWithRetries(List<Record> records){
        createDDBClient();
        for (Record record : records) {
            boolean processedSuccessfully = false;
            String data = null;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
	            String tableName = DDB_TABLE;
 		    String location;
 		    String name;
                    // For this app, we interpret the payload as UTF-8 chars.
                    data = decoder.decode(record.getData()).toString();
            	    LOG.info(data + "\n\n\n");

		    // commented by Fabian
		/*
		    Gson gson = new GsonBuilder().create();
	            TwitterMainResponse t = gson.fromJson(data, TwitterMainResponse.class);
            	    LOG.info(record.getSequenceNumber() + ", " + record.getPartitionKey() + ", " + t.getName() + ", " + t.getId() + ", " + t.getLocation() + ", " + t.getText() + ", " + t.getTimeStamp());

		    if (t.getLocation().equals("")){ 
		        location = "NA";
    		    } else {
			location = t.getLocation();
		    }

                    if (t.getName().equals("")){
                        name = "NA";
                    } else {
                        name = t.getName();
                    }


	            Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
            	    item.put("id", new AttributeValue(t.getId()));
            	    item.put("name", new AttributeValue(name));
            	    item.put("location", new AttributeValue(location));
            	    item.put("text", new AttributeValue(t.getText()));
            	    item.put("created_at", new AttributeValue(t.getTimeStamp()));
        	    PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
	            PutItemResult putItemResult = client.putItem(putItemRequest);
*/

                    // Logic to process record goes here.
                    //
                    processedSuccessfully = true;
                    break;
                } catch (CharacterCodingException e) {
                    LOG.error("Malformed data: " + data, e);
                    break;
                } catch (Throwable t) {
                    LOG.warn("Caught throwable while processing record " + record, t);
                }
                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
* {@inheritDoc}
*/
    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }
    
 

    /** Checkpoint with retries.
* @param checkpointer
*/
    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }
    }

}
