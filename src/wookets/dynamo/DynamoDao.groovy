package wookets.dynamo

import groovy.util.logging.Log4j

import java.io.InputStream
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Collection
import java.util.Date
import java.util.HashMap
import java.util.List

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model.AttributeAction
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate
import com.amazonaws.services.dynamodb.model.BatchGetItemRequest
import com.amazonaws.services.dynamodb.model.BatchGetItemResult
import com.amazonaws.services.dynamodb.model.BatchResponse
import com.amazonaws.services.dynamodb.model.DeleteItemRequest
import com.amazonaws.services.dynamodb.model.DeleteItemResult
import com.amazonaws.services.dynamodb.model.GetItemRequest
import com.amazonaws.services.dynamodb.model.GetItemResult
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.services.dynamodb.model.KeysAndAttributes
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.PutItemResult
import com.amazonaws.services.dynamodb.model.ScanRequest
import com.amazonaws.services.dynamodb.model.ScanResult
import com.amazonaws.services.dynamodb.model.UpdateItemRequest
import com.amazonaws.services.dynamodb.model.UpdateItemResult

@Log4j
class DynamoDao {
  
  // set these to something else if you want...
  String tableName = "Set me to something..."
  
  AmazonDynamoDBClient client;
  
  DynamoDao() {
    InputStream credentialsAsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("AwsCredentials.properties");
    AWSCredentials credentials = new PropertiesCredentials(credentialsAsStream);
    client = new AmazonDynamoDBClient(credentials);
  }
  DynamoDao(String tableName) {
    this()
    this.tableName = tableName
  } 
  
  /**
   * Will save an object to the dynamo db table. 
   * @param resource a generic groovy or java class that you want to save
   * @param resourceId the id of the resource you are saving that will be used to construct the hashkey. Note, we don't 
   *    make assumpts about what property you want your id to be used on.
   */
  void save(Object resource, String resourceId, String tenantId = "default") {
    String resourceType = DynamoUtil.resolveType(resource)
    log.debug "Saving ${resourceType}::${resourceId} - ${tenantId}"
    
    // convert resource to something dynamo understands
    def item = DynamoUtil.convertObjectToDynamo(resource)
    
    // set 'hashkey' property last so nothing can override it...
    item["hashkey"] = DynamoUtil.createKeyAttribute(tenantId, resourceType, resourceId)
    
    // push request to dynamo
    PutItemRequest request = new PutItemRequest(tableName, item)
    PutItemResult result = client.putItem(request)
    
    // we also need to add this type:id to the tenant relationship row
    updateTenantList("ADD", resourceType, resourceId, tenantId)
  }
  
  // read
  /**
   * Load a single object from dynamo db.
   * @param resourceTypeObject
   * @param resourceId
   * @return a fully qualified java object if you pass in a Class for resourceType or a dynamic groovy object with [_type: "Mock"]
   */
  Object load(Object resourceTypeObject, String resourceId, String tenantId = "default") {
    String resourceType = DynamoUtil.resolveType(resourceTypeObject)
    log.debug "Loading ${resourceType}::${resourceId} - ${tenantId}"
    
    GetItemRequest request = new GetItemRequest(tableName, DynamoUtil.createKey(tenantId, resourceType, resourceId))
    GetItemResult result = client.getItem(request)
    return DynamoUtil.convertObjectFromDynamo(result.item, resourceTypeObject)
  }
  
  /**
   * Load a group of objects from dynamo db.
   * @param resourceType
   * @param resourceIds
   * @return a bunch of objects. limit == 100 or 1MB
   */
  List loadBatch(Object resourceTypeObject, List resourceIds, String tenantId = "default") {
    String resourceType = DynamoUtil.resolveType(resourceTypeObject)
    log.debug "Loading multiple ${resourceType}::${resourceIds} - ${tenantId}"
    
    List keys = []
    resourceIds.each { String resourceId ->
      keys.add(DynamoUtil.createKey(tenantId, resourceType, resourceId))
    }
    Map requestItems = new HashMap<String, KeysAndAttributes>()
    requestItems.put(tableName, new KeysAndAttributes().withKeys(keys))
    BatchGetItemRequest request = new BatchGetItemRequest()
    request.withRequestItems(requestItems)
    BatchGetItemResult result = client.batchGetItem(request)
    
    BatchResponse batchResults = result.getResponses().get(tableName) // the limit on this is 100 results or 1MB
    List results = []
    batchResults.items.each { Map item ->
      results.add(DynamoUtil.convertObjectFromDynamo(item, resourceTypeObject))
    }
    return results
  }
  
  /**
   * Load all objects for a tenant.
   * @param resourceType
   * @return valid java classes or dynamic groovy objects based on resourceType.
   */
  List loadAll(Object resourceType, String tenantId = "default") {
    List resourceHashes = loadAllHash(resourceType, tenantId)
    List resourceIds = []
    resourceHashes.each { String resourceHash ->
      resourceIds.add(resourceHash.split(":")[1])
    }
    List objs = loadBatch(resourceType, resourceIds, tenantId) // query all actual objects
    return objs
  }
  
  /**
   * Load 'raw' hashes from the tenant relationship table.
   * @param resourceType
   * @return e.g. ["Mock:1", "Mock:3"]
   */
  List loadAllHash(Object resourceType, String tenantId = "default") {
    String resourceTypeKey = DynamoUtil.resolveType(resourceType)
    GetItemRequest request = new GetItemRequest(tableName, DynamoUtil.createKey(tenantId, resourceTypeKey))
    GetItemResult result = client.getItem(request)
    return DynamoUtil.fromDynamo(result.item["ul"])
  }
  
  // delete
  /**
   * Will remove an object from dynamo db.
   * @param resourceTypeObject
   * @param resourceId
   */
  void delete(Object resourceTypeObject, String resourceId, String tenantId = "default") {
    String resourceType = DynamoUtil.resolveType(resourceTypeObject)
    log.debug "Deleting ${resourceType}::${resourceId} - ${tenantId}"
    
    DeleteItemRequest request = new DeleteItemRequest(tableName, DynamoUtil.createKey(tenantId, resourceType, resourceId))
    DeleteItemResult result = client.deleteItem(request)
    
    // remove from tenant relationship as well
    updateTenantList("DELETE", resourceType, resourceId, tenantId)
  }
  
  void deleteAll(Object resourceType, String tenantId = "default") {
    log.debug "Deleting all ${resourceType} - ${tenantId}"
    
    // load from tenant relationship
    List objs = loadAllHash(resourceType, tenantId)
    
    // remove all object instances
    objs.each { String hash ->
      delete(resourceType, DynamoUtil.getObjectReferenceId(hash))
    }
  }
  
  /**
   * Will update an individual property on an object. 
   * @param action e.g. ADD or DELETE or PUT
   * @param resourceTypeObject e.g. Mock.class or "Mock" etc
   * @param resourceId
   * @param propertyName the name of the property on the resource object
   * @param propertyValue the value of the property that you want to add or put. If this is null, we assume a delete action which
   *  will remove the property from the object (since we don't store null properties)
   */
  void updateProperty(String action, Object resourceTypeObject, String resourceId, String propertyName, Object propertyValue, String tenantId = "default") {
    String resourceType = DynamoUtil.resolveType(resourceTypeObject)
    log.debug "Updating tenant list ${action} - ${resourceType} - ${resourceId} - ${tenantId}"
    
    def updateItems = new HashMap<String, AttributeValueUpdate>()
    if(propertyValue == null) 
      updateItems.put(propertyName, new AttributeValueUpdate().withAction(DynamoUtil.resolveAction("DELETE"))) // special case to remove the property
    else
      updateItems.put(propertyName, new AttributeValueUpdate(DynamoUtil.toDynamo(propertyValue), DynamoUtil.resolveAction(action)))
    Key key = DynamoUtil.createKey(tenantId, resourceType)
    UpdateItemRequest request = new UpdateItemRequest(tableName, key, updateItems)
    UpdateItemResult result = client.updateItem(request)
  }
  
  /**
   * Will update the object in the tenant list of object references.
   * @param action e.g. ADD or DELETE
   * @param resourceTypeObject e.g. Mock.class or "Mock" or [_type: "Mock"] or new Mock()
   * @param resourceId
   */
  private void updateTenantList(String action, Object resourceTypeObject, String resourceId, String tenantId = "default") {
    String resourceType = DynamoUtil.resolveType(resourceTypeObject)
    log.debug "Updating tenant list ${action} - ${resourceType} - ${resourceId} - ${tenantId}"
    
    def updateItems = new HashMap<String, AttributeValueUpdate>()
    String hash = DynamoUtil.createObjectReferenceHash(resourceType, resourceId)
    updateItems.put("ul", new AttributeValueUpdate(DynamoUtil.toDynamo([hash]), DynamoUtil.resolveAction(action)))
    Key key = DynamoUtil.createKey(tenantId, resourceType)
    UpdateItemRequest request = new UpdateItemRequest(tableName, key, updateItems)
    UpdateItemResult result = client.updateItem(request)
  }
  
}
