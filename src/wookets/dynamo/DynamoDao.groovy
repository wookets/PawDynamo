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
  DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
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
  
  // save
  void save(Object resource, String resourceId, String tenantId = "default") {
    String resourceTypeKey = DynamoUtil.resolveType(resource)
    log.debug "Saving ${resourceTypeKey}::${resourceId} - ${tenantId}"
    
    def item = [:] // item to save
    if(resource.class == null) { // dynamic object
      resource.each { String prop, Object val ->
        if(prop == "_type") return; // strip out '_type'
        if(val == null) return; // dont waste space with null values
        item[prop] = toDynamo(val)
      }
    } else { // typed object
      resource.properties.each { String prop, Object val ->
        if(prop == "class") return; // strip out java added properties
        if(prop == "metaClass") return; // strip out groovy added properties
        if(val == null) return; // we don't support storing nulls
        item[prop] = toDynamo(val)
      }
    }
    // set 'hashkey' property last so nothing can override it...
    item["hashkey"] = toDynamo("${tenantId}#${resourceTypeKey}#${resourceId}")
    
    PutItemRequest request = new PutItemRequest(tableName, item)
    PutItemResult result = client.putItem(request)
  }
  
  // read
  Object load(Object resourceType, String resourceId, String tenantId = "default") {
    log.debug "Loading ${resourceType}::${resourceId} - ${tenantId}"
    
    String resourceTypeKey = DynamoUtil.resolveType(resourceType)
    Key key = new Key(toDynamo("${tenantId}#${resourceTypeKey}#${resourceId}"))
    GetItemRequest request = new GetItemRequest(tableName, key)
    GetItemResult result = client.getItem(request)
    
    // do something if no result returned
    if(result.item == null)
      return null
    
    def obj = resourceType instanceof Class ? resourceType.newInstance() : [_type: resourceTypeKey] // return typed or dynamic
    result.item.each { String prop, AttributeValue val ->
      if(prop == "hashkey") return // strip out hashkey property...
      obj[prop] = fromDynamo(val)
    }
    return obj
  }
  
  List loadBatch(Object resourceType, List resourceIds, String tenantId = "default") {
    log.debug "Loading multiple ${resourceType}::${resourceIds} - ${tenantId}"
    
    String resourceTypeKey = DynamoUtil.resolveType(resourceType)
    
    List keys = []
    resourceIds.each { Object resourceId ->
      keys.add(new Key(new AttributeValue().withS("${tenantId}#${resourceTypeKey}#${resourceId}"))) 
    }
    Map requestItems = new HashMap<String, KeysAndAttributes>()
    requestItems.put(tableName, new KeysAndAttributes().withKeys(keys))
    BatchGetItemRequest request = new BatchGetItemRequest()
    request.withRequestItems(requestItems)
    BatchGetItemResult result = client.batchGetItem(request)
    
    BatchResponse batchResults = result.getResponses().get(tableName) // the limit on this is 100 results or 1MB
    List results = []
    batchResults.items.each { Map<String, AttributeValue> item ->
      def obj = resourceType instanceof Class ? resourceType.newInstance() : [_type: resourceTypeKey] // return typed or dynamic
      item.each { String prop, AttributeValue val ->
        obj[prop] = fromDynamo(val)
      }
      results.add(obj)
    }
    return results
  }
  
  List loadAll(Object resourceType, String tenantId = "default") {
    String tableName = DynamoUtil.resolveType(resourceType)
    ScanRequest scanRequest = new ScanRequest(tableName)
    ScanResult result = client.scan(scanRequest);
    
    List objs = []
    result.items.each { item ->
      println item
      def obj = resourceType instanceof Class ? resourceType.newInstance() : [_type: tableName] // return typed or dynamic
      item.each { String prop, AttributeValue val ->
        obj[prop] = fromDynamo(val)
      }
      objs.add(obj)
    }
    return objs
  }
  
  // delete
  void delete(Object resourceType, String resourceId, String tenantId = "default") {
    log.debug "Deleting ${resourceType}::${resourceId} - ${tenantId}"
    
    String resourceTypeKey = DynamoUtil.resolveType(resourceType)
    Key key = new Key(toDynamo("${tenantId}#${resourceTypeKey}#${resourceId}"))
    DeleteItemRequest request = new DeleteItemRequest(tableName, key)
    DeleteItemResult result = client.deleteItem(request)
  }
  
  // update
  void addToProperty(Object resourceType, String resourceId, String propertyName, Object propertyValue) {
    log.debug "Adding ${resourceType} - ${resourceId} - ${propertyName} - ${propertyValue}"
    
    String tableName = DynamoUtil.resolveType(resourceType)
    Key key = new Key(toDynamo(resourceId))
    def updateItems = new HashMap<String, AttributeValueUpdate>()
    
    AttributeAction action = propertyValue instanceof Collection ? AttributeAction.ADD : AttributeAction.PUT
    
    updateItems.put(propertyName, new AttributeValueUpdate(toDynamo(propertyValue), action))
    UpdateItemRequest request = new UpdateItemRequest(tableName, key, updateItems);          
    UpdateItemResult result = client.updateItem(request);
  }
  
  void replaceProperty(Object resourceType, String resourceId, String propertyName, Object propertyValue) {
    log.debug "Replacing ${resourceType} - ${resourceId} - ${propertyName} - ${propertyValue}"
    
    String tableName = DynamoUtil.resolveType(resourceType)
    Key key = new Key(toDynamo(resourceId))
    def updateItems = new HashMap<String, AttributeValueUpdate>()
    
    updateItems.put(propertyName, new AttributeValueUpdate(toDynamo(propertyValue), AttributeAction.PUT))
    UpdateItemRequest request = new UpdateItemRequest(tableName, key, updateItems);
    UpdateItemResult result = client.updateItem(request);
  }
  
  void removeFromProperty(Object resourceType, String resourceId, String propertyName, Object propertyValue) {
    log.debug "Removing ${resourceType} - ${resourceId} - ${propertyName} - ${propertyValue}"
    
    String tableName = DynamoUtil.resolveType(resourceType)
    Key key = new Key(toDynamo(resourceId))
    def updateItems = new HashMap<String, AttributeValueUpdate>()
    
    updateItems.put(propertyName, new AttributeValueUpdate(toDynamo(propertyValue), AttributeAction.DELETE))
    UpdateItemRequest request = new UpdateItemRequest(tableName, key, updateItems)
    UpdateItemResult result = client.updateItem(request)
  }
  
  void removeProperty(Object resourceType, String resourceId, String propertyName) {
    log.debug "Removing ${resourceType} - ${resourceId} - ${propertyName}"
    
    String tableName = DynamoUtil.resolveType(resourceType)
    String hashKey = resourceId
    
    def updateItems = new HashMap<String, AttributeValueUpdate>()
    Key key = new Key().withHashKeyElement(new AttributeValue().withS(hashKey))
    
    updateItems.put(propertyName, new AttributeValueUpdate().withAction(AttributeAction.DELETE));
    def updateItemRequest = new UpdateItemRequest().withTableName(tableName).withKey(key).withAttributeUpdates(updateItems);
    UpdateItemResult result = client.updateItem(updateItemRequest);
  }
  
  
  // private helper methods
  
  private AttributeValue toDynamo(Object value) {
    if(value instanceof Number) { // number support
      return new AttributeValue().withN(value)
    } 
    if(value instanceof String || value instanceof GString) { // string support
      return new AttributeValue().withS(value)
    }
    if(value instanceof Collection) { // list support
      if(value[0] instanceof Number) { // number list support
        return new AttributeValue().withNS(value)
      } else if(value[0] instanceof String) { // string list support
        return new AttributeValue().withSS(value)
      }
    } 
    if(value instanceof Boolean) { // boolean support
      return new AttributeValue().withS(value ? "bool:true" : "bool:false")
    } 
    if(value instanceof Date) { // date support
      return new AttributeValue().withS("date:" + dateFormatter.format(value))
    }
    throw new RuntimeException("Unsupported data type for value ${value}")
  }
  
  private Object fromDynamo(AttributeValue value) {
    if(value.getS() != null) {
      String val = value.getS()
      if(val == "bool:true") { // boolean support
        return true
      } else if(val == "bool:false") {
        return false
      } else if(val?.startsWith("date:")) { // date support
        return dateFormatter.parse(val.substring(5))
      } else { // string support
        return val
      }
    } else if(value.getN() != null) { // number support
      return value.getN()
    } else if(value.getSS() != null) { // set of strings support
      return value.getSS()
    } else if(value.getNS() != null) { // set of numbers support
      return value.getNS()
    }
  }
}
