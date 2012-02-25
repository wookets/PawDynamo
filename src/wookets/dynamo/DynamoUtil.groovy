package wookets.dynamo

import com.amazonaws.services.dynamodb.model.AttributeAction
import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.Key

import java.text.DateFormat;
import java.text.SimpleDateFormat

class DynamoUtil {
  
  static DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  
  static String resolveType(Object resource) {
    if(resource == null) throw new RuntimeException("Resource can not be null.")
    
    if(resource instanceof Class) { // resolve Resource.class (static java class references)
      return resource.simpleName
    }
    if(resource instanceof String) { // resolve "Resource" (string references)
      return resource
    } 
    if(resource.class != null) { // resolve resource (static java object references)
      return resource.class.simpleName
    }
    if(resource._type != null) { // resolve [:] (dynamic groovy objects)
      return resource._type
    }
    throw new RuntimeException("The resource needs to be typed with at least '_type' before it can be saved.")
  }
  
  static AttributeAction resolveAction(String action) {
    switch(action) {
      case "ADD": // will add to collection (does not work with non-collections)
        return AttributeAction.ADD
      case "DELETE": // will delete
        return AttributeAction.DELETE
      case "PUT": // will replace existing value (remove the whole collection or just replace individual property)
        return AttributeAction.PUT
      default:
        throw new RuntimeException("The value [${action}] is not a valid update action.")
    }
  }
  
  static String createObjectReferenceHash(String resourceType, String resourceId) {
    resourceType + ":" + resourceId
  }
  static AttributeValue createObjectReferenceAttribute(String resourceType, String resourceId) {
    return toDynamo(createObjectReferenceHash(resourceType, resourceId))
  }
  static String getObjectReferenceId(String refhash) {
    refhash.split(":")[1]
  } 
  static String getObjectReferenceType(String refhash) {
    refhash.split(":")[0]
  }
  
  static Object convertObjectToDynamo(Object resource) {
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
    return item
  }
  
  static Object convertObjectFromDynamo(Map<String, AttributeValue> item, Object resourceType) {
    if(item == null)// do something if no result returned
      return null
    
    def obj = resourceType instanceof Class ? resourceType.newInstance() : [_type: resolveType(resourceType)] // return typed or dynamic
    item.each { String prop, AttributeValue val -> // iterate through properties
      if(prop == "hashkey") return // strip out hashkey property, because users dont need to know our secret to awesomesauce
      obj[prop] = fromDynamo(val)
    }
    return obj
  }
  
  static String createKeyHash(String tenantId, Object resourceTypeObject, String resourceId = null) {
    String resourceType = resolveType(resourceTypeObject)
    if(resourceId != null)
      return tenantId + "#" + resourceType + "#" +  resourceId
    else
      return tenantId + "#" + resourceType
  }
  static AttributeValue createKeyAttribute(String tenantId, Object resourceType, String resourceId = null) {
    return new AttributeValue().withS(createKeyHash(tenantId, resourceType, resourceId))
  }
  static Key createKey(String tenantId, Object resourceType, String resourceId = null) {
    return new Key(createKeyAttribute(tenantId, resourceType, resourceId))
  }
  
  static AttributeValue toDynamo(Object value) {
    if(value instanceof Number) { // number support
      return new AttributeValue().withN(value)
    }
    if(value instanceof String) { // string support
      return new AttributeValue().withS(value)
    }
    if(value instanceof Collection) { // list support
      if(value[0] instanceof Number) { // number list support
        return new AttributeValue().withNS(value)
      }
      if(value[0] instanceof String) { // string list support
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
  
  static Object fromDynamo(AttributeValue value) {
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
