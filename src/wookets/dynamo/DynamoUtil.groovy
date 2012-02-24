package wookets.dynamo

import java.text.SimpleDateFormat

class DynamoUtil {
  
  static String resolveType(Object resource) {
    if(resource == null) throw new NullPointerException("Resource can not be null")
    
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
}
