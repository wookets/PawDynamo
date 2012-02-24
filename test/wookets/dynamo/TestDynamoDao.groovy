package wookets.dynamo;

import static org.junit.Assert.*;

import org.junit.BeforeClass
import org.junit.Test

class TestDynamoDao {
  
  static String TableName = "test"
  static String TenantId = "paw"
  DynamoDao dynamoDao = new DynamoDao(TableName)
  
  @Test
  void testSave() {
    def mock = [id: "1", name: "Nate", _type: "Mock"]
    dynamoDao.save(mock, mock.id)
  }
  
  @Test(expected=NullPointerException.class) 
  void testSaveNull() {
    dynamoDao.save null, null
  }
  @Test(expected=RuntimeException.class)
  void testSaveNoType() {
    dynamoDao.save([id:"3", name: "Mate"], "3")
  }
  
  @Test
  void testLoad() {
    // save an object in the db that we can use...
    def mock = [id: "1", name: "Jimmy", _type: "Mock"]
    dynamoDao.save(mock, mock.id)
    
    mock = dynamoDao.load("Mock", "1")
    assert mock.name == "Jimmy"
    assert mock.id == "1"
    assert mock._type == "Mock"
    assert mock["hashkey"] == null
  }
  
  @Test
  void testLoadObjectDoesntExist() {
    def mock = dynamoDao.load "Mock", "notexist"
    assert mock == null
  }
  
  @Test
  void testDelete() {
    dynamoDao.save([id: "1", name: "Ham", _type: "Mock"], "1")
    def mock = dynamoDao.load("Mock", "1")
    assert mock.id == "1"
    dynamoDao.delete("Mock", "1")
    mock = dynamoDao.load("Mock", "1")
    assert mock == null
  }
  
}
