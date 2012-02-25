package wookets.dynamo;

import static org.junit.Assert.*;

import org.junit.BeforeClass
import org.junit.Test

class TestDynamoDao {
  
  static String TableName = "test"
  static String TenantId = "paw"
  DynamoDao dynamoDao = new DynamoDao(TableName)
  
  @Test
  void wipeDb() {
    dynamoDao.deleteAll("Mock")
  }
  
  @Test
  void testSave() {
    def mock = [id: "1", name: "Nate", _type: "Mock"]
    dynamoDao.save(mock, mock.id)
  }
  
  @Test(expected=Exception.class) 
  void testSaveNull() {
    dynamoDao.save null, null
  }
  @Test(expected=Exception.class)
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
  void testLoadBatch() {
    def mock1 = [id: "1", name: "Ram", _type: "Mock"]
    def mock2 = [id: "2", name: "Cam", _type: "Mock"]
    def mock3 = [id: "3", name: "Ham", _type: "Mock"]
    dynamoDao.save(mock1, mock1.id)
    dynamoDao.save(mock2, mock2.id)
    dynamoDao.save(mock3, mock3.id)
    List mocks = dynamoDao.loadBatch("Mock", ["2","3"])
    mocks.each { mock ->
      assert mock.name == "Cam" || mock.name == "Ham"
    }
    assert mocks.size() == 2
  }
  
  @Test
  void testLoadAll() {
    List mocks = dynamoDao.loadAll("Mock")
    mocks.each { mock ->
      println mock.name
    }
    assert mocks.size() == 3
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
