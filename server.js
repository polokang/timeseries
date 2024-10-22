require('dotenv').config();
const mqtt = require('mqtt');
const zlib = require('zlib');
const { MongoClient } = require('mongodb');

// MQTT 配置
const protocol = process.env.MQTT_PROTOCOL || 'mqtt';
const host = process.env.MQTT_HOST || '127.0.0.1';
const ports =  process.env.MQTT_PORT || 1883;
const clientId = `mqtt_${Math.random().toString(16).slice(3)}`;
const username = process.env.MQTT_USERNAME || '';
const password = process.env.MQTT_PASSWORD || '';
const connectUrl = `${protocol}://${host}:${ports}`;
const topic = "C1M/#";

// MongoDB 配置
const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017';
const dbName = process.env.DB_NAME || 'sensorDataDB';
const collectionName = process.env.COLLECTION_NAME || 'sensorDataCollection';

// 连接到 MongoDB
async function connectToMongoDB() {
  const client = new MongoClient(mongoUrl);
  await client.connect();
  console.log('Connected to MongoDB');
  const db = client.db(dbName);
  
  // 确保集合为时序集合
  const collection = await db.createCollection(collectionName, {
    timeseries: {
      timeField: 'unixTimeStamp',  // 使用时间字段
      metaField: 'UnitId',         // 使用设备编号作为 meta 数据
      granularity: 'minutes'       // 数据精度为分钟级
    }
  }).catch(err => {
    if (err.codeName === 'NamespaceExists') {
      console.log('Collection already exists');
    } else {
      throw err;
    }
  });

  return collection;
}

const mqttClient = mqtt.connect(connectUrl, {
    clientId,
    clean: true,
    connectTimeout: 4000,
    username: username,
    password: password,
    reconnectPeriod: 1000,
  });

// 处理接收到的 MQTT 消息
mqttClient.on('connect', () => {
  // 订阅所有设备的主题
  mqttClient.subscribe(topic, (err) => {
    if (err) {
      console.error('Failed to subscribe to topic', err);
    } else {
      console.log('Subscribed to topic');
    }
  });
});

mqttClient.on('message', async (receivedTopic, message) => {
  try {
    const jsonString = message.toString("utf8");
    const decodedBuffer = Buffer.from(jsonString, "base64");

    // 解压缩接收到的 Gzip 数据
    zlib.gunzip(decodedBuffer, async (err, decompressedBuffer) => {
      if (err) {
        console.error('Error decompressing message:', err);
        return;
      }

      // 将解压缩后的数据转换为 JSON 对象
      const jsonData = JSON.parse(decompressedBuffer.toString());
      if(jsonData.unixTimeStamp){
        jsonData.unixTimeStamp = new Date(jsonData.unixTimeStamp * 1000);
      }else{
        jsonData.unixTimeStamp = new Date();
      }
      // 连接到 MongoDB 的时序集合
      const collection = await connectToMongoDB();

      // 直接将整个 JSON 文档存储到 MongoDB 中
      collection.insertOne(jsonData)
        .then(result => console.log(`Inserted data with _id: ${result.insertedId}`))
        .catch(error => console.error('Error inserting data into MongoDB:', error));
    });
  } catch (error) {
    console.error('Error processing MQTT message:', error);
  }
});

// 连接到 MongoDB
connectToMongoDB().catch(console.error);
