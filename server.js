require('dotenv').config();
const mqtt = require('mqtt');
const zlib = require('zlib');
const { MongoClient } = require('mongodb');

// MQTT 配置
const protocol = process.env.MQTT_PROTOCOL || 'mqtt';
const host = process.env.MQTT_HOST || '127.0.0.1';
const ports = process.env.MQTT_PORT || 1883;
const clientId = `mqtt_${Math.random().toString(16).slice(3)}`;
const username = process.env.MQTT_USERNAME || '';
const password = process.env.MQTT_PASSWORD || '';
const connectUrl = `${protocol}://${host}:${ports}`;
const topic = "C1M/#";

// MongoDB 配置
const mongoUrl = process.env.MONGO_URL || 'mongodb://localhost:27017';
const dbName = process.env.DB_NAME || 'sensorDataDB';
const collectionName = process.env.COLLECTION_NAME || 'sensorDataCollection';

let collection; // 保存 MongoDB 集合引用

// 连接到 MongoDB 并初始化集合
async function connectToMongoDB() {
  const client = new MongoClient(mongoUrl, { useUnifiedTopology: true });
  
  try {
    await client.connect();
    console.log('Connected to MongoDB');
    const db = client.db(dbName);

    // 创建时序集合或使用已有的
    collection = db.collection(collectionName);
    await db.createCollection(collectionName, {
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

    return client;
  } catch (error) {
    console.error('MongoDB connection error:', error);
    process.exit(1);  // 如果 MongoDB 连接失败，退出程序
  }
}

// 处理接收到的 MQTT 消息
function handleMqttMessage(receivedTopic, message) {
  try {
    const decodedBuffer = Buffer.from(message.toString("utf8"), "base64");

    // 解压缩接收到的 Gzip 数据
    zlib.gunzip(decodedBuffer, async (err, decompressedBuffer) => {
      if (err) {
        console.error('Error decompressing message:', err);
        return;
      }

      // 将解压缩后的数据转换为 JSON 对象
      const jsonData = JSON.parse(decompressedBuffer.toString());
      
      // 确保时间戳的正确性
      jsonData.unixTimeStamp = jsonData.unixTimeStamp 
        ? new Date(jsonData.unixTimeStamp * 1000)
        : new Date();

      // 插入数据到 MongoDB
      try {
        const result = await collection.insertOne(jsonData);
        console.log(`Inserted data with _id: ${result.insertedId}  ${jsonData.UnitId}  :${jsonData.unixTimeStamp}`);
      } catch (error) {
        console.error('Error inserting data into MongoDB:', error);
      }
    });
  } catch (error) {
    console.error('Error processing MQTT message:', error);
  }
}

// 程序启动时，连接到 MongoDB 并开始订阅 MQTT 消息
async function start() {
  await connectToMongoDB(); // 等待 MongoDB 连接完成

  // MQTT 客户端配置与消息处理
  const mqttClient = mqtt.connect(connectUrl, {
    clientId,
    clean: true,
    connectTimeout: 4000,
    username,
    password,
    reconnectPeriod: 1000,
  });

  mqttClient.on('connect', () => {
    // 连接成功后订阅主题
    mqttClient.subscribe(topic, (err) => {
      if (err) {
        console.error('Failed to subscribe to topic', err);
      } else {
        console.log('Subscribed to topic:', topic);
      }
    });
  });

  mqttClient.on('message', handleMqttMessage); // 处理接收到的消息
}

start().catch(console.error);
