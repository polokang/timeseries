const express = require("express");
const mongoose = require("mongoose");
const mqtt = require("mqtt");
const zlib = require("zlib");
const app = express();
const port = 3000;

// 连接到 MongoDB
mongoose
  .connect(
    "mongodb+srv://admin:84D43fhv.ht^J@clusterdev.mjmep.mongodb.net/sensor_data"
  )
  .then(() => {
    console.log("Connected to MongoDB");
  })
  .catch((err) => {
    console.error("Error connecting to MongoDB", err);
  });

// 定义动态结构的 Schema，存储不确定的 JSON 数据
const sensorSchema = new mongoose.Schema({
  sensorId: {
    type: String,
    required: true,
  },
  timestamp: {
    type: Date,
    default: Date.now,
    required: true,
  },
  data: {
    type: mongoose.Schema.Types.Mixed, // 存储不确定结构的 JSON 文档
    required: true,
  },
});

// 创建模型
const SensorData = mongoose.model("SensorData", sensorSchema);
const protocol = "mqtt";
const host = "comms.aquareporter.com.au";
const ports = "1883";
const clientId = `mqtt_${Math.random().toString(16).slice(3)}`;
const connectUrl = `${protocol}://${host}:${ports}`;
const mqttClient = mqtt.connect(connectUrl, {
  clientId,
  clean: true,
  connectTimeout: 4000,
  username: "Tatooine",
  password:
    "jnMO/eHbB1bPYpTn4OOdfeMFTYAyBdeEusCgTi7yXR+GK2dXDX8UikqjD2k/RvvgH02u/XnZ9vLj2Bh1ycOt1g==",
  reconnectPeriod: 1000,
});
const topic = "C1M/#";

mqttClient.on("connect", () => {
  console.log(`Connected to MQTT broker, subscribing to topic: ${topic}`);
  mqttClient.subscribe(topic, (err) => {
    if (err) {
      console.error("Failed to subscribe to topic", err);
    }
  });
});

mqttClient.on("message", async (receivedTopic, message) => {
  console.log(`Received message from topic: ${receivedTopic}`)
  const deviceTopicPattern = /^C1M\/(6\d{5})$/; // 匹配 C1M/600000 到 C1M/699999
  const match = receivedTopic.match(deviceTopicPattern);
  if (match) {
    const deviceNumber = match[1]; // 提取设备号
    console.log(`Received data from device: ${deviceNumber}`);
    try {
      // 尝试将 Buffer 直接转换为字符串
      const jsonString = message.toString("utf8");
      const decodedBuffer = Buffer.from(jsonString, "base64");

      zlib.gunzip(decodedBuffer, async (err, result) => {
        if (err) {
          console.error("Error decompressing message:", err);
        } else {
          // 解压后的结果是 Buffer，将其转换为字符串并解析为 JSON
          const decompressedData = result.toString();
          // 在这里可以将 jsonData 保存到 MongoDB
          const sensorData = new SensorData({
            sensorId: deviceNumber,
            timestamp: new Date(),
            data: decompressedData,
          });
          try {
            await sensorData.save();
            console.log(
              `Saved data from sensor ${decompressedData}`
            );
          } catch (err) {
            console.error("Error saving sensor data", err);
          }
        }
      });
    } catch (err) {
      console.error("Error processing MQTT message:", err);
    }
  }
});

// 生成 6 位数字编号的函数
// function generateSensorId() {
//   return 660090;
// }

// 模拟传感器生成数据的函数
// function generateSensorData() {
//   const temperature = (Math.random() * 15 + 20).toFixed(2); // 生成 20 到 35 之间的随机温度
//   const humidity = (Math.random() * 30 + 40).toFixed(2);    // 生成 40 到 70 之间的随机湿度
//   const sensorId = generateSensorId(); // 生成6位传感器编号
//   return { sensorId, temperature, humidity };
// }

// // 每分钟发送一次数据到 MongoDB
// setInterval(async () => {
//   const data = generateSensorData();
//   const sensorData = new SensorData(data);

//   try {
//     await sensorData.save();
//     console.log(`Saved data from sensor ${data.sensorId}: Temperature: ${data.temperature}°C, Humidity: ${data.humidity}%`);
//   } catch (err) {
//     console.error('Error saving sensor data', err);
//   }
// }, 60 * 1000); // 每60秒生成一次数据

// 定义路由
app.get("/", (req, res) => {
  res.send("Sensor data collection service is running!");
});

// 启动服务
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
