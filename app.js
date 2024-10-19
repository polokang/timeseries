const express = require('express');
const mongoose = require('mongoose');
const app = express();
const port = 3000;

// 连接到 MongoDB
mongoose.connect('mongodb+srv://admin:84D43fhv.ht^J@clusterdev.mjmep.mongodb.net/sensor_data')
  .then(() => {
    console.log('Connected to MongoDB');
  })
  .catch((err) => {
    console.error('Error connecting to MongoDB', err);
  });

// 定义时序数据的 Schema，包含传感器编号
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
  temperature: Number,
  humidity: Number,
});

// 创建模型
const SensorData = mongoose.model('SensorData', sensorSchema);

// 生成 6 位数字编号的函数
function generateSensorId() {
  return 660090;
}

// 模拟传感器生成数据的函数
function generateSensorData() {
  const temperature = (Math.random() * 15 + 20).toFixed(2); // 生成 20 到 35 之间的随机温度
  const humidity = (Math.random() * 30 + 40).toFixed(2);    // 生成 40 到 70 之间的随机湿度
  const sensorId = generateSensorId(); // 生成6位传感器编号
  return { sensorId, temperature, humidity };
}

// 每分钟发送一次数据到 MongoDB
setInterval(async () => {
  const data = generateSensorData();
  const sensorData = new SensorData(data);

  try {
    await sensorData.save();
    console.log(`Saved data from sensor ${data.sensorId}: Temperature: ${data.temperature}°C, Humidity: ${data.humidity}%`);
  } catch (err) {
    console.error('Error saving sensor data', err);
  }
}, 60 * 1000); // 每60秒生成一次数据

// 定义路由
app.get('/', (req, res) => {
  res.send('Sensor data collection service is running!');
});

// 启动服务
app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
