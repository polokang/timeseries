# 使用 Node.js 18 镜像
FROM node:18
# 设置工作目录
WORKDIR /app

# 安装依赖
COPY package*.json ./

# 构建应用程序
RUN npm install

# Copy the rest of your application code to the working directory
COPY . .

# Install nodemon globally
RUN npm install -g nodemon

# 设置环境变量
ENV NODE_ENV=production

# 暴露应用程序端口
EXPOSE 3000

# 设置入口点
CMD ["nodemon", "server.js"]
 