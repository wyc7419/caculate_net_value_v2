# Apifox 导入指南

本文档说明如何将净值分析系统的 API 导入到 Apifox。

## 方式一：导入 OpenAPI 文档（推荐）⭐

### 步骤 1：准备 OpenAPI 文档

已生成的 OpenAPI 文档位于：`caculate_net_value/net_value_api/openapi.yaml`

### 步骤 2：在 Apifox 中导入

#### 方法 A：直接导入文件
1. 打开 Apifox 客户端
2. 选择你的项目（或创建新项目）
3. 点击左侧菜单「项目设置」→「数据导入」
4. 选择「导入数据」→「OpenAPI/Swagger」
5. 选择「导入文件」
6. 上传 `openapi.yaml` 文件
7. 选择导入模式：
   - **智能合并**：推荐，自动合并更新
   - **完全覆盖**：删除现有数据后导入
   - **仅新增**：只导入新接口
8. 点击「确定」完成导入

#### 方法 B：通过 URL 导入（需要部署）
1. 将 `openapi.yaml` 放到可访问的 HTTP 服务器
2. 在 Apifox 中选择「导入 URL」
3. 输入文档地址，如：`http://your-server.com/openapi.yaml`
4. 点击「确定」

### 步骤 3：配置环境变量

导入后，在 Apifox 中配置环境：

**开发环境**
```
BASE_URL: http://localhost:5000
```

**生产环境**
```
BASE_URL: https://api.your-domain.com
```

---

## 方式二：使用 Apifox CLI（自动同步）

### 安装 Apifox CLI

```bash
npm install -g apifox-cli
```

### 登录 Apifox

```bash
apifox login
```

### 上传 OpenAPI 文档

```bash
apifox import --project-id YOUR_PROJECT_ID --file openapi.yaml
```

**获取 Project ID：**
1. 打开 Apifox 项目
2. 进入「项目设置」→「项目信息」
3. 复制「项目 ID」

---

## 方式三：手动创建（不推荐）

如果无法使用上述方式，可以手动在 Apifox 中创建接口：

### 接口 1：获取时间区间

- **接口名称**：获取所有可用时间区间
- **请求方式**：GET
- **请求路径**：`/netvalue/intervals`
- **返回示例**：
```json
{
  "success": true,
  "intervals": ["1h", "2h", "4h", "8h", "12h", "1d"]
}
```

### 接口 2：获取净值数据

- **接口名称**：获取指定地址的净值数据
- **请求方式**：GET
- **请求路径**：`/netvalue/data/{interval}/{address}`
- **路径参数**：
  - `interval`：时间区间（必填）
  - `address`：账户地址（必填）
- **Query 参数**：
  - `fields`：字段筛选（可选，默认 all）
  - `from_first_trade`：从首笔交易开始（可选，默认 true）
  - `normalize`：归一化（可选，默认 true）
  - `page`：页码（可选）
  - `page_size`：每页大小（可选，默认 1000）

---

## 测试接口

导入完成后，在 Apifox 中测试：

### 1. 测试获取时间区间

**请求**：
```
GET http://localhost:5000/netvalue/intervals
```

**预期响应**：
```json
{
  "success": true,
  "intervals": ["1h", "2h", "4h", "8h", "12h", "1d"]
}
```

### 2. 测试获取净值数据

**请求**：
```
GET http://localhost:5000/netvalue/data/1h/0x1234567890abcdef1234567890abcdef12345678
```

**预期响应**：
```json
{
  "success": true,
  "interval": "1h",
  "address": "0x1234567890abcdef1234567890abcdef12345678",
  "record_count": 100,
  "data": {
    "timestamps": [...],
    "net_values": [...],
    ...
  }
}
```

---

## 高级功能

### 1. 自动同步

在 Apifox 中设置「自动同步」：
1. 项目设置 → 数据导入 → 自动同步
2. 选择「OpenAPI」
3. 输入文档 URL
4. 设置同步频率（如每天、每小时）

### 2. Mock 服务

Apifox 可以根据导入的接口自动生成 Mock 数据：
1. 选择接口 → 点击「Mock」
2. 启用 Mock 服务
3. 获取 Mock URL

### 3. 自动化测试

创建测试用例：
1. 选择接口 → 点击「测试」
2. 添加断言（如检查返回的 success 字段）
3. 保存测试用例
4. 运行测试

---

## 常见问题

### Q1: 导入后接口路径不对？
**A**: 检查 `openapi.yaml` 中的 `servers` 配置，确保 URL 正确。

### Q2: 如何更新已导入的接口？
**A**: 重新导入，选择「智能合并」模式，Apifox 会自动合并更新。

### Q3: 可以导出 Postman 格式吗？
**A**: 可以。在 Apifox 中选择「项目设置」→「数据导出」→「Postman」。

### Q4: 如何团队协作？
**A**: 
1. 在 Apifox 中创建团队
2. 邀请成员加入项目
3. 所有成员可以同步查看和编辑接口文档

---

## 相关链接

- [Apifox 官网](https://www.apifox.cn/)
- [Apifox 文档](https://apifox.com/help/)
- [OpenAPI 规范](https://swagger.io/specification/)
- [Apifox CLI 文档](https://apifox.com/help/app/api-manage/api-import/apifox-cli/)

---

## 推荐工作流

```
开发 → 更新 API 代码 
     ↓
     生成/更新 openapi.yaml
     ↓
     导入到 Apifox（智能合并）
     ↓
     团队查看文档 & 测试接口
     ↓
     前端根据文档开发
```

使用 Apifox 可以实现：
- ✅ API 文档自动生成
- ✅ 接口在线测试
- ✅ Mock 数据
- ✅ 自动化测试
- ✅ 团队协作

