# A股多空情绪分析工具

基于交易员复盘规则，整合多源数据，自动生成市场情绪分析报告。

## 功能特点

- **多源数据整合**：融资融券、股指期货、行业资金流、龙虎榜、涨停板、外围市场
- **多空评分系统**：8大指标加权计算，2倍比例判定市场方向
- **明日开盘预判**：高开/低开预测 + 黄线/白线操作策略
- **涨停板深度分析**：梯队结构、毕业照检测、连板分布
- **Markdown报告**：格式美观，表格对齐，支持emoji图标

## 报告内容

| 模块 | 说明 |
|:-----|:-----|
| 📊 明日开盘预判 | 高开/低开方向、置信度、操作策略 |
| 📈 多空分数汇总 | 多空双方得分及市场方向结论 |
| 💰 融资融券 | 余额变化、3日趋势、信号判断 |
| 📉 股指期货 | IF/IM涨跌幅、多空信号 |
| 💹 行业资金流 | 近3日净流入/流出TOP5 |
| 🏢 龙虎榜 | 机构净买入、热门行业 |
| 🚀 涨停板结构 | 梯队分析、毕业照检测、连板分布 |
| 🎯 板块机会 | 三重共振方向、回避方向 |
| 🌏 外围市场 | 上证指数涨跌 |

## 核心算法

### 多空评分规则

| 指标 | 权重 | 多方条件 | 空方条件 |
|:-----|:----:|:---------|:---------|
| 融资融券 | 1.0 | 余额增加 | 余额减少 |
| 沪深300期货 | 1.0 | 上涨 | 下跌 |
| 中证1000期货 | 1.0 | 上涨 | 下跌 |
| 龙虎榜机构 | 1.0 | 净买入 | 净卖出 |
| 涨停环境 | 1.0 | 环境强 | 环境弱 |
| 外围市场 | 0.1 | 上涨 | 下跌 |

### 方向判定

- **多方占优**：多方分数 >= 2 × 空方分数
- **空方占优**：空方分数 >= 2 × 多方分数
- **震荡/分歧**：其他情况

### 开盘预判

- **高开**：多方>=2倍空方 且 >=3分（高置信度）
- **低开**：空方>=2倍多方 且 >=3分（高置信度）

### 黄线/白线策略

- **做黄线（题材）**：涨停环境强 + 高标>=4板
- **守白线（权重）**：涨停环境弱 或 IF强IM弱
- **震荡市**：信号分歧

### 毕业照检测

检测板块退潮信号：
- 连板断层（最高板<=2板）
- 涨停家数过少（<30家）
- 炸板率过高（>50%）
- 行业集中度过低（前5<35%）

## 项目结构

```
.
├── main.py                # 主脚本
├── requirements.txt       # 依赖包
├── README.md              # 说明文档
├── logs/                  # 报告输出目录
├── src/                   # 数据处理模块
│   ├── margin_demo.py     # 融资融券
│   ├── futures_demo.py    # 股指期货
│   ├── limitup_demo.py    # 涨停板
│   ├── industry_demo.py   # 行业资金流
│   └── institution_demo.py # 龙虎榜
├── deploy/                # 部署相关文件
│   ├── deploy.sh          # 部署脚本
│   ├── webhook_server.py  # Webhook服务
│   ├── ngastockanalyze.service  # Systemd服务
│   └── webhook.service    # Webhook Systemd服务
└── .github/workflows/     # GitHub Actions
    └── ci-cd.yml          # CI/CD工作流
```

## 快速开始

```bash
# 克隆仓库
git clone https://github.com/Alt3rmis/NGAStockAnalyze.git
cd NGAStockAnalyze

# 安装依赖
pip install -r requirements.txt

# 运行
python main.py
```

运行后会在 `logs/` 目录生成 `YYYYMMDD_market_sentiment.md` 报告文件。

## 配置说明

在 `main.py` 的 `Config` 类中可调整：

```python
class Config:
    OUTPUT_DIR = Path(__file__).parent / "logs"  # 输出目录
    DATE_LOOKBACK_DAYS = 7                        # 交易日查找天数
    MARGIN_CHANGE_DAYS = 3                        # 融资融券变化天数
    INDUSTRY_TOP_N = 5                            # 行业排名数量
    
    class Scoring:
        EXTERNAL_MARKET_WEIGHT = 0.1              # 外围市场权重
        STRONG_RATIO = 2.0                        # 强势判定比例
        MIN_CONFIDENCE_SCORE = 3                  # 高置信度最低分数
```

---

# 开发与部署指南

## 1. 本地开发环境配置

### 1.1 克隆仓库

```bash
git clone https://github.com/Alt3rmis/NGAStockAnalyze.git
cd NGAStockAnalyze
```

### 1.2 创建虚拟环境

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 1.3 安装依赖

```bash
pip install -r requirements.txt
```

### 1.4 分支策略

| 分支 | 用途 |
|:-----|:-----|
| `main` | 生产环境代码，只接受PR合并 |
| `develop` | 开发分支，日常开发在此进行 |
| `feature/*` | 新功能分支，从develop创建 |
| `hotfix/*` | 紧急修复分支，从main创建 |

### 1.5 开发流程

```bash
# 1. 从develop创建功能分支
git checkout develop
git pull origin develop
git checkout -b feature/new-feature

# 2. 开发并提交
git add .
git commit -m "feat: 添加新功能描述"

# 3. 推送到远程
git push origin feature/new-feature

# 4. 创建Pull Request到develop分支
# 5. 代码审查通过后合并
```

## 2. 提交信息规范

使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>(<scope>): <subject>

<body>

<footer>
```

### 类型(type)

| 类型 | 说明 |
|:-----|:-----|
| `feat` | 新功能 |
| `fix` | 修复Bug |
| `docs` | 文档更新 |
| `style` | 代码格式（不影响逻辑） |
| `refactor` | 重构代码 |
| `test` | 测试相关 |
| `chore` | 构建/工具变更 |

### 示例

```bash
git commit -m "feat(limitup): 添加毕业照检测算法"
git commit -m "fix(margin): 修复融资余额计算错误"
git commit -m "docs: 更新部署文档"
```

## 3. 服务器部署

### 3.1 服务器要求

- Ubuntu 20.04+ 或 CentOS 7+
- Python 3.9+
- Git

### 3.2 首次部署

```bash
# 登录服务器
ssh user@your-server

# 下载部署脚本（首次）
mkdir -p /opt/NGAStockAnalyze/deploy
cd /opt/NGAStockAnalyze

# 克隆仓库
git clone https://github.com/Alt3rmis/NGAStockAnalyze.git .

# 运行初始化
chmod +x deploy/deploy.sh
./deploy/deploy.sh init
```

### 3.3 配置Systemd服务

```bash
# 复制服务文件
cp deploy/ngastockanalyze.service /etc/systemd/system/

# 重载systemd
systemctl daemon-reload

# 启动服务
systemctl start ngastockanalyze
systemctl enable ngastockanalyze

# 查看状态
systemctl status ngastockanalyze
```

### 3.4 部署脚本命令

```bash
./deploy/deploy.sh init      # 首次初始化
./deploy/deploy.sh install   # 安装/更新依赖
./deploy/deploy.sh update    # 拉取最新代码
./deploy/deploy.sh start     # 启动服务
./deploy/deploy.sh stop      # 停止服务
./deploy/deploy.sh restart   # 重启服务
./deploy/deploy.sh status    # 查看状态
./deploy/deploy.sh rollback  # 回滚到上一版本
```

## 4. 自动化部署

### 4.1 方案一：GitHub Actions（推荐）

已在 `.github/workflows/ci-cd.yml` 中配置：

- **代码检查**：推送时自动运行 lint
- **测试**：运行单元测试
- **自动部署**：
  - `develop` 分支 → 部署到测试环境
  - `main` 分支 → 部署到生产环境

#### 配置GitHub Secrets

在仓库 Settings → Secrets and variables → Actions 中添加：

| Secret | 说明 |
|:-------|:-----|
| `STAGING_HOST` | 测试服务器IP |
| `STAGING_USER` | 测试服务器用户名 |
| `STAGING_SSH_KEY` | SSH私钥 |
| `PRODUCTION_HOST` | 生产服务器IP |
| `PRODUCTION_USER` | 生产服务器用户名 |
| `PRODUCTION_SSH_KEY` | SSH私钥 |

### 4.2 方案二：Webhook自动部署

#### 服务器端配置

```bash
# 安装webhook服务
cp deploy/webhook.service /etc/systemd/system/

# 编辑配置，设置Webhook Secret
vim /etc/systemd/system/webhook.service
# 修改 Environment="WEBHOOK_SECRET=your_secret_here"

# 启动服务
systemctl daemon-reload
systemctl start webhook
systemctl enable webhook
```

#### GitHub配置

1. 进入仓库 Settings → Webhooks
2. 点击 "Add webhook"
3. 配置：
   - **Payload URL**: `http://your-server:9000/webhook`
   - **Content type**: `application/json`
   - **Secret**: 与服务器配置一致
   - **Events**: 选择 "Just the push event"
4. 点击 "Add webhook"

## 5. 错误处理与回滚

### 5.1 查看日志

```bash
# 服务日志
tail -f /opt/NGAStockAnalyze/logs/service.log

# 错误日志
tail -f /opt/NGAStockAnalyze/logs/error.log

# 部署历史
cat /opt/NGAStockAnalyze/logs/deploy_history.log
```

### 5.2 回滚操作

```bash
# 方法1：使用部署脚本
./deploy/deploy.sh rollback

# 方法2：手动回滚到指定版本
cd /opt/NGAStockAnalyze
git log --oneline -10          # 查看最近提交
git checkout <commit-hash>     # 切换到指定版本
./deploy/deploy.sh restart     # 重启服务
```

### 5.3 常见问题

| 问题 | 解决方案 |
|:-----|:---------|
| 依赖安装失败 | 检查Python版本，清理pip缓存 |
| 服务无法启动 | 检查日志，确认端口未被占用 |
| Webhook无响应 | 检查防火墙，确认服务运行中 |
| 数据获取失败 | 检查网络连接，API可能限流 |

### 5.4 备份恢复

```bash
# 备份位置
ls /opt/backups/NGAStockAnalyze/

# 手动备份
tar -czf backup_$(date +%Y%m%d).tar.gz /opt/NGAStockAnalyze

# 从备份恢复
tar -xzf backup_20260225.tar.gz -C /
```

## 6. 安全建议

1. **不要提交敏感信息**：使用环境变量或配置文件（已在.gitignore中）
2. **定期更新依赖**：`pip install --upgrade -r requirements.txt`
3. **使用SSH密钥**：而非密码登录服务器
4. **配置防火墙**：只开放必要端口
5. **定期备份**：配置自动备份任务

---

## 注意事项

- 数据来源于 akshare，需要网络连接
- 龙虎榜热门行业分析暂时跳过（可能卡住）
- 建议在交易日收盘后运行，数据更完整
