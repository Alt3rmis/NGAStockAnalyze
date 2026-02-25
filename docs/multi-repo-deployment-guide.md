# GitHub Actions 多仓库部署隔离配置指南

本指南确保在同一GitHub账户下配置新仓库的部署流程时，不影响现有仓库的工作流。

## 架构概览

```
GitHub Account
├── Repository A (现有)          ← 继续正常运行
│   └── Secrets: DEPLOY_HOST_A, DEPLOY_KEY_A
│   └── 部署到: /opt/project-a/
│
└── Repository B (NGAStockAnalyze) ← 新配置
    └── Secrets: DEPLOY_HOST_B, DEPLOY_KEY_B
    └── 部署到: /opt/NGAStockAnalyze/
```

---

## 第一部分：服务器端配置

### 1.1 创建专用部署用户（推荐）

为避免权限冲突，建议为新项目创建专用用户：

```bash
# SSH登录到服务器
ssh root@your-server-ip

# 创建专用部署用户
sudo useradd -m -s /bin/bash deploy-nga

# 设置密码（可选，建议使用SSH密钥）
sudo passwd deploy-nga

# 添加到sudo组（如需要）
sudo usermod -aG sudo deploy-nga
```

### 1.2 创建项目目录

```bash
# 切换到新用户
su - deploy-nga

# 创建项目目录
mkdir -p /home/deploy-nga/NGAStockAnalyze
mkdir -p /home/deploy-nga/NGAStockAnalyze/logs
mkdir -p /home/deploy-nga/backups

# 设置权限
chmod 755 /home/deploy-nga
chmod 755 /home/deploy-nga/NGAStockAnalyze
```

或者使用现有用户（如root），确保目录隔离：

```bash
# 使用现有用户
mkdir -p /opt/NGAStockAnalyze
mkdir -p /opt/NGAStockAnalyze/logs
mkdir -p /opt/backups/NGAStockAnalyze

# 设置权限
chmod 755 /opt/NGAStockAnalyze
```

### 1.3 生成专用SSH密钥

**重要：为新仓库生成独立的SSH密钥，不要复用现有密钥**

```bash
# 创建SSH目录（如果不存在）
mkdir -p ~/.ssh
chmod 700 ~/.ssh

# 生成专用密钥对（使用有意义的名称）
ssh-keygen -t ed25519 -C "github-actions-nga-deploy" -f ~/.ssh/github_actions_nga

# 查看生成的密钥
ls -la ~/.ssh/github_actions_nga*

# 输出示例：
# github_actions_nga      (私钥 - 将添加到GitHub Secrets)
# github_actions_nga.pub  (公钥 - 将添加到服务器authorized_keys)
```

### 1.4 配置服务器authorized_keys

```bash
# 将新公钥添加到authorized_keys
cat ~/.ssh/github_actions_nga.pub >> ~/.ssh/authorized_keys

# 设置权限
chmod 600 ~/.ssh/authorized_keys

# 验证公钥已添加
cat ~/.ssh/authorized_keys | grep "github-actions-nga-deploy"
```

### 1.5 配置SSH多密钥管理

编辑SSH配置文件以支持多个仓库连接：

```bash
# 编辑SSH配置
nano ~/.ssh/config
```

添加以下内容（不修改现有配置）：

```ssh
# === 现有项目配置（保持不变） ===
# Host github-existing
#     HostName github.com
#     User git
#     IdentityFile ~/.ssh/existing_deploy_key

# === NGAStockAnalyze 项目配置（新增） ===
Host github-nga
    HostName github.com
    User git
    IdentityFile ~/.ssh/github_actions_nga
    IdentitiesOnly yes
```

```bash
# 设置权限
chmod 600 ~/.ssh/config
```

### 1.6 克隆项目到服务器

```bash
# 进入项目目录
cd /opt/NGAStockAnalyze  # 或 /home/deploy-nga/NGAStockAnalyze

# 使用SSH方式克隆（推荐）
git clone git@github.com:Alt3rmis/NGAStockAnalyze.git .

# 或使用HTTPS方式（需要token或公开仓库）
git clone https://github.com/Alt3rmis/NGAStockAnalyze.git .

# 设置git配置
git config user.email "deploy@example.com"
git config user.name "Deploy Bot"
```

### 1.7 安装项目依赖

```bash
# 创建Python虚拟环境
cd /opt/NGAStockAnalyze
python3 -m venv venv

# 激活并安装依赖
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
deactivate

# 验证安装
ls -la venv/lib/python*/site-packages/ | grep akshare
```

### 1.8 复制部署脚本并设置权限

```bash
# 确保部署脚本可执行
chmod +x /opt/NGAStockAnalyze/deploy/deploy.sh

# 测试部署脚本
/opt/NGAStockAnalyze/deploy/deploy.sh status
```

### 1.9 配置Systemd服务（如需后台运行）

```bash
# 复制服务文件
sudo cp /opt/NGAStockAnalyze/deploy/ngastockanalyze.service /etc/systemd/system/

# 编辑服务文件，确认路径正确
sudo nano /etc/systemd/system/ngastockanalyze.service
```

确认以下内容：

```ini
[Unit]
Description=NGAStockAnalyze Service
After=network.target

[Service]
Type=simple
User=root  # 或 deploy-nga
WorkingDirectory=/opt/NGAStockAnalyze
Environment="PATH=/opt/NGAStockAnalyze/venv/bin"
ExecStart=/opt/NGAStockAnalyze/venv/bin/python /opt/NGAStockAnalyze/main.py
Restart=always
RestartSec=10
StandardOutput=append:/opt/NGAStockAnalyze/logs/service.log
StandardError=append:/opt/NGAStockAnalyze/logs/error.log

[Install]
WantedBy=multi-user.target
```

```bash
# 重载systemd
sudo systemctl daemon-reload

# 启动服务（测试）
sudo systemctl start ngastockanalyze
sudo systemctl status ngastockanalyze

# 如需开机自启
sudo systemctl enable ngastockanalyze
```

---

## 第二部分：GitHub端配置

### 2.1 查看私钥内容

在服务器上执行：

```bash
cat ~/.ssh/github_actions_nga
```

复制完整输出，包括：
```
-----BEGIN OPENSSH PRIVATE KEY-----
...（多行内容）...
-----END OPENSSH PRIVATE KEY-----
```

### 2.2 配置Repository Secrets

1. 打开 https://github.com/Alt3rmis/NGAStockAnalyze
2. 点击 **Settings** 标签
3. 左侧菜单：**Secrets and variables** → **Actions**
4. 点击 **New repository secret**

添加以下Secrets：

| Name | Value | 说明 |
|:-----|:------|:-----|
| `NGA_DEPLOY_HOST` | 你的服务器IP | 如 `123.45.67.89` |
| `NGA_DEPLOY_USER` | 部署用户名 | `root` 或 `deploy-nga` |
| `NGA_DEPLOY_PORT` | SSH端口 | `22`（默认）或自定义端口 |
| `NGA_DEPLOY_KEY` | 私钥完整内容 | 1.1步骤复制的私钥 |
| `NGA_DEPLOY_PATH` | 项目路径 | `/opt/NGAStockAnalyze` |

**重要：使用唯一的Secret名称（如NGA_前缀），避免与现有仓库冲突**

### 2.3 配置Environment Variables（可选）

在同一页面的 **Variables** 标签：

| Name | Value |
|:-----|:------|
| `NGA_APP_URL` | `http://your-domain.com` |

### 2.4 配置部署环境

1. **Settings** → **Environments**
2. 点击 **New environment**
3. 创建 `production-nga`
4. 配置保护规则（可选）：
   - Required reviewers
   - Wait timer
   - Deployment branches: `main`

### 2.5 工作流文件说明

工作流文件 `.github/workflows/ci-cd.yml` 已配置完成，关键点：

```yaml
env:
  DEPLOY_PATH: ${{ secrets.NGA_DEPLOY_PATH || '/opt/NGAStockAnalyze' }}
```

**Secret命名隔离策略**：
- 使用 `NGA_` 前缀区分本项目
- 环境名称使用 `staging-nga` 和 `production-nga`
- Artifact名称使用 `test-report-nga`
- 部署标签使用 `nga-deploy-*`

---

## 第三部分：验证与测试

### 3.1 服务器端验证

```bash
# 1. 验证目录结构
ls -la /opt/NGAStockAnalyze/

# 2. 验证虚拟环境
source /opt/NGAStockAnalyze/venv/bin/activate
python -c "import akshare; print('akshare OK')"
deactivate

# 3. 验证部署脚本
/opt/NGAStockAnalyze/deploy/deploy.sh status

# 4. 验证SSH密钥（使用新密钥连接）
ssh -i ~/.ssh/github_actions_nga git@github.com
# 应显示: Hi Alt3rmis/NGAStockAnalyze! You've successfully authenticated...

# 5. 验证现有项目未受影响
ls -la /opt/existing-project/  # 检查现有项目目录
systemctl status existing-service  # 检查现有服务状态
```

### 3.2 GitHub Actions 测试

#### 测试1：手动触发工作流

1. 进入 https://github.com/Alt3rmis/NGAStockAnalyze/actions
2. 选择 **CI/CD Pipeline**
3. 点击 **Run workflow**
4. 选择分支 `main`
5. 观察运行日志

#### 测试2：推送触发

```bash
# 本地修改并推送
echo "# Test deployment" >> README.md
git add README.md
git commit -m "test: trigger deployment"
git push origin main
```

### 3.3 验证隔离性

#### 检查现有仓库工作流

1. 进入现有仓库的 Actions 页面
2. 确认最近的工作流运行正常
3. 检查部署日志无异常

#### 对比检查清单

| 检查项 | 现有仓库 | 新仓库 (NGAStockAnalyze) |
|:-------|:---------|:-------------------------|
| Secret名称 | `DEPLOY_*` | `NGA_DEPLOY_*` |
| 部署路径 | `/opt/project-a/` | `/opt/NGAStockAnalyze/` |
| SSH密钥 | `~/.ssh/deploy_key` | `~/.ssh/github_actions_nga` |
| Systemd服务 | `project-a.service` | `ngastockanalyze.service` |
| 环境名称 | `production` | `production-nga` |

---

## 第四部分：故障排查

### 4.1 SSH连接失败

```bash
# 检查SSH服务状态
systemctl status sshd

# 检查防火墙
ufw status
ufw allow 22/tcp

# 检查密钥权限
ls -la ~/.ssh/
# 应显示: authorized_keys (600), github_actions_nga (600), github_actions_nga.pub (644)

# 测试SSH连接
ssh -vvv -i ~/.ssh/github_actions_nga user@localhost
```

### 4.2 部署脚本执行失败

```bash
# 查看部署日志
tail -100 /opt/NGAStockAnalyze/logs/deploy_history.log

# 手动执行测试
cd /opt/NGAStockAnalyze
./deploy/deploy.sh update

# 检查git权限
git status
git pull origin main
```

### 4.3 服务启动失败

```bash
# 查看服务日志
journalctl -u ngastockanalyze -n 50

# 查看错误日志
tail -50 /opt/NGAStockAnalyze/logs/error.log

# 手动测试
cd /opt/NGAStockAnalyze
source venv/bin/activate
python main.py
```

### 4.4 回滚操作

```bash
# 回滚到上一版本
cd /opt/NGAStockAnalyze
./deploy/deploy.sh rollback

# 或手动回滚
git log --oneline -10
git checkout <previous-commit>
systemctl restart ngastockanalyze
```

---

## 第五部分：安全建议

### 5.1 SSH密钥管理

- ✅ 每个仓库使用独立的SSH密钥
- ✅ 密钥设置强密码（或无密码）
- ✅ 定期轮换密钥
- ✅ 私钥只在GitHub Secrets中存储

### 5.2 权限最小化

```bash
# 如果使用专用用户，限制sudo权限
visudo
# 添加: deploy-nga ALL=(ALL) NOPASSWD: /bin/systemctl restart ngastockanalyze
```

### 5.3 网络安全

```bash
# 限制SSH访问IP（可选）
ufw allow from YOUR_IP to any port 22

# 更改默认SSH端口（可选）
nano /etc/ssh/sshd_config
# Port 2222
```

---

## 快速配置清单

### 服务器端

- [ ] 创建项目目录 `/opt/NGAStockAnalyze`
- [ ] 生成专用SSH密钥 `~/.ssh/github_actions_nga`
- [ ] 添加公钥到 `authorized_keys`
- [ ] 克隆项目代码
- [ ] 创建虚拟环境并安装依赖
- [ ] 测试部署脚本
- [ ] 配置Systemd服务（可选）

### GitHub端

- [ ] 添加Secret `NGA_DEPLOY_HOST`
- [ ] 添加Secret `NGA_DEPLOY_USER`
- [ ] 添加Secret `NGA_DEPLOY_KEY`
- [ ] 添加Secret `NGA_DEPLOY_PATH`
- [ ] 创建Environment `staging-nga`
- [ ] 创建Environment `production-nga`
- [ ] 手动触发工作流测试

### 验证

- [ ] GitHub Actions运行成功
- [ ] 服务器代码已更新
- [ ] 服务运行正常
- [ ] 现有仓库工作流未受影响