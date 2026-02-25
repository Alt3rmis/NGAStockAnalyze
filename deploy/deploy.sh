#!/bin/bash

# A股多空情绪分析工具 - 服务器部署脚本
# 用法: ./deploy.sh [命令]
# 命令: install | update | start | stop | restart | status | rollback

set -e

# ==================== 配置 ====================
PROJECT_NAME="NGAStockAnalyze"
PROJECT_DIR="/opt/${PROJECT_NAME}"
VENV_DIR="${PROJECT_DIR}/venv"
LOG_DIR="${PROJECT_DIR}/logs"
BACKUP_DIR="/opt/backups/${PROJECT_NAME}"
GIT_REPO="https://github.com/Alt3rmis/NGAStockAnalyze.git"
BRANCH="main"
SERVICE_NAME="${PROJECT_NAME}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ==================== 安装依赖 ====================
install_dependencies() {
    log_info "安装系统依赖..."
    apt-get update
    apt-get install -y python3 python3-pip python3-venv git
    
    log_info "创建虚拟环境..."
    python3 -m venv "${VENV_DIR}"
    
    log_info "安装Python依赖..."
    source "${VENV_DIR}/bin/activate"
    pip install --upgrade pip
    pip install -r "${PROJECT_DIR}/requirements.txt"
    deactivate
    
    log_info "依赖安装完成"
}

# ==================== 更新代码 ====================
update_code() {
    log_info "开始更新代码..."
    
    cd "${PROJECT_DIR}"
    
    CURRENT_BRANCH=$(git branch --show-current)
    log_info "当前分支: ${CURRENT_BRANCH}"
    
    git fetch origin
    
    LOCAL=$(git rev-parse HEAD)
    REMOTE=$(git rev-parse "origin/${BRANCH}")
    
    if [ "$LOCAL" = "$REMOTE" ]; then
        log_info "代码已是最新，无需更新"
        return 0
    fi
    
    log_info "备份当前版本..."
    mkdir -p "${BACKUP_DIR}"
    BACKUP_NAME="backup_$(date +%Y%m%d_%H%M%S).tar.gz"
    tar -czf "${BACKUP_DIR}/${BACKUP_NAME}" \
        --exclude="${PROJECT_DIR}/venv" \
        --exclude="${PROJECT_DIR}/logs" \
        --exclude="${PROJECT_DIR}/.git" \
        "${PROJECT_DIR}" 2>/dev/null || true
    
    echo "${LOCAL}" > "${BACKUP_DIR}/last_commit.txt"
    log_info "备份已保存: ${BACKUP_NAME}"
    
    log_info "拉取最新代码..."
    git pull origin "${BRANCH}"
    
    log_info "更新依赖..."
    source "${VENV_DIR}/bin/activate"
    pip install -r "${PROJECT_DIR}/requirements.txt"
    deactivate
    
    log_info "代码更新完成"
}

# ==================== 启动服务 ====================
start_service() {
    log_info "启动服务..."
    
    if [ -f "/etc/systemd/system/${SERVICE_NAME}.service" ]; then
        systemctl start "${SERVICE_NAME}"
        systemctl enable "${SERVICE_NAME}"
        log_info "服务已通过systemd启动"
    else
        cd "${PROJECT_DIR}"
        source "${VENV_DIR}/bin/activate"
        nohup python main.py > "${LOG_DIR}/service.log" 2>&1 &
        echo $! > "${PROJECT_DIR}/.pid"
        deactivate
        log_info "服务已后台启动，PID: $(cat ${PROJECT_DIR}/.pid)"
    fi
}

# ==================== 停止服务 ====================
stop_service() {
    log_info "停止服务..."
    
    if [ -f "/etc/systemd/system/${SERVICE_NAME}.service" ]; then
        systemctl stop "${SERVICE_NAME}"
        log_info "服务已通过systemd停止"
    elif [ -f "${PROJECT_DIR}/.pid" ]; then
        PID=$(cat "${PROJECT_DIR}/.pid")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID"
            log_info "服务已停止，PID: ${PID}"
        fi
        rm -f "${PROJECT_DIR}/.pid"
    else
        log_warn "未找到运行中的服务"
    fi
}

# ==================== 重启服务 ====================
restart_service() {
    stop_service
    sleep 2
    start_service
    log_info "服务已重启"
}

# ==================== 服务状态 ====================
check_status() {
    if [ -f "/etc/systemd/system/${SERVICE_NAME}.service" ]; then
        systemctl status "${SERVICE_NAME}" --no-pager
    elif [ -f "${PROJECT_DIR}/.pid" ]; then
        PID=$(cat "${PROJECT_DIR}/.pid")
        if kill -0 "$PID" 2>/dev/null; then
            log_info "服务运行中，PID: ${PID}"
        else
            log_error "服务已停止（PID文件存在但进程不存在）"
        fi
    else
        log_info "服务未运行"
    fi
}

# ==================== 回滚 ====================
rollback() {
    log_warn "开始回滚..."
    
    if [ ! -f "${BACKUP_DIR}/last_commit.txt" ]; then
        log_error "未找到上次提交记录，无法回滚"
        exit 1
    fi
    
    LAST_COMMIT=$(cat "${BACKUP_DIR}/last_commit.txt")
    
    cd "${PROJECT_DIR}"
    git checkout "${LAST_COMMIT}"
    
    source "${VENV_DIR}/bin/activate"
    pip install -r "${PROJECT_DIR}/requirements.txt"
    deactivate
    
    log_info "已回滚到提交: ${LAST_COMMIT}"
    restart_service
}

# ==================== 初始化项目 ====================
init_project() {
    log_info "初始化项目..."
    
    mkdir -p "${PROJECT_DIR}"
    mkdir -p "${LOG_DIR}"
    mkdir -p "${BACKUP_DIR}"
    
    if [ ! -d "${PROJECT_DIR}/.git" ]; then
        log_info "克隆仓库..."
        git clone "${GIT_REPO}" "${PROJECT_DIR}"
        cd "${PROJECT_DIR}"
        git checkout "${BRANCH}"
    fi
    
    install_dependencies
    
    log_info "项目初始化完成"
}

# ==================== 主函数 ====================
case "${1}" in
    init)
        init_project
        ;;
    install)
        install_dependencies
        ;;
    update)
        update_code
        ;;
    start)
        start_service
        ;;
    stop)
        stop_service
        ;;
    restart)
        restart_service
        ;;
    status)
        check_status
        ;;
    rollback)
        rollback
        ;;
    *)
        echo "用法: $0 {init|install|update|start|stop|restart|status|rollback}"
        echo ""
        echo "命令说明:"
        echo "  init     - 首次初始化项目"
        echo "  install  - 安装/更新依赖"
        echo "  update   - 拉取最新代码"
        echo "  start    - 启动服务"
        echo "  stop     - 停止服务"
        echo "  restart  - 重启服务"
        echo "  status   - 查看服务状态"
        echo "  rollback - 回滚到上一版本"
        exit 1
        ;;
esac
