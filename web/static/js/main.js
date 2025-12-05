// 全局变量
let charts = {};
let currentData = null;
let allAddresses = []; // 存储当前时间区间的所有地址

// 批量计算状态
let batchState = {
    isRunning: false,
    isPaused: false,
    isCancelled: false,
    addresses: [],
    currentIndex: 0,
    currentViewingAddress: null,  // 当前查看的地址
    addressLogs: {},  // 每个地址的日志 {address: [logs]}
    addressWarnings: {},  // 每个地址的警告信息 {address: {hasSnapshotWarning: bool, warningCount: 0}}
    results: {
        success: [],
        fail: [],
        skip: []
    }
};

// API 基础 URL
const API_BASE = '';

// 初始化
document.addEventListener('DOMContentLoaded', function() {
    console.log('✅ 页面DOM加载完成');
    
    // 绑定标签页切换事件
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
    
    // 绑定事件（查看净值面板）
    document.getElementById('interval').addEventListener('change', loadAddresses);
    document.getElementById('loadBtn').addEventListener('click', loadData);
    document.getElementById('refreshBtn').addEventListener('click', () => {
        loadAddresses();
        if (document.getElementById('address').value) {
            loadData();
        }
    });
    document.getElementById('refreshCacheBtn').addEventListener('click', refreshCache);
    
    // 绑定事件（计算净值面板 - 单个地址）
    document.getElementById('calcBtn').addEventListener('click', startCalculation);
    document.getElementById('clearLogBtn').addEventListener('click', clearLog);
    document.getElementById('viewCalculatedBtn').addEventListener('click', viewCalculatedData);
    
    // 绑定事件（计算净值面板 - 批量计算）
    document.querySelectorAll('.calc-mode-btn').forEach(btn => {
        btn.addEventListener('click', () => switchCalcMode(btn.dataset.mode));
    });
    document.getElementById('batchAddresses').addEventListener('input', updateBatchAddressCount);
    document.getElementById('batchStartBtn').addEventListener('click', startBatchCalculation);
    document.getElementById('batchPauseBtn').addEventListener('click', pauseBatchCalculation);
    document.getElementById('batchResumeBtn').addEventListener('click', resumeBatchCalculation);
    document.getElementById('batchCancelBtn').addEventListener('click', cancelBatchCalculation);
    document.getElementById('toggleDetailBtn').addEventListener('click', toggleBatchDetail);
    document.getElementById('clearBatchLogBtn').addEventListener('click', clearBatchLog);
    
    // 绑定地址搜索事件
    document.getElementById('addressSearch').addEventListener('input', filterAddresses);
    document.getElementById('addressSearch').addEventListener('keyup', (e) => {
        if (e.key === 'Enter' && document.getElementById('address').options.length > 1) {
            // 按回车选中第一个搜索结果
            const select = document.getElementById('address');
            if (select.options[1]) { // 跳过第一个"请选择"选项
                select.selectedIndex = 1;
                updateSelectedAddress();
                loadData();
            }
        }
    });
    
    // 绑定地址选择变化事件
    document.getElementById('address').addEventListener('change', updateSelectedAddress);
    
    // 绑定复制按钮事件（使用新的复制按钮）
    document.getElementById('copyAddressBtn').addEventListener('click', copyDisplayAddress);
    
    // 绑定导出CSV按钮事件
    document.getElementById('exportCsvBtn').addEventListener('click', exportToCsv);
    
    // 绑定表格切换按钮事件
    document.getElementById('toggleTableBtn').addEventListener('click', toggleDataTable);
    
    // 绑定事件（过去持仓面板）
    document.getElementById('exportPositionsBtn').addEventListener('click', startPositionsExport);
    document.getElementById('clearPositionsLogBtn').addEventListener('click', clearPositionsLog);
    document.getElementById('downloadPositionsCsvBtn').addEventListener('click', downloadPositionsCsv);
    
    // 等待页面完全加载（包括图片、样式等）后再初始化图表
    if (document.readyState === 'complete') {
        initCharts();
    } else {
        window.addEventListener('load', () => {
            console.log('✅ 页面资源加载完成');
            initCharts();
        });
    }
    
    // 加载地址列表
    loadAddresses();
});

// 初始化图表
function initCharts() {
    // 延迟初始化，确保容器已完全渲染
    setTimeout(() => {
        charts.main = echarts.init(document.getElementById('mainChart'));
        charts.assets = echarts.init(document.getElementById('assetsChart'));
        charts.account = echarts.init(document.getElementById('accountChart'));
        charts.realizedPnl = echarts.init(document.getElementById('realizedPnlChart'));
        charts.virtualPnl = echarts.init(document.getElementById('virtualPnlChart'));
        
        // 初始化后立即调整大小
        Object.values(charts).forEach(chart => {
            if (chart) chart.resize();
        });
        
        console.log('✅ 图表初始化完成');
    }, 100);
    
    // 响应式调整
    window.addEventListener('resize', () => {
        Object.values(charts).forEach(chart => {
            if (chart) chart.resize();
        });
    });
    
    // 使用 ResizeObserver 监听容器大小变化（更可靠）
    if (window.ResizeObserver) {
        const chartContainers = [
            'mainChart', 
            'assetsChart', 
            'accountChart', 
            'realizedPnlChart', 
            'virtualPnlChart'
        ];
        
        chartContainers.forEach(id => {
            const container = document.getElementById(id);
            if (container) {
                const observer = new ResizeObserver(() => {
                    const chartKey = id.replace('Chart', '').replace('main', 'main');
                    if (charts[id.replace('Chart', '')] || charts.main && id === 'mainChart') {
                        const chart = id === 'mainChart' ? charts.main : charts[id.replace('Chart', '')];
                        if (chart) {
                            chart.resize();
                        }
                    }
                });
                observer.observe(container);
            }
        });
    }
}

// 加载地址列表
async function loadAddresses() {
    const interval = document.getElementById('interval').value;
    const addressSelect = document.getElementById('address');
    const addressSearch = document.getElementById('addressSearch');
    
    try {
        const response = await fetch(`${API_BASE}/api/addresses/${interval}`);
        const result = await response.json();
        
        if (result.success) {
            // 保存所有地址到全局变量
            allAddresses = result.data;
            
            // 清空搜索框
            addressSearch.value = '';
            
            // 显示所有地址
            displayAddresses(allAddresses);
            
            // 如果数据来自缓存，在控制台显示
            if (result.cached) {
                console.log(`✅ 从缓存加载 ${interval} 的地址列表 (${result.data.length} 个地址)`);
            }
        } else {
            showError('加载地址失败: ' + result.error);
        }
    } catch (error) {
        showError('加载地址失败: ' + error.message);
    }
}

// 显示地址列表
function displayAddresses(addresses) {
    const addressSelect = document.getElementById('address');
    const addressCount = document.getElementById('addressCount');
    
    // 清空现有选项
    addressSelect.innerHTML = '<option value="">请选择...</option>';
    
    // 添加地址选项（显示完整地址）
    addresses.forEach(addr => {
        const option = document.createElement('option');
        option.value = addr;
        option.textContent = addr; // 显示完整地址
        addressSelect.appendChild(option);
    });
    
    // 更新计数
    if (addresses.length > 0) {
        addressCount.textContent = `${addresses.length} 个`;
    } else {
        addressCount.textContent = '0';
    }
}

// 过滤地址列表
function filterAddresses() {
    const searchTerm = document.getElementById('addressSearch').value.toLowerCase().trim();
    
    if (!searchTerm) {
        // 如果搜索框为空，显示所有地址
        displayAddresses(allAddresses);
        return;
    }
    
    // 过滤地址
    const filteredAddresses = allAddresses.filter(addr => 
        addr.toLowerCase().includes(searchTerm)
    );
    
    // 显示过滤后的地址
    displayAddresses(filteredAddresses);
    
    // 如果只有一个匹配结果，自动选中
    if (filteredAddresses.length === 1) {
        document.getElementById('address').selectedIndex = 1;
        updateSelectedAddress();
    }
}

// 更新选中地址显示
function updateSelectedAddress() {
    const addressSelect = document.getElementById('address');
    const selectedAddress = addressSelect.value;
    const displayDiv = document.getElementById('selectedAddressDisplay');
    const textInput = document.getElementById('selectedAddressText');
    
    // 检查元素是否存在（兼容旧版布局）
    if (!displayDiv || !textInput) {
        return;
    }
    
    if (selectedAddress && selectedAddress !== '') {
        // 显示地址文本框
        displayDiv.style.display = 'block';
        textInput.value = selectedAddress;
    } else {
        // 隐藏地址文本框
        displayDiv.style.display = 'none';
        textInput.value = '';
    }
}

// 复制地址到剪贴板
async function copyAddress() {
    const textInput = document.getElementById('selectedAddressText');
    const copyBtn = document.getElementById('copyAddressBtn');
    const address = textInput.value;
    
    if (!address) {
        return;
    }
    
    try {
        // 使用现代 Clipboard API
        await navigator.clipboard.writeText(address);
        
        // 显示成功反馈
        const originalText = copyBtn.textContent;
        copyBtn.textContent = '✅ 已复制';
        copyBtn.classList.add('copied');
        
        // 2秒后恢复
        setTimeout(() => {
            copyBtn.textContent = originalText;
            copyBtn.classList.remove('copied');
        }, 2000);
        
        console.log('✅ 地址已复制到剪贴板:', address);
        
    } catch (err) {
        // 如果 Clipboard API 不可用，使用旧方法
        try {
            textInput.select();
            document.execCommand('copy');
            
            const originalText = copyBtn.textContent;
            copyBtn.textContent = '✅ 已复制';
            copyBtn.classList.add('copied');
            
            setTimeout(() => {
                copyBtn.textContent = originalText;
                copyBtn.classList.remove('copied');
            }, 2000);
            
            console.log('✅ 地址已复制到剪贴板 (fallback):', address);
            
        } catch (err2) {
            console.error('❌ 复制失败:', err2);
            alert('复制失败，请手动选择复制');
        }
    }
}

// 刷新缓存
async function refreshCache() {
    const btn = document.getElementById('refreshCacheBtn');
    const originalText = btn.textContent;
    
    try {
        // 显示加载状态
        btn.disabled = true;
        btn.textContent = '⏳ 刷新中...';
        
        const response = await fetch(`${API_BASE}/api/refresh-cache`, {
            method: 'POST'
        });
        const result = await response.json();
        
        if (result.success) {
            console.log('✅ 缓存刷新成功:', result.cache_info);
            
            // 刷新当前选中的时间区间的地址列表
            await loadAddresses();
            
            // 显示成功提示
            btn.textContent = '✅ 成功';
            setTimeout(() => {
                btn.textContent = originalText;
                btn.disabled = false;
            }, 2000);
        } else {
            showError('刷新缓存失败: ' + result.error);
            btn.textContent = originalText;
            btn.disabled = false;
        }
    } catch (error) {
        showError('刷新缓存失败: ' + error.message);
        btn.textContent = originalText;
        btn.disabled = false;
    }
}

// 加载数据
async function loadData() {
    const interval = document.getElementById('interval').value;
    const address = document.getElementById('address').value;
    const fromFirstTrade = document.getElementById('fromFirstTrade').checked;
    
    if (!address) {
        showError('请先选择账户地址');
        return;
    }
    
    // 显示加载状态
    showLoading();
    hideError();
    hideCharts();
    
    try {
        // 构建 URL 参数
        const params = new URLSearchParams();
        params.append('from_first_trade', fromFirstTrade ? 'true' : 'false');
        
        const response = await fetch(`${API_BASE}/api/netvalue/${interval}/${address}?${params.toString()}`);
        const result = await response.json();
        
        if (result.success) {
            currentData = result;
            updateStats(result.stats);
            updateCharts(result.data);
            updateDataTable(result.data);  // 更新数据表格
            updateDataInfo(interval, address, result.stats);  // 更新数据信息面板
            showCharts();
            showDataInfo();  // 显示数据信息面板
        } else {
            showError('加载数据失败: ' + result.error);
        }
    } catch (error) {
        showError('加载数据失败: ' + error.message);
    } finally {
        hideLoading();
    }
}

// 更新统计卡片
function updateStats(stats) {
    // 当前净值
    document.getElementById('currentNetValue').textContent = stats.last_net_value.toFixed(4);
    
    const netValueChange = stats.last_net_value - stats.first_net_value;
    const netValueChangeEl = document.getElementById('netValueChange');
    netValueChangeEl.textContent = (netValueChange >= 0 ? '+' : '') + netValueChange.toFixed(4);
    netValueChangeEl.className = 'stat-change ' + (netValueChange >= 0 ? 'positive' : 'negative');
    
    // 收益率
    const returnRate = stats.return_rate;
    const returnRateEl = document.getElementById('returnRate');
    returnRateEl.textContent = (returnRate >= 0 ? '+' : '') + returnRate.toFixed(2) + '%';
    returnRateEl.style.color = returnRate >= 0 ? '#00C853' : '#D50000';
    
    // 累计 PnL
    document.getElementById('cumulativePnl').textContent = '$' + stats.last_pnl.toLocaleString('en-US', {maximumFractionDigits: 2});
    
    const pnlChange = stats.last_pnl - stats.first_pnl;
    const pnlChangeEl = document.getElementById('pnlChange');
    pnlChangeEl.textContent = (pnlChange >= 0 ? '+' : '') + '$' + pnlChange.toLocaleString('en-US', {maximumFractionDigits: 2});
    pnlChangeEl.className = 'stat-change ' + (pnlChange >= 0 ? 'positive' : 'negative');
    
    // 总资产（使用最后一条数据）
    if (currentData && currentData.data && currentData.data.total_assets.length > 0) {
        const lastAssets = currentData.data.total_assets[currentData.data.total_assets.length - 1];
        document.getElementById('totalAssets').textContent = '$' + lastAssets.toLocaleString('en-US', {maximumFractionDigits: 2});
    }
}

// 更新图表
function updateCharts(data) {
    // 转换时间戳为日期
    const times = data.timestamps.map(ts => new Date(ts).toLocaleString('zh-CN'));
    
    // 主图表：净值与 PnL
    charts.main.setOption({
        title: {
            text: ''
        },
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'cross'
            }
        },
        legend: {
            data: ['净值', '累计 PnL']
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            data: times,
            boundaryGap: false
        },
        yAxis: [
            {
                type: 'value',
                name: '净值',
                position: 'left'
            },
            {
                type: 'value',
                name: '累计 PnL ($)',
                position: 'right'
            }
        ],
        series: [
            {
                name: '净值',
                type: 'line',
                data: data.net_values,
                smooth: true,
                itemStyle: {
                    color: '#667eea'
                },
                areaStyle: {
                    color: {
                        type: 'linear',
                        x: 0,
                        y: 0,
                        x2: 0,
                        y2: 1,
                        colorStops: [
                            {offset: 0, color: 'rgba(102, 126, 234, 0.5)'},
                            {offset: 1, color: 'rgba(102, 126, 234, 0.05)'}
                        ]
                    }
                }
            },
            {
                name: '累计 PnL',
                type: 'line',
                yAxisIndex: 1,
                data: data.cumulative_pnl,
                smooth: true,
                itemStyle: {
                    color: '#ff7f0e'
                }
            }
        ]
    });
    
    // 总资产走势
    charts.assets.setOption({
        tooltip: {
            trigger: 'axis'
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            data: times,
            boundaryGap: false
        },
        yAxis: {
            type: 'value',
            name: '总资产 ($)'
        },
        series: [{
            name: '总资产',
            type: 'line',
            data: data.total_assets,
            smooth: true,
            itemStyle: {
                color: '#2ca02c'
            },
            areaStyle: {
                color: {
                    type: 'linear',
                    x: 0,
                    y: 0,
                    x2: 0,
                    y2: 1,
                    colorStops: [
                        {offset: 0, color: 'rgba(44, 160, 44, 0.5)'},
                        {offset: 1, color: 'rgba(44, 160, 44, 0.05)'}
                    ]
                }
            }
        }]
    });
    
    // 现货 vs 合约
    charts.account.setOption({
        tooltip: {
            trigger: 'axis'
        },
        legend: {
            data: ['现货账户', '合约账户']
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            data: times,
            boundaryGap: false
        },
        yAxis: {
            type: 'value',
            name: '账户价值 ($)'
        },
        series: [
            {
                name: '现货账户',
                type: 'line',
                stack: '账户',
                data: data.spot_account_value,
                itemStyle: {
                    color: '#9467bd'
                },
                areaStyle: {}
            },
            {
                name: '合约账户',
                type: 'line',
                stack: '账户',
                data: data.perp_account_value,
                itemStyle: {
                    color: '#e377c2'
                },
                areaStyle: {}
            }
        ]
    });
    
    // 已实现盈亏
    const realizedColors = data.realized_pnl.map(v => v >= 0 ? '#00C853' : '#D50000');
    charts.realizedPnl.setOption({
        tooltip: {
            trigger: 'axis'
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            data: times
        },
        yAxis: {
            type: 'value',
            name: '已实现 PnL ($)'
        },
        series: [{
            name: '已实现盈亏',
            type: 'bar',
            data: data.realized_pnl,
            itemStyle: {
                color: function(params) {
                    return params.data >= 0 ? '#00C853' : '#D50000';
                }
            }
        }]
    });
    
    // 虚拟盈亏
    charts.virtualPnl.setOption({
        tooltip: {
            trigger: 'axis'
        },
        grid: {
            left: '3%',
            right: '4%',
            bottom: '3%',
            containLabel: true
        },
        xAxis: {
            type: 'category',
            data: times
        },
        yAxis: {
            type: 'value',
            name: '虚拟 PnL ($)'
        },
        series: [{
            name: '虚拟盈亏',
            type: 'bar',
            data: data.virtual_pnl,
            itemStyle: {
                color: function(params) {
                    return params.data >= 0 ? '#00C853' : '#D50000';
                }
            }
        }]
    });
    
    // 更新完所有图表后，确保它们正确渲染
    setTimeout(() => {
        Object.values(charts).forEach(chart => {
            if (chart) {
                chart.resize();
            }
        });
        console.log('✅ 图表数据更新并调整大小完成');
    }, 50);
}

// UI 辅助函数
function showLoading() {
    document.getElementById('loading').style.display = 'block';
}

function hideLoading() {
    document.getElementById('loading').style.display = 'none';
}

function showError(message) {
    const errorEl = document.getElementById('error');
    errorEl.textContent = '❌ ' + message;
    errorEl.style.display = 'block';
}

function hideError() {
    document.getElementById('error').style.display = 'none';
}

function showCharts() {
    document.getElementById('statsCards').style.display = 'grid';
    document.querySelector('.charts-container').style.display = 'block';
    
    // 显示后延迟调整图表大小，确保容器已完全显示
    setTimeout(() => {
        Object.values(charts).forEach(chart => {
            if (chart) {
                chart.resize();
            }
        });
        console.log('✅ 图表大小已调整');
    }, 100);
}

function hideCharts() {
    document.getElementById('statsCards').style.display = 'none';
    document.querySelector('.charts-container').style.display = 'none';
    document.getElementById('dataInfoPanel').style.display = 'none';  // 同时隐藏数据信息面板
}

// 手动刷新所有图表大小（用于调试和手动修复）
function resizeAllCharts() {
    Object.values(charts).forEach(chart => {
        if (chart) {
            chart.resize();
        }
    });
    console.log('✅ 手动调整所有图表大小');
}

// 将函数暴露到全局，方便调试
window.resizeAllCharts = resizeAllCharts;

// 更新数据信息面板
function updateDataInfo(interval, address, stats = null) {
    // 更新时间区间显示
    const intervalMap = {
        '1m': '1分钟',
        '3m': '3分钟',
        '5m': '5分钟',
        '15m': '15分钟',
        '30m': '30分钟',
        '1h': '1小时',
        '2h': '2小时',
        '4h': '4小时',
        '8h': '8小时',
        '12h': '12小时',
        '1d': '1天'
    };
    
    let intervalDisplay = intervalMap[interval] || interval;
    
    // 显示是否从第一笔交易开始
    if (stats && stats.from_first_trade !== undefined) {
        const fromFirstTradeText = stats.from_first_trade ? '（从第一笔交易开始）' : '（全部数据）';
        intervalDisplay += ` ${fromFirstTradeText}`;
    }
    
    document.getElementById('displayInterval').textContent = intervalDisplay;
    
    // 更新地址显示
    document.getElementById('displayAddress').value = address;
    
    // 存储当前的interval和address，供导出功能使用
    window.currentInterval = interval;
    window.currentAddress = address;
}

// 显示数据信息面板
function showDataInfo() {
    document.getElementById('dataInfoPanel').style.display = 'block';
}

// 隐藏数据信息面板
function hideDataInfo() {
    document.getElementById('dataInfoPanel').style.display = 'none';
}

// 复制显示的地址到剪贴板
async function copyDisplayAddress() {
    const addressInput = document.getElementById('displayAddress');
    const copyBtn = document.getElementById('copyAddressBtn');
    const address = addressInput.value;
    
    if (!address) {
        return;
    }
    
    try {
        // 使用现代 Clipboard API
        await navigator.clipboard.writeText(address);
        
        // 显示成功反馈
        const originalText = copyBtn.textContent;
        copyBtn.textContent = '✅ 已复制';
        
        // 2秒后恢复
        setTimeout(() => {
            copyBtn.textContent = originalText;
        }, 2000);
        
        console.log('✅ 地址已复制到剪贴板:', address);
        
    } catch (err) {
        // 如果 Clipboard API 不可用，使用旧方法
        try {
            addressInput.select();
            document.execCommand('copy');
            
            const originalText = copyBtn.textContent;
            copyBtn.textContent = '✅ 已复制';
            
            setTimeout(() => {
                copyBtn.textContent = originalText;
            }, 2000);
            
            console.log('✅ 地址已复制到剪贴板 (fallback):', address);
            
        } catch (err2) {
            console.error('❌ 复制失败:', err2);
            alert('复制失败，请手动选择复制');
        }
    }
}

// 导出CSV
async function exportToCsv() {
    const interval = window.currentInterval;
    const address = window.currentAddress;
    
    if (!interval || !address) {
        showError('请先加载数据');
        return;
    }
    
    const btn = document.getElementById('exportCsvBtn');
    const originalText = btn.textContent;
    
    try {
        // 显示加载状态
        btn.disabled = true;
        btn.textContent = '⏳ 导出中...';
        
        // 请求导出API
        const response = await fetch(`${API_BASE}/api/export/${interval}/${address}`);
        
        if (!response.ok) {
            throw new Error('导出失败: ' + response.statusText);
        }
        
        // 获取文件内容
        const blob = await response.blob();
        
        // 创建下载链接
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.style.display = 'none';
        a.href = url;
        
        // 从响应头获取文件名，如果没有则生成一个
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = `netvalue_${interval}_${address.substring(0, 10)}.csv`;
        
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?(.+)"?/i);
            if (filenameMatch) {
                filename = filenameMatch[1];
            }
        }
        
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        
        // 清理
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        // 显示成功提示
        btn.textContent = '✅ 已导出';
        setTimeout(() => {
            btn.textContent = originalText;
            btn.disabled = false;
        }, 2000);
        
        console.log('✅ CSV导出成功');
        
    } catch (error) {
        console.error('❌ CSV导出失败:', error);
        showError('导出失败: ' + error.message);
        btn.textContent = originalText;
        btn.disabled = false;
    }
}

// ==================== 计算净值相关函数 ====================

// 切换标签页
function switchTab(tabName) {
    // 切换按钮状态
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.dataset.tab === tabName) {
            btn.classList.add('active');
        }
    });
    
    // 切换面板显示
    document.querySelectorAll('.tab-panel').forEach(panel => {
        panel.classList.remove('active');
    });
    
    if (tabName === 'view') {
        document.getElementById('viewPanel').classList.add('active');
    } else if (tabName === 'calculate') {
        document.getElementById('calculatePanel').classList.add('active');
    } else if (tabName === 'positions') {
        document.getElementById('positionsPanel').classList.add('active');
    }
}

// 开始计算
async function startCalculation() {
    const interval = document.getElementById('calcInterval').value;
    const address = document.getElementById('calcAddress').value.trim();
    const forceOverwrite = document.getElementById('forceOverwrite').checked;
    
    // 验证输入
    if (!address) {
        showCalcStatus('error', '请输入账户地址', '地址不能为空');
        return;
    }
    
    if (!address.match(/^0x[a-fA-F0-9]{40}$/)) {
        showCalcStatus('warning', '地址格式可能不正确', '标准以太坊地址应该是42个字符（0x + 40个十六进制字符）');
        return;
    }
    
    // 禁用按钮
    const calcBtn = document.getElementById('calcBtn');
    const originalText = calcBtn.textContent;
    calcBtn.disabled = true;
    calcBtn.textContent = '⏳ 检查中...';
    
    try {
        // 如果勾选了强制覆盖，直接开始计算
        if (forceOverwrite) {
            calcBtn.textContent = '⏳ 计算中...';
            await performCalculation(interval, address, true);
            return;
        }
        
        // 1. 检查数据是否存在
        const checkResponse = await fetch(`${API_BASE}/api/check-data/${interval}/${address}`);
        const checkResult = await checkResponse.json();
        
        if (checkResult.success && checkResult.exists) {
            // 数据已存在，显示提示
            const lastUpdate = checkResult.last_update ? 
                new Date(checkResult.last_update).toLocaleString('zh-CN') : '未知';
            
            showCalcStatusWithActions(
                'info',
                '数据已存在',
                `该地址在 ${interval} 周期已有数据（最后更新：${lastUpdate}）`,
                interval,
                address
            );
            
            calcBtn.textContent = originalText;
            calcBtn.disabled = false;
            return;
        }
        
        // 2. 数据不存在，开始计算
        calcBtn.textContent = '⏳ 计算中...';
        await performCalculation(interval, address, forceOverwrite);
        
    } catch (error) {
        showCalcStatus('error', '操作失败', error.message);
        calcBtn.textContent = originalText;
        calcBtn.disabled = false;
    }
}

// 强制重新计算（从"数据已存在"提示中调用，强制全量覆盖）
async function forceCalculation(interval, address) {
    hideCalcStatus();
    const calcBtn = document.getElementById('calcBtn');
    calcBtn.disabled = true;
    calcBtn.textContent = '⏳ 计算中...';
    
    // 强制重新计算时，使用全量覆盖模式
    await performCalculation(interval, address, true);
}

// 执行计算
async function performCalculation(interval, address, forceOverwrite = false) {
    // 显示日志容器
    document.getElementById('logContainer').style.display = 'block';
    document.getElementById('calcComplete').style.display = 'none';
    clearLog();
    
    // 构建 URL，包含 force_overwrite 参数
    const params = new URLSearchParams();
    if (forceOverwrite) {
        params.append('force_overwrite', 'true');
    }
    const url = `${API_BASE}/api/calculate/${interval}/${address}${params.toString() ? '?' + params.toString() : ''}`;
    
    // 使用EventSource接收SSE流
    const eventSource = new EventSource(url);
    
    eventSource.onmessage = function(event) {
        try {
            const data = JSON.parse(event.data);
            
            if (data.type === 'log') {
                appendLog(data.message);
            } else if (data.type === 'complete') {
                eventSource.close();
                
                const calcBtn = document.getElementById('calcBtn');
                calcBtn.disabled = false;
                calcBtn.textContent = '🚀 开始计算';
                
                if (data.success) {
                    // 显示完成界面
                    showCalculationComplete(interval, address);
                } else {
                    appendLog('❌ 计算失败', 'error');
                    showCalcStatus('error', '计算失败', '请查看日志了解详细信息');
                }
            }
        } catch (error) {
            console.error('解析SSE消息失败:', error);
        }
    };
    
    eventSource.onerror = function(error) {
        console.error('SSE连接错误:', error);
        eventSource.close();
        
        const calcBtn = document.getElementById('calcBtn');
        calcBtn.disabled = false;
        calcBtn.textContent = '🚀 开始计算';
        
        appendLog('❌ 连接中断', 'error');
        showCalcStatus('error', '连接失败', '与服务器的连接中断，请重试');
    };
}

// 显示计算完成界面
function showCalculationComplete(interval, address) {
    // 保留日志容器，不要隐藏，用户可能需要回头查看
    // document.getElementById('logContainer').style.display = 'none';
    document.getElementById('calcComplete').style.display = 'block';
    
    // 存储当前计算的interval和address，供查看按钮使用
    window.calculatedInterval = interval;
    window.calculatedAddress = address;
}

// 查看计算后的数据
function viewCalculatedData() {
    const interval = window.calculatedInterval;
    const address = window.calculatedAddress;
    
    if (!interval || !address) {
        return;
    }
    
    // 切换到查看面板
    switchTab('view');
    
    // 设置interval
    document.getElementById('interval').value = interval;
    
    // 加载地址列表并选中
    loadAddresses().then(() => {
        // 等待地址列表加载完成后选中地址
        setTimeout(() => {
            const addressSelect = document.getElementById('address');
            addressSelect.value = address;
            
            // 加载数据
            loadData();
        }, 500);
    });
}

// 查看已存在的数据
function viewExistingData(interval, address) {
    // 切换到查看面板
    switchTab('view');
    
    // 设置interval
    document.getElementById('interval').value = interval;
    
    // 加载地址列表并选中
    loadAddresses().then(() => {
        setTimeout(() => {
            const addressSelect = document.getElementById('address');
            addressSelect.value = address;
            loadData();
        }, 500);
    });
}

// 显示计算状态提示（带操作按钮）
function showCalcStatusWithActions(type, title, message, interval, address) {
    const statusEl = document.getElementById('calcStatus');
    statusEl.className = `calc-status ${type}`;
    
    statusEl.innerHTML = `
        <div class="status-content">
            <div class="status-text">
                <h4>${title}</h4>
                <p>${message}</p>
            </div>
            <div class="status-actions">
                <button class="btn-secondary" onclick="viewExistingData('${interval}', '${address}')">📊 直接查看</button>
                <button class="btn-secondary" onclick="forceCalculation('${interval}', '${address}')">♻️ 重新计算</button>
            </div>
        </div>
    `;
    
    statusEl.style.display = 'block';
}

// 显示计算状态提示
function showCalcStatus(type, title, message) {
    const statusEl = document.getElementById('calcStatus');
    statusEl.className = `calc-status ${type}`;
    
    statusEl.innerHTML = `
        <div class="status-content">
            <div class="status-text">
                <h4>${title}</h4>
                <p>${message}</p>
            </div>
        </div>
    `;
    
    statusEl.style.display = 'block';
}

// 隐藏计算状态提示
function hideCalcStatus() {
    document.getElementById('calcStatus').style.display = 'none';
}

// 添加日志
function appendLog(message, type = '') {
    const logContent = document.getElementById('logContent');
    const logLine = document.createElement('div');
    logLine.className = `log-line ${type}`;
    logLine.textContent = message;
    logContent.appendChild(logLine);
    
    // 自动滚动到底部
    logContent.scrollTop = logContent.scrollHeight;
}

// 清空日志
function clearLog() {
    document.getElementById('logContent').innerHTML = '';
}

// ==================== 批量计算相关函数 ====================

// 切换计算模式
function switchCalcMode(mode) {
    // 切换按钮状态
    document.querySelectorAll('.calc-mode-btn').forEach(btn => {
        btn.classList.remove('active');
        if (btn.dataset.mode === mode) {
            btn.classList.add('active');
        }
    });
    
    // 切换面板显示
    document.querySelectorAll('.calc-mode-panel').forEach(panel => {
        panel.classList.remove('active');
    });
    
    if (mode === 'single') {
        document.getElementById('singleCalcPanel').classList.add('active');
    } else if (mode === 'batch') {
        document.getElementById('batchCalcPanel').classList.add('active');
    }
}

// 更新批量地址计数
function updateBatchAddressCount() {
    const textarea = document.getElementById('batchAddresses');
    const addresses = parseBatchAddresses(textarea.value);
    document.getElementById('batchAddressCount').textContent = addresses.length;
}

// 解析批量地址
function parseBatchAddresses(text) {
    return text
        .split('\n')
        .map(line => line.trim())
        .filter(line => line.length > 0 && line.startsWith('0x'));
}

// 开始批量计算
async function startBatchCalculation() {
    const interval = document.getElementById('batchInterval').value;
    const forceOverwrite = document.getElementById('batchForceOverwrite').checked;
    const addresses = parseBatchAddresses(document.getElementById('batchAddresses').value);
    
    if (addresses.length === 0) {
        alert('请输入至少一个有效的地址');
        return;
    }
    
    // 初始化批量状态
    batchState = {
        isRunning: true,
        isPaused: false,
        isCancelled: false,
        addresses: addresses,
        currentIndex: 0,
        currentViewingAddress: null,
        addressLogs: {},
        addressWarnings: {},
        interval: interval,
        forceOverwrite: forceOverwrite,
        results: {
            success: [],
            fail: [],
            skip: []
        }
    };
    
    // 为每个地址初始化日志数组和警告信息
    addresses.forEach(addr => {
        batchState.addressLogs[addr] = [];
        batchState.addressWarnings[addr] = {
            hasSnapshotWarning: false,
            warningCount: 0
        };
    });
    
    // 更新 UI
    updateBatchUI('running');
    initBatchDetailList();
    clearBatchLog();  // 清空之前的日志
    
    // 开始处理
    await processBatchQueue();
}

// 暂停批量计算
function pauseBatchCalculation() {
    batchState.isPaused = true;
    updateBatchUI('paused');
}

// 继续批量计算
async function resumeBatchCalculation() {
    batchState.isPaused = false;
    updateBatchUI('running');
    await processBatchQueue();
}

// 取消批量计算
function cancelBatchCalculation() {
    batchState.isCancelled = true;
    batchState.isRunning = false;
    updateBatchUI('cancelled');
}

// 处理批量队列
async function processBatchQueue() {
    const total = batchState.addresses.length;
    
    while (batchState.currentIndex < total) {
        // 检查是否暂停或取消
        if (batchState.isPaused) {
            return; // 暂停时退出，等待继续
        }
        if (batchState.isCancelled) {
            return; // 取消时退出
        }
        
        const address = batchState.addresses[batchState.currentIndex];
        
        // 添加日志分隔线到该地址的日志
        appendBatchLogToAddress(address, `━━━ [${batchState.currentIndex + 1}/${total}] ${address} ━━━`, 'separator');
        
        // 更新详情列表中的当前项为处理中
        updateDetailItemStatus(batchState.currentIndex, 'processing', '计算中...');
        updateBatchProgress();
        
        // 自动切换到当前地址的日志显示
        switchToAddressLog(address);
        
        try {
            // 执行单个地址的计算
            const result = await calculateSingleAddress(
                address, 
                batchState.interval, 
                batchState.forceOverwrite
            );
            
            if (result.success) {
                batchState.results.success.push(address);
                
                // 检查是否有快照校验警告
                const warnings = batchState.addressWarnings[address];
                if (warnings && warnings.hasSnapshotWarning) {
                    updateDetailItemStatus(
                        batchState.currentIndex, 
                        'success-warning', 
                        `成功 (${warnings.warningCount}个快照校验警告)`
                    );
                    appendBatchLogToAddress(address, `✅ 计算成功 (包含 ${warnings.warningCount} 个快照校验警告)`, 'warning');
                } else {
                    updateDetailItemStatus(batchState.currentIndex, 'success', '成功');
                    appendBatchLogToAddress(address, '✅ 计算成功', 'success');
                }
            } else if (result.skipped) {
                batchState.results.skip.push(address);
                updateDetailItemStatus(batchState.currentIndex, 'skip', '已跳过');
                appendBatchLogToAddress(address, '⏭️ 已跳过', 'warning');
            } else {
                batchState.results.fail.push(address);
                updateDetailItemStatus(batchState.currentIndex, 'fail', '失败');
                appendBatchLogToAddress(address, '❌ 计算失败', 'error');
            }
        } catch (error) {
            console.error(`计算失败 [${address}]:`, error);
            batchState.results.fail.push(address);
            updateDetailItemStatus(batchState.currentIndex, 'fail', '失败');
            appendBatchLogToAddress(address, `❌ 错误: ${error.message}`, 'error');
        }
        
        batchState.currentIndex++;
        updateBatchProgress();
    }
    
    // 所有任务完成
    batchState.isRunning = false;
    updateBatchUI('completed');
}

// 计算单个地址（返回 Promise）
function calculateSingleAddress(address, interval, forceOverwrite) {
    return new Promise((resolve) => {
        // 构建 URL
        const params = new URLSearchParams();
        if (forceOverwrite) {
            params.append('force_overwrite', 'true');
        }
        const url = `${API_BASE}/api/calculate/${interval}/${address}${params.toString() ? '?' + params.toString() : ''}`;
        
        let eventSource = null;
        let resolved = false;
        let lastActivityTime = Date.now();
        let hasReceivedComplete = false;
        
        const cleanup = () => {
            if (eventSource) {
                eventSource.close();
                eventSource = null;
            }
        };
        
        const doResolve = (result) => {
            if (!resolved) {
                resolved = true;
                cleanup();
                resolve(result);
            }
        };
        
        // 超时检测（5分钟无活动则认为失败）
        const timeoutChecker = setInterval(() => {
            if (Date.now() - lastActivityTime > 5 * 60 * 1000) {
                console.warn('计算超时，强制结束:', address);
                clearInterval(timeoutChecker);
                appendBatchLogToAddress(address, `⚠️ 计算超时`, 'warning');
                doResolve({ success: false, skipped: false });
            }
        }, 10000);
        
        try {
            eventSource = new EventSource(url);
            
            eventSource.onopen = function() {
                console.log('SSE连接已建立:', address);
                lastActivityTime = Date.now();
            };
            
            eventSource.onmessage = function(event) {
                lastActivityTime = Date.now();
                try {
                    const data = JSON.parse(event.data);
                    
                    if (data.type === 'log') {
                        // 显示日志到该地址的日志区域
                        appendBatchLogToAddress(address, data.message);
                    } else if (data.type === 'complete') {
                        hasReceivedComplete = true;
                        clearInterval(timeoutChecker);
                        doResolve({ success: data.success, skipped: false });
                    }
                } catch (error) {
                    console.error('解析SSE消息失败:', error, event.data);
                }
            };
            
            eventSource.onerror = function(error) {
                clearInterval(timeoutChecker);
                
                // 如果已经收到 complete，onerror 是正常的连接关闭
                if (hasReceivedComplete) {
                    console.log('SSE连接正常关闭:', address);
                    return;
                }
                
                // 检查连接状态
                if (eventSource && eventSource.readyState === EventSource.CLOSED) {
                    console.warn('SSE连接被关闭，未收到complete:', address);
                    appendBatchLogToAddress(address, `⚠️ 连接异常关闭`, 'warning');
                } else {
                    console.error('SSE连接错误:', address, error);
                    appendBatchLogToAddress(address, `⚠️ 连接错误`, 'error');
                }
                
                doResolve({ success: false, skipped: false });
            };
            
        } catch (error) {
            console.error('创建EventSource失败:', error);
            clearInterval(timeoutChecker);
            appendBatchLogToAddress(address, `❌ 无法建立连接: ${error.message}`, 'error');
            doResolve({ success: false, skipped: false });
        }
    });
}

// 初始化详情列表
function initBatchDetailList() {
    const detailList = document.getElementById('batchDetailList');
    detailList.innerHTML = '';
    
    batchState.addresses.forEach((address, index) => {
        const item = document.createElement('div');
        item.className = 'batch-detail-item';
        item.id = `batch-detail-${index}`;
        item.style.cursor = 'pointer';
        item.innerHTML = `
            <span class="detail-index">#${index + 1}</span>
            <span class="detail-address">${address}</span>
            <span class="detail-status pending">等待中</span>
        `;
        
        // 添加点击事件
        item.addEventListener('click', () => {
            switchToAddressLog(address);
        });
        
        detailList.appendChild(item);
    });
}

// 更新详情项状态
function updateDetailItemStatus(index, status, text) {
    const item = document.getElementById(`batch-detail-${index}`);
    if (item) {
        item.className = `batch-detail-item ${status}`;
        const statusEl = item.querySelector('.detail-status');
        statusEl.className = `detail-status ${status}`;
        statusEl.textContent = text;
        
        // 如果是 success-warning，添加提示
        if (status === 'success-warning') {
            const address = batchState.addresses[index];
            const warnings = batchState.addressWarnings[address];
            if (warnings) {
                item.title = `计算成功，但存在 ${warnings.warningCount} 个快照校验警告。点击查看详情。`;
            }
        }
        
        // 滚动到当前项
        item.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
}

// 更新批量进度
function updateBatchProgress() {
    const total = batchState.addresses.length;
    const current = batchState.currentIndex;
    const successCount = batchState.results.success.length;
    const failCount = batchState.results.fail.length;
    const skipCount = batchState.results.skip.length;
    const processed = successCount + failCount + skipCount;
    
    // 计算有警告的成功数量
    let warningCount = 0;
    batchState.results.success.forEach(addr => {
        if (batchState.addressWarnings[addr] && batchState.addressWarnings[addr].hasSnapshotWarning) {
            warningCount++;
        }
    });
    
    // 更新进度条
    const percentage = total > 0 ? (processed / total) * 100 : 0;
    document.getElementById('batchProgressBar').style.width = `${percentage}%`;
    
    // 更新统计数字
    document.getElementById('batchTotalCount').textContent = total;
    document.getElementById('batchSuccessCount').textContent = successCount;
    document.getElementById('batchWarningCount').textContent = warningCount;
    document.getElementById('batchFailCount').textContent = failCount;
    document.getElementById('batchSkipCount').textContent = skipCount;
    document.getElementById('batchCurrentIndex').textContent = current;
    document.getElementById('batchTotalIndex').textContent = total;
}

// 更新批量 UI 状态
function updateBatchUI(status) {
    const startBtn = document.getElementById('batchStartBtn');
    const pauseBtn = document.getElementById('batchPauseBtn');
    const resumeBtn = document.getElementById('batchResumeBtn');
    const cancelBtn = document.getElementById('batchCancelBtn');
    const progressPanel = document.getElementById('batchProgressPanel');
    const logContainer = document.getElementById('batchLogContainer');
    const statusEl = document.getElementById('batchProgressStatus');
    const textarea = document.getElementById('batchAddresses');
    
    // 显示进度面板和日志容器
    progressPanel.style.display = 'block';
    logContainer.style.display = 'block';
    
    switch (status) {
        case 'running':
            startBtn.disabled = true;
            pauseBtn.disabled = false;
            pauseBtn.style.display = 'inline-block';
            resumeBtn.style.display = 'none';
            cancelBtn.disabled = false;
            textarea.disabled = true;
            statusEl.textContent = '计算中...';
            statusEl.className = 'progress-status';
            break;
            
        case 'paused':
            startBtn.disabled = true;
            pauseBtn.style.display = 'none';
            resumeBtn.style.display = 'inline-block';
            cancelBtn.disabled = false;
            textarea.disabled = true;
            statusEl.textContent = '已暂停';
            statusEl.className = 'progress-status paused';
            break;
            
        case 'completed':
            startBtn.disabled = false;
            pauseBtn.disabled = true;
            pauseBtn.style.display = 'inline-block';
            resumeBtn.style.display = 'none';
            cancelBtn.disabled = true;
            textarea.disabled = false;
            statusEl.textContent = '已完成';
            statusEl.className = 'progress-status completed';
            break;
            
        case 'cancelled':
            startBtn.disabled = false;
            pauseBtn.disabled = true;
            pauseBtn.style.display = 'inline-block';
            resumeBtn.style.display = 'none';
            cancelBtn.disabled = true;
            textarea.disabled = false;
            statusEl.textContent = '已取消';
            statusEl.className = 'progress-status cancelled';
            break;
    }
}

// 切换详情列表显示
function toggleBatchDetail() {
    const detailList = document.getElementById('batchDetailList');
    const toggleBtn = document.getElementById('toggleDetailBtn');
    
    if (detailList.classList.contains('collapsed')) {
        detailList.classList.remove('collapsed');
        toggleBtn.textContent = '收起 ▲';
    } else {
        detailList.classList.add('collapsed');
        toggleBtn.textContent = '展开 ▼';
    }
}

// 添加批量计算日志到指定地址
function appendBatchLogToAddress(address, message, type = '') {
    // 存储到该地址的日志数组
    if (!batchState.addressLogs[address]) {
        batchState.addressLogs[address] = [];
    }
    batchState.addressLogs[address].push({ message, type });
    
    // 检测快照校验失败的关键词
    if (!batchState.addressWarnings[address]) {
        batchState.addressWarnings[address] = {
            hasSnapshotWarning: false,
            warningCount: 0
        };
    }
    
    // 检测快照校验相关警告
    const snapshotKeywords = ['快照校验', '校验失败', '误差', '替换为快照', 'snapshot', 'verification'];
    const warningKeywords = ['⚠️', 'WARNING', '警告'];
    
    const lowerMessage = message.toLowerCase();
    const hasSnapshotKeyword = snapshotKeywords.some(kw => message.includes(kw) || lowerMessage.includes(kw.toLowerCase()));
    const hasWarningKeyword = warningKeywords.some(kw => message.includes(kw) || lowerMessage.includes(kw.toLowerCase()));
    
    if (hasSnapshotKeyword && hasWarningKeyword) {
        batchState.addressWarnings[address].hasSnapshotWarning = true;
        batchState.addressWarnings[address].warningCount++;
    }
    
    // 如果当前正在查看这个地址，实时更新显示
    if (batchState.currentViewingAddress === address) {
        const logContent = document.getElementById('batchLogContent');
        if (!logContent) return;
        
        const logLine = document.createElement('div');
        if (type === 'separator') {
            logLine.className = 'log-separator';
            logLine.innerHTML = `<span>${message}</span>`;
        } else {
            logLine.className = `log-line ${type}`;
            logLine.textContent = message;
        }
        logContent.appendChild(logLine);
        
        // 自动滚动到底部
        logContent.scrollTop = logContent.scrollHeight;
    }
}

// 切换到指定地址的日志显示
function switchToAddressLog(address) {
    batchState.currentViewingAddress = address;
    
    const logContent = document.getElementById('batchLogContent');
    if (!logContent) return;
    
    // 更新当前查看的地址显示
    const currentLogAddress = document.getElementById('currentLogAddress');
    if (currentLogAddress) {
        const index = batchState.addresses.indexOf(address);
        currentLogAddress.textContent = `当前查看: #${index + 1} ${address}`;
    }
    
    // 清空当前显示
    logContent.innerHTML = '';
    
    // 显示该地址的所有日志
    const logs = batchState.addressLogs[address] || [];
    logs.forEach(log => {
        const logLine = document.createElement('div');
        if (log.type === 'separator') {
            logLine.className = 'log-separator';
            logLine.innerHTML = `<span>${log.message}</span>`;
        } else {
            logLine.className = `log-line ${log.type}`;
            logLine.textContent = log.message;
        }
        logContent.appendChild(logLine);
    });
    
    // 滚动到底部
    logContent.scrollTop = logContent.scrollHeight;
    
    // 高亮当前选中的地址项
    document.querySelectorAll('.batch-detail-item').forEach(item => {
        item.classList.remove('selected');
    });
    const index = batchState.addresses.indexOf(address);
    if (index !== -1) {
        const item = document.getElementById(`batch-detail-${index}`);
        if (item) {
            item.classList.add('selected');
        }
    }
}

// 清空批量计算日志
function clearBatchLog() {
    const logContent = document.getElementById('batchLogContent');
    if (logContent) {
        logContent.innerHTML = '';
    }
    batchState.addressLogs = {};
    batchState.addressWarnings = {};
    batchState.currentViewingAddress = null;
}

// ==================== 数据表格相关函数 ====================

// 更新数据表格
function updateDataTable(data) {
    const tableBody = document.getElementById('dataTableBody');
    if (!tableBody) return;
    
    // 清空现有数据
    tableBody.innerHTML = '';
    
    // 遍历数据并添加行
    const dataLength = data.timestamps.length;
    for (let i = 0; i < dataLength; i++) {
        const row = document.createElement('tr');
        
        // 时间
        const timestamp = data.timestamps[i];
        const date = new Date(timestamp);
        const timeStr = date.toLocaleString('zh-CN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit'
        });
        
        // 构建行HTML
        row.innerHTML = `
            <td>${timeStr}</td>
            <td>${formatNumber(data.net_values[i], 4)}</td>
            <td class="${getValueClass(data.cumulative_pnl[i])}">${formatCurrency(data.cumulative_pnl[i])}</td>
            <td>${formatCurrency(data.total_assets[i])}</td>
            <td>${formatNumber(data.total_shares[i], 4)}</td>
            <td>${formatCurrency(data.spot_account_value[i])}</td>
            <td>${formatCurrency(data.perp_account_value[i])}</td>
            <td class="${getValueClass(data.realized_pnl[i])}">${formatCurrency(data.realized_pnl[i])}</td>
            <td class="${getValueClass(data.virtual_pnl[i])}">${formatCurrency(data.virtual_pnl[i])}</td>
        `;
        
        tableBody.appendChild(row);
    }
    
    console.log(`✅ 表格数据更新完成，共 ${dataLength} 行`);
}

// 格式化数字
function formatNumber(value, decimals = 2) {
    if (value === null || value === undefined) return '-';
    return parseFloat(value).toFixed(decimals);
}

// 格式化货币
function formatCurrency(value, decimals = 2) {
    if (value === null || value === undefined) return '-';
    const num = parseFloat(value);
    return '$' + num.toLocaleString('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

// 获取数值的颜色类
function getValueClass(value) {
    if (value === null || value === undefined) return 'value-neutral';
    const num = parseFloat(value);
    if (num > 0) return 'value-positive';
    if (num < 0) return 'value-negative';
    return 'value-neutral';
}

// 切换数据表格显示
function toggleDataTable() {
    const tableWrapper = document.getElementById('tableWrapper');
    const toggleBtn = document.getElementById('toggleTableBtn');
    
    if (!tableWrapper || !toggleBtn) return;
    
    if (tableWrapper.classList.contains('collapsed')) {
        tableWrapper.classList.remove('collapsed');
        toggleBtn.textContent = '收起 ▲';
    } else {
        tableWrapper.classList.add('collapsed');
        toggleBtn.textContent = '展开 ▼';
    }
}

// ==================== 过去持仓相关函数 ====================

let currentPositionsCsvFilename = '';

// 开始导出持仓CSV
async function startPositionsExport() {
    const address = document.getElementById('positionsAddress').value.trim();
    
    if (!address) {
        alert('请输入账户地址');
        return;
    }
    
    // 隐藏完成区域，显示日志
    document.getElementById('positionsComplete').style.display = 'none';
    document.getElementById('positionsLogContainer').style.display = 'block';
    clearPositionsLog();
    
    // 禁用按钮
    const btn = document.getElementById('exportPositionsBtn');
    btn.disabled = true;
    btn.textContent = '⏳ 正在导出...';
    
    try {
        const response = await fetch(`${API_BASE}/api/positions/export`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ address: address })
        });
        
        if (!response.ok) {
            throw new Error('请求失败');
        }
        
        // 使用SSE接收日志
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        
        while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            
            const chunk = decoder.decode(value);
            const lines = chunk.split('\n\n');
            
            for (const line of lines) {
                if (line.startsWith('data: ')) {
                    const dataStr = line.substring(6);
                    try {
                        const data = JSON.parse(dataStr);
                        
                        if (data.type === 'log') {
                            appendPositionsLog(data.message);
                        } else if (data.type === 'complete') {
                            if (data.success) {
                                currentPositionsCsvFilename = data.filename;
                                showPositionsComplete(address, data.filename);
                            } else {
                                appendPositionsLog('\n❌ 导出失败\n', 'error');
                            }
                        } else if (data.type === 'error') {
                            appendPositionsLog('\n❌ ' + data.message + '\n', 'error');
                        }
                    } catch (e) {
                        console.error('解析SSE数据失败:', e);
                    }
                }
            }
        }
    } catch (error) {
        appendPositionsLog('\n❌ 导出失败: ' + error.message + '\n', 'error');
    } finally {
        btn.disabled = false;
        btn.textContent = '📥 导出持仓CSV';
    }
}

// 显示导出完成
function showPositionsComplete(address, filename) {
    const completeDiv = document.getElementById('positionsComplete');
    const messageP = document.getElementById('positionsCompleteMessage');
    
    messageP.textContent = `持仓数据已成功导出：${filename}`;
    completeDiv.style.display = 'block';
    
    // 滚动到完成区域
    completeDiv.scrollIntoView({ behavior: 'smooth', block: 'center' });
}

// 下载CSV文件
async function downloadPositionsCsv() {
    const address = document.getElementById('positionsAddress').value.trim().toLowerCase();
    
    if (!address) {
        alert('请输入账户地址');
        return;
    }
    
    try {
        const response = await fetch(`${API_BASE}/api/positions/download/${address}`);
        
        if (!response.ok) {
            const result = await response.json();
            alert('下载失败: ' + (result.error || '未知错误'));
            return;
        }
        
        // 获取文件
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = currentPositionsCsvFilename || 'positions.csv';
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        
        console.log('✅ CSV文件下载成功');
    } catch (error) {
        alert('下载失败: ' + error.message);
    }
}

// 添加持仓日志
function appendPositionsLog(message, type = 'info') {
    const logContent = document.getElementById('positionsLogContent');
    if (!logContent) return;
    
    const logLine = document.createElement('div');
    logLine.className = `log-line log-${type}`;
    
    // 处理换行符
    const lines = message.split('\n');
    lines.forEach((line, index) => {
        if (line.trim()) {
            const lineSpan = document.createElement('div');
            lineSpan.textContent = line;
            logLine.appendChild(lineSpan);
        }
    });
    
    logContent.appendChild(logLine);
    
    // 自动滚动到底部
    logContent.scrollTop = logContent.scrollHeight;
}

// 清空持仓日志
function clearPositionsLog() {
    const logContent = document.getElementById('positionsLogContent');
    if (logContent) {
        logContent.innerHTML = '';
    }
}

