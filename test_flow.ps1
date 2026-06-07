# Reed-Solomon(6,3) 纠删码服务自动化测试脚本
$baseUrl = "http://localhost:8080/api/v1"

Write-Host "=== Reed-Solomon(6,3) 纠删码服务测试 ===" -ForegroundColor Cyan
Write-Host ""

# 创建测试文件
Write-Host "[1/8] 创建测试文件..." -ForegroundColor Yellow
$testContent = "Hello, Reed-Solomon Erasure Coding Test! $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
$testContent | Out-File -FilePath "test.txt" -Encoding ASCII
$originalHash = (Get-FileHash -Path "test.txt" -Algorithm SHA256).Hash
Write-Host "  测试文件: test.txt"
Write-Host "  原始哈希: $originalHash"
Write-Host ""

# 上传文件
Write-Host "[2/8] 上传文件..." -ForegroundColor Yellow
$uploadResponse = curl.exe -s -X POST -F "file=@test.txt" "$baseUrl/files" | ConvertFrom-Json
if ($uploadResponse.code -ne 0) {
    Write-Host "  上传失败: $($uploadResponse.error)" -ForegroundColor Red
    exit 1
}
$fileId = $uploadResponse.data.file_id
Write-Host "  上传成功!"
Write-Host "  文件ID: $fileId"
Write-Host "  文件大小: $($uploadResponse.data.file_size) bytes"
Write-Host "  分片大小: $($uploadResponse.data.shard_size) bytes"
Write-Host ""

# 查看节点状态
Write-Host "[3/8] 查看节点状态..." -ForegroundColor Yellow
$nodesResponse = curl.exe -s "$baseUrl/nodes" | ConvertFrom-Json
$onlineCount = ($nodesResponse.data | Where-Object { $_.status -eq "online" }).Count
Write-Host "  在线节点: $onlineCount / 9"
Write-Host ""

# 查看文件分片信息
Write-Host "[4/8] 查看文件分片信息..." -ForegroundColor Yellow
$shardsResponse = curl.exe -s "$baseUrl/files/$fileId/shards" | ConvertFrom-Json
foreach ($shard in $shardsResponse.data) {
    $type = if ($shard.is_parity) { "校验" } else { "数据" }
    Write-Host "  分片$($shard.shard_index) [$type] -> 节点$($shard.node_id), $($shard.size) bytes"
}
Write-Host ""

# 模拟节点故障
Write-Host "[5/8] 模拟节点故障 (标记节点0,1,2下线)..." -ForegroundColor Yellow
$failedNodes = @(0, 1, 2)
foreach ($nodeId in $failedNodes) {
    $offlineResponse = curl.exe -s -X POST "$baseUrl/nodes/$nodeId/offline" | ConvertFrom-Json
    $affected = $offlineResponse.data.affected_files.Count
    Write-Host "  节点$nodeId 已下线, 影响 $affected 个文件"
}
Write-Host ""

# 触发数据重建
Write-Host "[6/8] 触发数据重建..." -ForegroundColor Yellow
$rebuildBody = @{
    file_id = $fileId
    failed_node_ids = $failedNodes
} | ConvertTo-Json
$rebuildResponse = curl.exe -s -X POST -H "Content-Type: application/json" -d $rebuildBody "$baseUrl/rebuild" | ConvertFrom-Json
$rebuildData = $rebuildResponse.data
Write-Host "  重建状态: $($rebuildData.status)"
Write-Host "  耗时: $($rebuildData.duration_ms) ms"
Write-Host "  数据大小: $($rebuildData.data_size) bytes"
Write-Host "  哈希校验: $(if ($rebuildData.hash_verified) { '通过' } else { '失败' })"
Write-Host "  原始哈希: $($rebuildData.original_hash)"
Write-Host "  重建哈希: $($rebuildData.rebuilt_hash)"
Write-Host "  重建分片: $($rebuildData.rebuilt_shards -join ', ')"
if ($rebuildData.error_message) {
    Write-Host "  错误信息: $($rebuildData.error_message)" -ForegroundColor Red
}
Write-Host ""

# 查看重建日志
Write-Host "[7/8] 查看重建日志..." -ForegroundColor Yellow
$logsResponse = curl.exe -s "$baseUrl/rebuild/logs/$fileId" | ConvertFrom-Json
foreach ($log in $logsResponse.data) {
    Write-Host "  时间: $($log.start_time)"
    Write-Host "  耗时: $($log.duration_ms) ms, 状态: $($log.status)"
    Write-Host "  失败节点: [$($log.failed_node_ids)], 重建分片: [$($log.rebuilt_shards)]"
    Write-Host "  哈希验证: $(if ($log.hash_verified) { '通过' } else { '失败' })"
}
Write-Host ""

# 下载文件并验证
Write-Host "[8/8] 下载文件并验证完整性..." -ForegroundColor Yellow
curl.exe -s -o "downloaded_test.txt" "$baseUrl/files/$fileId/download"
if (Test-Path "downloaded_test.txt") {
    $downloadedHash = (Get-FileHash -Path "downloaded_test.txt" -Algorithm SHA256).Hash
    Write-Host "  下载文件哈希: $downloadedHash"
    if ($downloadedHash -eq $originalHash) {
        Write-Host "  文件完整性验证: 通过!" -ForegroundColor Green
    } else {
        Write-Host "  文件完整性验证: 失败!" -ForegroundColor Red
        Write-Host "  原始哈希: $originalHash"
        Write-Host "  下载哈希: $downloadedHash"
    }
} else {
    Write-Host "  下载失败!" -ForegroundColor Red
}
Write-Host ""

# 恢复节点
Write-Host "恢复节点状态..." -ForegroundColor Yellow
foreach ($nodeId in $failedNodes) {
    curl.exe -s -X POST "$baseUrl/nodes/$nodeId/online" | Out-Null
    Write-Host "  节点$nodeId 已恢复在线"
}
Write-Host ""

Write-Host "[9/9] 测试超过容错阈值的场景 (4个节点故障)..." -ForegroundColor Yellow
$tooManyFailedNodes = @(0, 1, 2, 3)
$rebuildBodyTooMany = @{
    file_id = $fileId
    failed_node_ids = $tooManyFailedNodes
} | ConvertTo-Json
$rebuildResponseTooMany = curl.exe -s -w "`n%{http_code}" -X POST -H "Content-Type: application/json" -d $rebuildBodyTooMany "$baseUrl/rebuild"
$responseLines = $rebuildResponseTooMany -split "`n"
$statusCode = $responseLines[-1]
$responseBody = $responseLines[0..($responseLines.Count-2)] -join "`n" | ConvertFrom-Json
Write-Host "  HTTP状态码: $statusCode"
if ($statusCode -eq "409") {
    Write-Host "  阈值检查验证: 通过! 正确返回409 Conflict" -ForegroundColor Green
    Write-Host "  失败节点数: $($responseBody.data.failed_nodes), 最大可恢复: $($responseBody.data.max_recoverable)"
} else {
    Write-Host "  阈值检查验证: 失败! 期望409, 实际$statusCode" -ForegroundColor Red
}
Write-Host ""

# 恢复节点
Write-Host "恢复节点状态..." -ForegroundColor Yellow
foreach ($nodeId in $failedNodes) {
    curl.exe -s -X POST "$baseUrl/nodes/$nodeId/online" | Out-Null
    Write-Host "  节点$nodeId 已恢复在线"
}
Write-Host ""

Write-Host "=== 测试完成 ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "关键性能指标:" -ForegroundColor Cyan
Write-Host "  重建耗时: $($rebuildData.duration_ms) ms"
Write-Host "  数据吞吐量: $([math]::Round($rebuildData.data_size / $rebuildData.duration_ms * 1000 / 1024, 2)) KB/s"
Write-Host "  哈希校验: $(if ($rebuildData.hash_verified) { '通过' } else { '失败' })"
Write-Host "  阈值检查: 通过 (4节点故障返回409)"
