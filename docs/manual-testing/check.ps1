# Проверка исходных восьми строк; ошибочные INSERT не должны изменять таблицу.
[CmdletBinding()]
param([string]$BaseUrl = 'http://localhost:8123')

$ErrorActionPreference = 'Stop'
$BaseUrl = $BaseUrl.TrimEnd('/')
$script:Passed = 0

function Request {
    param([string]$Path, [string]$Method = 'GET', [byte[]]$Data = $null,
        [string]$ContentType = 'text/plain; charset=utf-8')
    $options = @{ Uri = "$BaseUrl$Path"; Method = $Method; UseBasicParsing = $true; TimeoutSec = 30 }
    if ($null -ne $Data) { $options.Body = $Data; $options.ContentType = $ContentType }
    try {
        $response = Invoke-WebRequest @options
        $body = $response.Content
        if ($body -is [byte[]]) { $body = [System.Text.Encoding]::UTF8.GetString($body) }
        return @{ Status = [int]$response.StatusCode; Body = [string]$body }
    } catch {
        $response = $_.Exception.Response
        if ($null -eq $response) { throw }
        $body = $_.ErrorDetails.Message
        if ([string]::IsNullOrEmpty($body)) {
            if ($response.PSObject.Methods.Name -contains 'GetResponseStream') {
                $reader = New-Object System.IO.StreamReader($response.GetResponseStream())
                try { $body = $reader.ReadToEnd() } finally { $reader.Dispose() }
            } else { $body = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult() }
        }
        return @{ Status = [int]$response.StatusCode; Body = [string]$body }
    }
}

function Query {
    param([string]$Sql, [string]$Format = 'TSV')
    Request -Path "/query?format=$Format" -Method POST -Data ([System.Text.Encoding]::UTF8.GetBytes($Sql))
}

function Assert {
    param([bool]$Condition, [string]$Name, [string]$Details = '')
    if (-not $Condition) { throw "FAIL: $Name`n$Details" }
    $script:Passed++
    Write-Host "OK: $Name" -ForegroundColor Green
}

function ExpectText {
    param([hashtable]$Response, [string]$Expected, [string]$Name)
    $actual = $Response.Body.Replace("`r`n", "`n").TrimEnd([char[]]"`r`n")
    Assert ($Response.Status -eq 200 -and $actual -ceq $Expected) $Name "HTTP $($Response.Status): $($Response.Body)"
}

ExpectText (Request '/ping') 'Ok.' 'GET /ping'
ExpectText (Query 'SELECT count() FROM manual_events') '8' 'Исходные восемь строк'
ExpectText (Query 'SELECT id, category, amount FROM manual_events ORDER BY id') "1`tbooks`t100`n2`tgames`t200`n3`tbooks`t-50`n4`ttools`t300`n5`tgames`t150`n6`tbooks`t75`n7`ttools`t25`n8`tgames`t0" 'Сортировка всех строк'
ExpectText (Query 'SELECT count() AS n, sum(amount) AS total, min(amount) AS lo, max(amount) AS hi, sum(ratio) AS ratio_total FROM manual_events') "8`t800`t-50`t300`t9" 'Все агрегаты'
ExpectText (Query 'SELECT category, count() AS n, sum(amount) AS total FROM manual_events GROUP BY category ORDER BY total DESC, category ASC') "games`t3`t350`ntools`t2`t325`nbooks`t3`t125" 'GROUP BY и сортировка по алиасу'
ExpectText (Query "SELECT category, count() AS n, sum(amount) AS total FROM manual_events WHERE event_date = '2026-10-01' GROUP BY category ORDER BY total DESC, category ASC LIMIT 2") "tools`t1`t300`ngames`t1`t200" 'WHERE, GROUP BY, ORDER BY, LIMIT'
ExpectText (Query 'SELECT id, category, amount FROM manual_events ORDER BY amount DESC, id ASC LIMIT 3') "4`ttools`t300`n2`tgames`t200`n5`tgames`t150" 'Первые три по сумме'
ExpectText (Query 'SELECT id FROM manual_events ORDER BY amount DESC, id ASC LIMIT 3') "4`n2`n5" 'Скрытая колонка сортировки'

$response = Query 'SELECT count() AS n, sum(amount) AS total FROM manual_events' 'JSONEachRow'
$json = $response.Body | ConvertFrom-Json
Assert ($response.Status -eq 200 -and $json.n -eq 8 -and $json.total -eq 800 -and $json.n -isnot [string]) 'JSON сохраняет числовые типы' $response.Body
$response = Query 'SELECT category, count() AS n FROM manual_events GROUP BY category ORDER BY n DESC, category ASC LIMIT 1' 'CSV'
ExpectText $response 'books,3' 'Вывод CSV без заголовка'
$response = Query 'SELECT sum(amount) AS total FROM manual_events' 'Pretty'
Assert ($response.Status -eq 200 -and $response.Body.Contains('| total |') -and $response.Body.Contains('| 800   |')) 'Вывод Pretty' $response.Body

$response = Query "EXPLAIN SELECT id, category FROM manual_events WHERE event_date = '2026-10-01'"
Assert ($response.Status -eq 200 -and $response.Body.Contains('granules: 2 / 4, rows: 4 / 8')) 'EXPLAIN по ключу: 2 из 4 гранул' $response.Body
$response = Query "EXPLAIN SELECT id, category FROM manual_events WHERE category = 'books'"
Assert ($response.Status -eq 200 -and $response.Body.Contains('granules: 4 / 4, rows: 8 / 8')) 'EXPLAIN неключевого условия: полный скан' $response.Body

foreach ($case in @(
    @{ Sql = 'SELECT missing_column FROM manual_events'; Status = 400; Name = 'Неизвестная колонка' },
    @{ Sql = 'SELECT * FROM table_that_does_not_exist'; Status = 404; Name = 'Неизвестная таблица' },
    @{ Sql = 'SELECT sum(category) FROM manual_events'; Status = 400; Name = 'Ошибка типа агрегата' },
    @{ Sql = 'SELECT id FROM manual_events ORDER BY id + 1'; Status = 400; Name = 'Неподдерживаемый ORDER BY' },
    @{ Sql = 'SELECT DISTINCT category FROM manual_events'; Status = 400; Name = 'Неподдерживаемый DISTINCT' }
)) {
    $response = Query $case.Sql
    $errorBody = $response.Body | ConvertFrom-Json
    Assert ($response.Status -eq $case.Status -and $null -ne $errorBody.error) $case.Name $response.Body
}

foreach ($case in @(
    @{ File = 'bad-input.csv'; Format = 'CSV'; Message = 'row 2' },
    @{ File = 'bad-duplicate.jsonl'; Format = 'JSONEachRow'; Message = 'duplicate' },
    @{ File = 'bad-null.jsonl'; Format = 'JSONEachRow'; Message = 'NULL' }
)) {
    $response = Request -Path "/insert/manual_events?format=$($case.Format)" -Method POST -ContentType 'application/octet-stream' -Data ([System.IO.File]::ReadAllBytes((Join-Path $PSScriptRoot $case.File)))
    $errorBody = $response.Body | ConvertFrom-Json
    Assert ($response.Status -eq 400 -and $errorBody.error.Contains($case.Message)) "Ошибка файла $($case.File)" $response.Body
}
ExpectText (Query 'SELECT count() FROM manual_events') '8' 'Ошибочные INSERT атомарны'
ExpectText (Query 'SELECT id FROM manual_events WHERE id = 99') '' 'Корректная первая строка ошибочного файла не вставлена'
Write-Host "Готово: $script:Passed проверок пройдено. Таблица не изменена." -ForegroundColor Cyan
