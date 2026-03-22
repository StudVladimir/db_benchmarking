param(
    [string]$CsvFolder = "data_csv(third)",
    [string]$MySqlContainer = "dbb-mysql",
    [string]$PostgresContainer = "dbb-postgres",
    [string]$SqliteContainer = "dbb-sqlite",
    [string]$SqliteDbPath = "/data/benchmark.db",
    [bool]$ResetData = $true,
    [switch]$SkipMySql,
    [switch]$SkipPostgres,
    [switch]$SkipSqlite,
    [switch]$OnlySqlite
)

$ErrorActionPreference = "Stop"

function Invoke-Step {
    param(
        [string]$Message,
        [scriptblock]$Action
    )

    Write-Host "`n=== $Message ===" -ForegroundColor Cyan
    & $Action
}

function Assert-ExitCode {
    param([string]$Step)

    if ($LASTEXITCODE -ne 0) {
        throw "Step failed: $Step (exit code $LASTEXITCODE)"
    }
}

$requiredFiles = @(
    "Users.csv",
    "Sellers.csv",
    "Categories.csv",
    "Addresses.csv",
    "Products.csv",
    "Orders.csv",
    "Product_Categories.csv",
    "User_Addresses.csv",
    "Order_Items.csv"
)

if (-not (Test-Path -LiteralPath $CsvFolder)) {
    throw "CSV folder not found: $CsvFolder"
}

$missing = $requiredFiles | Where-Object { -not (Test-Path -LiteralPath (Join-Path $CsvFolder $_)) }
if ($missing.Count -gt 0) {
    throw "Missing CSV files: $($missing -join ', ')"
}

if ($OnlySqlite) {
    $SkipMySql = $true
    $SkipPostgres = $true
    $SkipSqlite = $false
}

if (-not $SkipMySql) {
    Invoke-Step "Copy CSV files to MySQL container" {
        docker exec -i $MySqlContainer sh -c "mkdir -p /tmp/csv"
        Assert-ExitCode "create /tmp/csv in MySQL container"

        docker cp "$CsvFolder/." "${MySqlContainer}:/tmp/csv/"
        Assert-ExitCode "copy CSV to MySQL container"
    }

    Invoke-Step "Enable local_infile in MySQL" {
        docker exec -i $MySqlContainer mysql -uroot -proot -e "SET GLOBAL local_infile = 1;"
        Assert-ExitCode "enable MySQL local_infile"
    }

    if ($ResetData) {
        Invoke-Step "Reset MySQL data" {
            $resetMySql = @'
SET FOREIGN_KEY_CHECKS=0;
TRUNCATE TABLE Order_Items;
TRUNCATE TABLE User_Addresses;
TRUNCATE TABLE Product_Categories;
TRUNCATE TABLE Orders;
TRUNCATE TABLE Products;
TRUNCATE TABLE Addresses;
TRUNCATE TABLE Categories;
TRUNCATE TABLE Sellers;
TRUNCATE TABLE Users;
SET FOREIGN_KEY_CHECKS=1;
'@
            $resetMySql | docker exec -i $MySqlContainer mysql -uroot -proot mydb
            Assert-ExitCode "truncate MySQL tables"
        }
    }

    Invoke-Step "Import CSV into MySQL" {
        $mysqlImport = @'
SET FOREIGN_KEY_CHECKS=0;
LOAD DATA LOCAL INFILE '/tmp/csv/Users.csv' INTO TABLE Users FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (idUser,Firstname,Lastname,Birthdate,Email,Phone,Login,Password_hash,Status);
LOAD DATA LOCAL INFILE '/tmp/csv/Sellers.csv' INTO TABLE Sellers FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (idSeller,Name,Rating,Status,Register_Date);
LOAD DATA LOCAL INFILE '/tmp/csv/Categories.csv' INTO TABLE Categories FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (idCategory,Name);
LOAD DATA LOCAL INFILE '/tmp/csv/Addresses.csv' INTO TABLE Addresses FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (idAddresses,Country,City,Street,Appartment);
LOAD DATA LOCAL INFILE '/tmp/csv/Products.csv' INTO TABLE Products FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (idProduct,Title,Seller_id,Description,Price,Currency,Created_Date,Quantity);
LOAD DATA LOCAL INFILE '/tmp/csv/Orders.csv' INTO TABLE Orders FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (idOrder,Users_id,Status,Create_Date,Paid_Date,Price_Total,Currency,Address_id);
LOAD DATA LOCAL INFILE '/tmp/csv/Product_Categories.csv' INTO TABLE Product_Categories FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (@idProduct_Category,Product_id,Category_id);
LOAD DATA LOCAL INFILE '/tmp/csv/User_Addresses.csv' INTO TABLE User_Addresses FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (idUser_Addresses,User_id,Address_id);
LOAD DATA LOCAL INFILE '/tmp/csv/Order_Items.csv' INTO TABLE Order_Items FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS (idOrder_Items,Order_id,Product_id,Quantity);
SET FOREIGN_KEY_CHECKS=1;
'@
        $mysqlImport | docker exec -i $MySqlContainer mysql --local-infile=1 -uroot -proot mydb
        Assert-ExitCode "import MySQL CSV"
    }

    Invoke-Step "Validate MySQL row counts" {
        $mysqlCheck = @'
SELECT 'Users' AS t, COUNT(*) AS c FROM Users
UNION ALL SELECT 'Sellers', COUNT(*) FROM Sellers
UNION ALL SELECT 'Categories', COUNT(*) FROM Categories
UNION ALL SELECT 'Addresses', COUNT(*) FROM Addresses
UNION ALL SELECT 'Products', COUNT(*) FROM Products
UNION ALL SELECT 'Orders', COUNT(*) FROM Orders
UNION ALL SELECT 'Product_Categories', COUNT(*) FROM Product_Categories
UNION ALL SELECT 'User_Addresses', COUNT(*) FROM User_Addresses
UNION ALL SELECT 'Order_Items', COUNT(*) FROM Order_Items;
'@
        $mysqlCheck | docker exec -i $MySqlContainer mysql -uroot -proot mydb
        Assert-ExitCode "validate MySQL counts"
    }
}

if (-not $SkipPostgres) {
    Invoke-Step "Copy CSV files to PostgreSQL container" {
        docker exec -i $PostgresContainer sh -c "mkdir -p /tmp/csv"
        Assert-ExitCode "create /tmp/csv in PostgreSQL container"

        docker cp "$CsvFolder/." "${PostgresContainer}:/tmp/csv/"
        Assert-ExitCode "copy CSV to PostgreSQL container"
    }

    if ($ResetData) {
        Invoke-Step "Reset PostgreSQL data" {
            $pgReset = @'
TRUNCATE TABLE mydb."Order_Items", mydb."User_Addresses", mydb."Product_Categories", mydb."Orders", mydb."Products", mydb."Addresses", mydb."Categories", mydb."Sellers", mydb."Users" RESTART IDENTITY CASCADE;
'@
            $pgReset | docker exec -i $PostgresContainer psql -v ON_ERROR_STOP=1 -U benchmark -d benchmark
            Assert-ExitCode "truncate PostgreSQL tables"
        }
    }

    Invoke-Step "Import CSV into PostgreSQL" {
        $pgImport = @'
SET search_path TO mydb;
\copy "Users" ("idUser","Firstname","Lastname","Birthdate","Email","Phone","Login","Password_hash","Status") FROM '/tmp/csv/Users.csv' CSV HEADER;
\copy "Sellers" ("idSeller","Name","Rating","Status","Register_Date") FROM '/tmp/csv/Sellers.csv' CSV HEADER;
\copy "Categories" ("idCategory","Name") FROM '/tmp/csv/Categories.csv' CSV HEADER;
\copy "Addresses" ("idAddresses","Country","City","Street","Appartment") FROM '/tmp/csv/Addresses.csv' CSV HEADER;
\copy "Products" ("idProduct","Title","Seller_id","Description","Price","Currency","Created_Date","Quantity") FROM '/tmp/csv/Products.csv' CSV HEADER;
\copy "Orders" ("idOrder","Users_id","Status","Create_Date","Paid_Date","Price_Total","Currency","Address_id") FROM '/tmp/csv/Orders.csv' CSV HEADER;
\copy "Product_Categories" ("idProduct_Categories","Product_id","Category_id") FROM '/tmp/csv/Product_Categories.csv' CSV HEADER;
\copy "User_Addresses" ("idUser_Addresses","User_id","Address_id") FROM '/tmp/csv/User_Addresses.csv' CSV HEADER;
\copy "Order_Items" ("idOrder_Items","Order_id","Product_id","Quantity") FROM '/tmp/csv/Order_Items.csv' CSV HEADER;
'@
        $pgImport | docker exec -i $PostgresContainer psql -v ON_ERROR_STOP=1 -U benchmark -d benchmark
        Assert-ExitCode "import PostgreSQL CSV"
    }

    Invoke-Step "Fix PostgreSQL identity sequences" {
        $pgSeq = @'
SELECT setval(pg_get_serial_sequence('mydb."Users"','idUser'), COALESCE((SELECT MAX("idUser") FROM mydb."Users"),1), true);
SELECT setval(pg_get_serial_sequence('mydb."Sellers"','idSeller'), COALESCE((SELECT MAX("idSeller") FROM mydb."Sellers"),1), true);
SELECT setval(pg_get_serial_sequence('mydb."Products"','idProduct'), COALESCE((SELECT MAX("idProduct") FROM mydb."Products"),1), true);
SELECT setval(pg_get_serial_sequence('mydb."Categories"','idCategory'), COALESCE((SELECT MAX("idCategory") FROM mydb."Categories"),1), true);
SELECT setval(pg_get_serial_sequence('mydb."Addresses"','idAddresses'), COALESCE((SELECT MAX("idAddresses") FROM mydb."Addresses"),1), true);
SELECT setval(pg_get_serial_sequence('mydb."Orders"','idOrder'), COALESCE((SELECT MAX("idOrder") FROM mydb."Orders"),1), true);
SELECT setval(pg_get_serial_sequence('mydb."Product_Categories"','idProduct_Categories'), COALESCE((SELECT MAX("idProduct_Categories") FROM mydb."Product_Categories"),1), true);
SELECT setval(pg_get_serial_sequence('mydb."Order_Items"','idOrder_Items'), COALESCE((SELECT MAX("idOrder_Items") FROM mydb."Order_Items"),1), true);
SELECT setval(pg_get_serial_sequence('mydb."User_Addresses"','idUser_Addresses'), COALESCE((SELECT MAX("idUser_Addresses") FROM mydb."User_Addresses"),1), true);
'@
        $pgSeq | docker exec -i $PostgresContainer psql -v ON_ERROR_STOP=1 -U benchmark -d benchmark
        Assert-ExitCode "fix PostgreSQL sequences"
    }

    Invoke-Step "Validate PostgreSQL row counts" {
        $pgCheck = @'
SELECT 'Users' AS t, COUNT(*) AS c FROM mydb."Users"
UNION ALL SELECT 'Sellers', COUNT(*) FROM mydb."Sellers"
UNION ALL SELECT 'Categories', COUNT(*) FROM mydb."Categories"
UNION ALL SELECT 'Addresses', COUNT(*) FROM mydb."Addresses"
UNION ALL SELECT 'Products', COUNT(*) FROM mydb."Products"
UNION ALL SELECT 'Orders', COUNT(*) FROM mydb."Orders"
UNION ALL SELECT 'Product_Categories', COUNT(*) FROM mydb."Product_Categories"
UNION ALL SELECT 'User_Addresses', COUNT(*) FROM mydb."User_Addresses"
UNION ALL SELECT 'Order_Items', COUNT(*) FROM mydb."Order_Items";
'@
        $pgCheck | docker exec -i $PostgresContainer psql -v ON_ERROR_STOP=1 -U benchmark -d benchmark
        Assert-ExitCode "validate PostgreSQL counts"
    }
}

if (-not $SkipSqlite) {
    Invoke-Step "Copy CSV files to SQLite container" {
        docker exec -i $SqliteContainer sh -c "mkdir -p /tmp/csv"
        Assert-ExitCode "create /tmp/csv in SQLite container"

        docker cp "$CsvFolder/." "${SqliteContainer}:/tmp/csv/"
        Assert-ExitCode "copy CSV to SQLite container"
    }

    if ($ResetData) {
        Invoke-Step "Reset SQLite data" {
            $sqliteReset = @'
PRAGMA foreign_keys = OFF;
DELETE FROM "Order_Items";
DELETE FROM "User_Addresses";
DELETE FROM "Product_Categories";
DELETE FROM "Orders";
DELETE FROM "Products";
DELETE FROM "Addresses";
DELETE FROM "Categories";
DELETE FROM "Sellers";
DELETE FROM "Users";
DELETE FROM sqlite_sequence WHERE name IN (
  'Users',
  'Sellers',
  'Products',
  'Categories',
  'Addresses',
  'Orders',
  'Product_Categories',
  'Order_Items',
  'User_Addresses'
);
PRAGMA foreign_keys = ON;
'@
            $sqliteReset | docker exec -i $SqliteContainer sqlite3 $SqliteDbPath
            Assert-ExitCode "reset SQLite tables"
        }
    }

    Invoke-Step "Import CSV into SQLite" {
        $sqliteImport = @'
PRAGMA foreign_keys = OFF;
.mode csv
.import --skip 1 /tmp/csv/Users.csv Users
.import --skip 1 /tmp/csv/Sellers.csv Sellers
.import --skip 1 /tmp/csv/Categories.csv Categories
.import --skip 1 /tmp/csv/Addresses.csv Addresses
.import --skip 1 /tmp/csv/Products.csv Products
.import --skip 1 /tmp/csv/Orders.csv Orders
.import --skip 1 /tmp/csv/Product_Categories.csv Product_Categories
.import --skip 1 /tmp/csv/User_Addresses.csv User_Addresses
.import --skip 1 /tmp/csv/Order_Items.csv Order_Items
PRAGMA foreign_keys = ON;
PRAGMA foreign_key_check;
'@
        $sqliteImport | docker exec -i $SqliteContainer sqlite3 $SqliteDbPath
        Assert-ExitCode "import SQLite CSV"
    }

    Invoke-Step "Validate SQLite row counts" {
        $sqliteCheck = @'
SELECT 'Users' AS t, COUNT(*) AS c FROM "Users"
UNION ALL SELECT 'Sellers', COUNT(*) FROM "Sellers"
UNION ALL SELECT 'Categories', COUNT(*) FROM "Categories"
UNION ALL SELECT 'Addresses', COUNT(*) FROM "Addresses"
UNION ALL SELECT 'Products', COUNT(*) FROM "Products"
UNION ALL SELECT 'Orders', COUNT(*) FROM "Orders"
UNION ALL SELECT 'Product_Categories', COUNT(*) FROM "Product_Categories"
UNION ALL SELECT 'User_Addresses', COUNT(*) FROM "User_Addresses"
UNION ALL SELECT 'Order_Items', COUNT(*) FROM "Order_Items";
'@
        $sqliteCheck | docker exec -i $SqliteContainer sqlite3 -header -column $SqliteDbPath
        Assert-ExitCode "validate SQLite counts"
    }
}

Write-Host "`nImport completed successfully." -ForegroundColor Green
