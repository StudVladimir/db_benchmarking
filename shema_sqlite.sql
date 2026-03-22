-- SQLite schema converted from schema_myslq_new.sql
-- Note: SQLite has no schemas; all tables are created in the same database file.

PRAGMA foreign_keys = ON;

-- -----------------------------------------------------
-- Table `Users`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "Users" (
  "idUser" INTEGER PRIMARY KEY AUTOINCREMENT,
  "Firstname" VARCHAR(45) NOT NULL,
  "Lastname" VARCHAR(45) NOT NULL,
  "Birthdate" DATE NOT NULL,
  "Email" VARCHAR(100) NOT NULL,
  "Phone" VARCHAR(20) NOT NULL,
  "Login" VARCHAR(100) NOT NULL,
  "Password_hash" VARCHAR(255) NOT NULL,
  "Status" VARCHAR(15) NOT NULL DEFAULT 'active'
);

CREATE UNIQUE INDEX IF NOT EXISTS "Login_UNIQUE" ON "Users" ("Login");

-- -----------------------------------------------------
-- Table `Sellers`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "Sellers" (
  "idSeller" INTEGER PRIMARY KEY AUTOINCREMENT,
  "Name" VARCHAR(45) NOT NULL,
  "Rating" TINYINT NOT NULL DEFAULT 0,
  "Status" VARCHAR(15) NOT NULL DEFAULT 'active',
  "Register_Date" DATE NOT NULL
);

-- -----------------------------------------------------
-- Table `Products`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "Products" (
  "idProduct" INTEGER PRIMARY KEY AUTOINCREMENT,
  "Title" VARCHAR(100) NOT NULL,
  "Seller_id" INT NOT NULL,
  "Description" TEXT NOT NULL,
  "Price" DECIMAL(7,2) NOT NULL,
  "Currency" CHAR(3) NOT NULL DEFAULT 'EUR',
  "Created_Date" DATETIME NOT NULL,
  "Quantity" INT NOT NULL,
  CONSTRAINT "fk_Products_Seller1"
    FOREIGN KEY ("Seller_id")
    REFERENCES "Sellers" ("idSeller")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX IF NOT EXISTS "fk_Products_Seller1_idx" ON "Products" ("Seller_id");

-- -----------------------------------------------------
-- Table `Categories`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "Categories" (
  "idCategory" INTEGER PRIMARY KEY AUTOINCREMENT,
  "Name" VARCHAR(45) NOT NULL
);

-- -----------------------------------------------------
-- Table `Addresses`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "Addresses" (
  "idAddresses" INTEGER PRIMARY KEY AUTOINCREMENT,
  "Country" VARCHAR(45) NOT NULL,
  "City" VARCHAR(45) NOT NULL,
  "Street" VARCHAR(45) NOT NULL,
  "Appartment" VARCHAR(10) NOT NULL
);

-- -----------------------------------------------------
-- Table `Orders`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "Orders" (
  "idOrder" INTEGER PRIMARY KEY AUTOINCREMENT,
  "Users_id" INT NOT NULL,
  "Status" VARCHAR(15) NOT NULL DEFAULT 'created',
  "Create_Date" DATETIME NOT NULL,
  "Paid_Date" DATETIME NULL,
  "Price_Total" DECIMAL(7,2) NOT NULL,
  "Currency" CHAR(3) NOT NULL DEFAULT 'EUR',
  "Address_id" INT NOT NULL,
  CONSTRAINT "fk_Orders_Users1"
    FOREIGN KEY ("Users_id")
    REFERENCES "Users" ("idUser")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT "fk_Orders_Addresses1"
    FOREIGN KEY ("Address_id")
    REFERENCES "Addresses" ("idAddresses")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX IF NOT EXISTS "fk_Orders_Users1_idx" ON "Orders" ("Users_id");
CREATE INDEX IF NOT EXISTS "fk_Orders_Addresses1_idx" ON "Orders" ("Address_id");

-- -----------------------------------------------------
-- Table `Product_Categories`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "Product_Categories" (
  "idProduct_Categories" INTEGER PRIMARY KEY AUTOINCREMENT,
  "Product_id" INT NOT NULL,
  "Category_id" INT NOT NULL,
  CONSTRAINT "fk_Product_Categories_Products1"
    FOREIGN KEY ("Product_id")
    REFERENCES "Products" ("idProduct")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT "fk_Product_Categories_Categories1"
    FOREIGN KEY ("Category_id")
    REFERENCES "Categories" ("idCategory")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX IF NOT EXISTS "fk_Product_Categories_Products1_idx"
  ON "Product_Categories" ("Product_id");

CREATE INDEX IF NOT EXISTS "fk_Product_Categories_Categories1_idx"
  ON "Product_Categories" ("Category_id");

-- -----------------------------------------------------
-- Table `Order_Items`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "Order_Items" (
  "idOrder_Items" INTEGER PRIMARY KEY AUTOINCREMENT,
  "Order_id" INT NOT NULL,
  "Product_id" INT NOT NULL,
  "Quantity" INT NOT NULL,
  CONSTRAINT "fk_Order_Items_Orders1"
    FOREIGN KEY ("Order_id")
    REFERENCES "Orders" ("idOrder")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT "fk_Order_Items_Products1"
    FOREIGN KEY ("Product_id")
    REFERENCES "Products" ("idProduct")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX IF NOT EXISTS "fk_Order_Items_Orders1_idx"
  ON "Order_Items" ("Order_id");

CREATE INDEX IF NOT EXISTS "fk_Order_Items_Products1_idx"
  ON "Order_Items" ("Product_id");

-- -----------------------------------------------------
-- Table `User_Addresses`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS "User_Addresses" (
  "idUser_Addresses" INTEGER PRIMARY KEY AUTOINCREMENT,
  "User_id" INT NOT NULL,
  "Address_id" INT NOT NULL,
  CONSTRAINT "fk_User_Addresses_Users1"
    FOREIGN KEY ("User_id")
    REFERENCES "Users" ("idUser")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT "fk_User_Addresses_Addresses1"
    FOREIGN KEY ("Address_id")
    REFERENCES "Addresses" ("idAddresses")
    ON DELETE NO ACTION
    ON UPDATE NO ACTION
);

CREATE INDEX IF NOT EXISTS "fk_User_Addresses_Users1_idx"
  ON "User_Addresses" ("User_id");

CREATE INDEX IF NOT EXISTS "fk_User_Addresses_Addresses1_idx"
  ON "User_Addresses" ("Address_id");