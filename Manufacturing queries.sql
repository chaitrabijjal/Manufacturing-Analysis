create database project;
#import table manufacturing_dataset from excel
show tables;
desc `manufacturing_dataset`;
RENAME TABLE `manufacturing dataset` TO manufacturing_dataset;
select * from manufacturing_dataset;
# group by (employee wise rejected qty)
SELECT 
    `EMP Code`,
    `Emp Name`,
    SUM(`Rejected Qty`) AS employee_rejected_qty
FROM manufacturing_dataset
GROUP BY `EMP Code`, `Emp Name`
ORDER BY `employee_rejected_qty` DESC;

# MACHINE WISE REJECTED QTY
SELECT 
    `Machine code`,
    SUM(`Rejected Qty`) AS machine_rejected_qty
FROM manufacturing_dataset
GROUP BY `Machine code`
ORDER BY `machine_rejected_qty` DESC;

# DEPARTMENT WISE MANUFACTURING VS REJECTED QTY
SELECT 
    `Department Name`,
    SUM(`Produced Qty`) AS total_manufactured,
    SUM(`Rejected Qty`) AS total_rejected,
    ROUND( (SUM(`Rejected Qty`) / SUM(`Produced Qty`)) * 100 ,2) AS rejection_percent
FROM manufacturing_dataset
GROUP BY `Department Name`;

# Index on EMP Code
SELECT `EMP Code`, SUM(`Rejected Qty`)
FROM manufacturing_dataset
GROUP BY `EMP Code`;

# Composite Index (EMP Code + Doc Date)
SELECT *
FROM manufacturing_dataset
WHERE `EMP Code` = 'E127'
AND `doc_date_clean` BETWEEN '2024-01-01' AND '2024-01-31';


# Index on Doc Date
/*CREATE INDEX idx_manufacturing_dataset_doc_date
ON manufacturing_dataset (`doc_date_clean`);*/

# Create index on Cust Code
/*CREATE INDEX idx_manufacturing_dataset_CustCode
ON manufacturing_dataset (`Cust Code`);*/

SHOW INDEXES FROM manufacturing_dataset;

SHOW COLUMNS FROM manufacturing_dataset;

# Add a tempory date column
/*ALTER TABLE manufacturing_dataset 
ADD COLUMN doc_date_clean DATE;*/

SET SQL_SAFE_UPDATES = 0;

# Convert text date into date
/*UPDATE manufacturing_dataset
SET doc_date_clean = STR_TO_DATE(`Doc Date`, '%Y-%m-%d %H:%i:%s');*/

# replace old column
#ALTER TABLE manufacturing_dataset DROP COLUMN `Doc Date`;
#ALTER TABLE manufacturing_dataset CHANGE doc_date_clean `Doc Date` DATE;

# Change datatype
/*ALTER TABLE manufacturing_dataset
MODIFY `Cust Code` VARCHAR(50);*/

# Create trigger
/*CREATE DEFINER=`root`@`localhost` TRIGGER `manufacturing_dataset_BEFORE_INSERT` BEFORE INSERT ON `manufacturing_dataset` FOR EACH ROW BEGIN
IF NEW.`Rejected Qty` < 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Rejected Qty cannot be negative!';
    END IF;
END*/
show triggers;
show tables;

# Stored Procedure 
/*  CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_insert_manufacturing_record`(
    IN p_emp_code VARCHAR(50),
    IN p_emp_name VARCHAR(100),
    IN p_doc_date DATE,
    IN p_rejected_qty INT,
    IN p_item_name VARCHAR(255),
    IN p_cust_code VARCHAR(50)
)
BEGIN
INSERT INTO manufacturing_dataset
    (`EMP Code`, `Emp Name`, `doc_date_clean`, `Rejected Qty`, `Item Name`, `Cust Code`)
    VALUES
    (p_emp_code, p_emp_name, p_doc_date, p_rejected_qty, p_item_name, p_cust_code);
END  */
CALL sp_insert_manufacturing_record(
    'E101',
    'Amit Kumar',
    '2025-01-01',
    5,
    'Gear Assembly',
    'C0099'
);

DESCRIBE manufacturing_dataset;

# Call it stored procedure employee daily rejection
/* CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_daily_rejection_report`(IN p_date DATE)
BEGIN
    SELECT 
        `EMP Code`,
        `Emp Name`,
        SUM(`Rejected Qty`) AS Total_Rejected
    FROM manufacturing_dataset
    WHERE `doc_date_clean` = p_date
    GROUP BY `EMP Code`, `Emp Name`
    ORDER BY Total_Rejected DESC;
END */
CALL sp_daily_rejection_report('2015-01-05');

# Machine perfromance stored procedure

/*CREATE PROCEDURE sp_machine_performance()
BEGIN
    SELECT 
        `Machine Code`,
        SUM(`today Manufactured qty`) AS Total_Produced,
        SUM(`Rejected Qty`) AS Total_Rejected
    FROM manufacturing_dataset
    GROUP BY `Machine Code`
    ORDER BY Total_Produced DESC;
END */
CALL sp_machine_performance();

# Index + Stored Procedure work together
/*CREATE INDEX idx_manufacturing_dataset_empcode_doc_date_clean
ON manufacturing_dataset (`EMP Code`, `doc_date_clean`);*/

CALL sp_daily_rejection_report('2015-01-05'); # Index + stored procedure

/*ALTER TABLE manufacturing_dataset
MODIFY `EMP Code` VARCHAR(10);*/

# Create View on employee rejection 
/*CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `vw_employee_rejection` AS
    SELECT 
        `manufacturing_dataset`.`EMP Code` AS `EMP Code`,
        `manufacturing_dataset`.`Emp Name` AS `Emp Name`,
        SUM(`manufacturing_dataset`.`Rejected Qty`) AS `Total_Rejected`
    FROM
        `manufacturing_dataset`
    GROUP BY `manufacturing_dataset`.`EMP Code` , `manufacturing_dataset`.`Emp Name` */
    SELECT * FROM vw_employee_rejection;

# Date wise rejection report
/*CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `vw_daily_rejection` AS
    SELECT 
        `manufacturing_dataset`.`doc_date_clean` AS `doc_date_clean`,
        `manufacturing_dataset`.`EMP Code` AS `EMP Code`,
        `manufacturing_dataset`.`Emp Name` AS `Emp Name`,
        SUM(`manufacturing_dataset`.`Rejected Qty`) AS `Total_Rejected`
    FROM
        `manufacturing_dataset`
    GROUP BY `manufacturing_dataset`.`doc_date_clean` , `manufacturing_dataset`.`EMP Code` , `manufacturing_dataset`.`Emp Name` */
    SELECT *
FROM vw_daily_rejection
WHERE `doc_date_clean` = '2025-01-01';

#See all View
SHOW FULL TABLES
WHERE Table_type = 'VIEW';

# Window function Employees by Rejected Quantity
SELECT
    `EMP Code`,
    `Emp Name`,
    `Rejected Qty`,
    RANK() OVER (ORDER BY `Rejected Qty` DESC) AS Rejection_Rank
FROM manufacturing_dataset;

# Dense rank No Gaps in Ranking
SELECT
    `EMP Code`,
    `Emp Name`,
    `Rejected Qty`,
    DENSE_RANK() OVER (ORDER BY `Rejected Qty` DESC) AS Dense_Rejection_Rank
FROM manufacturing_dataset;

# Running Total of Rejected Qty (OVER ORDER BY)
SELECT
    `EMP Code`,
    `doc_date_clean`,
    `Rejected Qty`,
    SUM(`Rejected Qty`) OVER (ORDER BY `doc_date_clean`) AS Running_Rejected_Total
FROM manufacturing_dataset;

# Machine Performance Rank (Partition by Machine)
SELECT
    `Machine Code`,
    `EMP Code`,
    `today Manufactured qty`,
    ROW_NUMBER() OVER (PARTITION BY `Machine Code` ORDER BY `today Manufactured qty` DESC) AS Machine_Rank
FROM manufacturing_dataset;

# Aggregrate Function
/*CREATE DEFINER=`root`@`localhost` FUNCTION `fn_total_rejected`() RETURNS int
    DETERMINISTIC
BEGIN
    DECLARE total_rej INT;

    SELECT SUM(`Rejected Qty`)
    INTO total_rej
    FROM manufacturing_dataset;

    RETURN total_rej;
END*/

SELECT fn_total_rejected();


# Parameterized Aggregate Function
/* CREATE DEFINER=`root`@`localhost` FUNCTION `fn_employee_rejection`(p_emp_code VARCHAR(10)) RETURNS int
    DETERMINISTIC
BEGIN
    DECLARE emp_rej INT;

    SELECT SUM(`Rejected Qty`)
    INTO emp_rej
    FROM manufacturing_dataset
    WHERE `EMP Code` = p_emp_code;

    RETURN emp_rej;
END*/

SELECT fn_employee_rejection('EM190');




