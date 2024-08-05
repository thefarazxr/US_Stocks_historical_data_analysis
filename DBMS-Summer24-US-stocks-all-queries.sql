CREATE DATABASE STOOQ_DATA_USA
GO

USE STOOQ_DATA_USA
GO

-- Create the DailyOHLCV table to store daily OHLCV data for US stocks and ETFs.
CREATE TABLE DailyOHLCV (
    -- Name of the exchange where the stock or ETF is listed (e.g., NYSE, NASDAQ).
    ExchangeName VARCHAR(100) NOT NULL,
    -- Type of listing, either 'Stocks' or 'ETFS'. This ensures that only these two values are allowed.
    ExchangeType CHAR(6) NOT NULL CHECK (ExchangeType IN ('Stocks', 'ETFS')),
    -- Ticker symbol of the stock or ETF.
    Ticker VARCHAR(18) NOT NULL,
    -- The period of the data, typically 'D' for daily.
    Period VARCHAR(5) NOT NULL,
    -- The date of the trade in YYYY-MM-DD format.
    TradeDate DATE NOT NULL,
    -- The time of the trade in HH:MM:SS format.
    TradeTime TIME(0) NOT NULL,
    -- The opening price of the stock or ETF on that date.
    OpenPrice DECIMAL(18, 4) NOT NULL,
    -- The highest price of the stock or ETF during the trading day.
    HighPrice DECIMAL(18, 4) NOT NULL,
    -- The lowest price of the stock or ETF during the trading day.
    LowPrice DECIMAL(18, 4) NOT NULL,
    -- The closing price of the stock or ETF on that date.
    ClosePrice DECIMAL(18, 4) NOT NULL,
    -- The total volume of shares traded on that date.
    Volume DECIMAL(22, 9) NOT NULL,
    -- The open interest, which is generally used in the context of options and futures. It can be NULL if not applicable.
    OpenInterest BIGINT NULL,
    -- Primary key constraint to ensure each record is unique based on the combination of ExchangeName, Ticker, TradeDate, and TradeTime.
    PRIMARY KEY (ExchangeName, Ticker, TradeDate, TradeTime)
);


--check total row count
Select Count(*) from STOOQ_DATA_USA.dbo.DailyOHLCV;

--To imporove speed, lets do indexing

-- Create an index on the Ticker column
CREATE INDEX idx_ticker ON dbo.DailyOHLCV(Ticker);

-- Create an index on ExchangeName for queries that filter by exchange
CREATE INDEX idx_exchangeName ON dbo.DailyOHLCV(ExchangeName);

-- Create an index on ExchangeType for queries that filter by exchange
CREATE INDEX idx_exchangeType ON dbo.DailyOHLCV(ExchangeType);

-- number of listings by ExchangeName and Type
WITH TickerCOUNT as (
select ExchangeName, ExchangeType, count(distinct Ticker) AS TickerCount
from STOOQ_DATA_USA.dbo.DailyOHLCV
group by ExchangeName, ExchangeType
--order by ExchangeName;
)


-- Total ticker count
select sum(TickerCount)
from TickerCOUNT

select Distinct Ticker
from DailyOHLCV

select distinct(TICKER), count(*) from STOOQ_DATA_USA.dbo.DailyOHLCV
--where ExchangeName='NASDAQ'
--and ExchangeType='Stocks'
group by Ticker
order by Ticker;

use STOOQ_DATA_USA
go

--get max HighPrice of NVDIA stock (up until July 9, 2024)
select max(HighPrice) from STOOQ_DATA_USA.dbo.DailyOHLCV
where Ticker='NVDA.US';

select ExchangeName, ExchangeType, count(distinct TickerSymbol) AS TickerCount
from STOOQ_DATA_USA.dbo.Tickers
group by ExchangeName, ExchangeType

--Exchange Table
CREATE TABLE Exchanges (
    ExchangeID INT IDENTITY(1,1) PRIMARY KEY,
    ExchangeName VARCHAR(100) NOT NULL,
    ExchangeType CHAR(6) NOT NULL CHECK (ExchangeType IN ('Stocks', 'Etfs'))
);

SET IDENTITY_INSERT dbo.Exchanges ON;
--This should FAIL, as it fails the check constraints
INSERT INTO Exchanges(ExchangeID, ExchangeName, ExchangeType) VALUES (5, 'FARAZ_Exchange', 'Crypto')

select * from Exchanges

--Tickers Table
CREATE TABLE Tickers (
     -- Unique identifier for each ticker
    TickerID INT IDENTITY(1,1) PRIMARY KEY,
    -- Symbol representing the stock ticker (e.g., AAPL for Apple Inc.)
    TickerSymbol VARCHAR(18) NOT NULL,
    -- Full name of the stock (e.g., Apple Inc.)
    StockName VARCHAR(100),
    -- Name of the exchange where the stock is listed (e.g., NYSE, NASDAQ)
    ExchangeName VARCHAR(100)
);

select * from NASDAQ_tickers
select distinct Ticker from DailyOHLCV
select * from NYSE_tickers


INSERT INTO Tickers 
select Symbol,StockName, ExchangeName from NYSE_Tickers

INSERT INTO Tickers 
select Symbol,StockName, ExchangeName from NASDAQ_Tickers

select * from Tickers

--To have same symbol formatting of ending with .US as in DailyOHLCV table
UPDATE Tickers
SET TickerSymbol=TickerSymbol+'.US'

--add new column in Tickers table for Exchange Type
ALTER TABLE Tickers
ADD ExchangeType char(6) 

select distinct Ticker from DailyOHLCV

select Tickers.TickerSymbol, DailyOHLCV.Ticker, Tickers.StockName, Tickers.ExchangeName, DailyOHLCV.ExchangeName 
,Tickers.ExchangeType , DailyOHLCV.ExchangeType
from Tickers
JOIN DailyOHLCV
ON DailyOHLCV.Ticker=Tickers.TickerSymbol

	
select distinct Tickers.ExchangeType
from Tickers
JOIN DailyOHLCV
ON DailyOHLCV.Ticker=Tickers.TickerSymbol

select distinct ExchangeID, ExchangeName, ExchangeType 
from Tickers
where ExchangeID is not NULL;


--Normalize Tickers with Exchanges
--Add ExchangeID and  later remove ExchangeName and ExchangeType
ALTER TABLE Tickers
 ADD ExchangeID INT ,
	 CONSTRAINT FK_ExchangeID
 FOREIGN KEY (ExchangeID)
 REFERENCES Exchanges(ExchangeID)

 Update t
	SET t.ExchangeID=e.ExchangeID
	FROM Exchanges e
	JOIN Tickers t
	ON 
	t.ExchangeName=e.ExchangeName
and t.ExchangeType=e.ExchangeType;

 --Normalize DailyOHLCV -normalize with ExchangeID,
 select  distinct ExchangeID, ExchangeName, ExchangeType
from Exchanges

select distinct ExchangeID, ExchangeName, ExchangeType 
from Tickers
where ExchangeID is not NULL;

select ExchangeName, ExchangeType, count(distinct Ticker) AS TickerCount
from STOOQ_DATA_USA.dbo.DailyOHLCV
group by ExchangeName, ExchangeType

select ExchangeName, ExchangeType, count(distinct TickerSymbol) AS TickerCount
from Tickers
group by ExchangeName, ExchangeType

--Add ExchangeID and  later remove ExchangeName and ExchangeType
ALTER TABLE DailyOHLCV
 ADD ExchangeID INT ,
	 CONSTRAINT FK_ExchangeID_in_DailyOHLCV
 FOREIGN KEY (ExchangeID)
 REFERENCES Exchanges(ExchangeID)

 
 Update d
	SET d.ExchangeID=e.ExchangeID
	FROM Exchanges e
	JOIN DailyOHLCV d
	ON 
	d.ExchangeName=e.ExchangeName
and d.ExchangeType=e.ExchangeType;

--For complex calculation create a VIEW instead of a table, 
--so that even if base table updates we dont need to update the calc based tables!

--1. Basic Stats (Descriptive Analysis)
--all stocks 
SELECT
    AVG(OpenPrice) AS AvgOpenPrice,
    AVG(ClosePrice) AS AvgClosePrice,
    MAX(HighPrice) AS MaxHighPrice,
    MIN(LowPrice) AS MinLowPrice,
    SUM(Volume) AS TotalVolume
FROM DailyOHLCV;

--Ticker wise
SELECT  Ticker,
    AVG(OpenPrice) AS AvgOpenPrice,
    AVG(ClosePrice) AS AvgClosePrice,
    MAX(HighPrice) AS MaxHighPrice,
    MIN(LowPrice) AS MinLowPrice,
    SUM(Volume) AS TotalVolume
FROM DailyOHLCV
where TradeDate BETWEEN '2011-10-05' and '2024-10-05'
group by Ticker
order by Ticker;

--2. Time-Based Analysis
-- Aggregating Day-to-Day or Weekly or Monthly

-- Daily Aggregation
SELECT
    Ticker,
    TradeDate,
    AVG(OpenPrice) AS AvgOpenPrice,
    MAX(HighPrice) AS MaxHighPrice,
    MIN(LowPrice) AS MinLowPrice,
    AVG(ClosePrice) AS AvgClosePrice,
    SUM(Volume) AS TotalVolume
FROM DailyOHLCV
GROUP BY Ticker, TradeDate
ORDER BY Ticker, TradeDate;
--Lets do Moving Averages for companies like (
/*
AAPL
,META
,MSFT
,NVDA
,AMZN
,SOFI
,UBER
,SPOT
,SNAP
,EBAY
,DXCM
)
*/

-- 10-day Moving Average of Close Price
CREATE VIEW View_MovingAvg10 AS	
SELECT
    Ticker,
    TradeDate,
    AVG(ClosePrice) OVER (PARTITION BY Ticker ORDER BY TradeDate ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS MovingAvg10
	--current row and 9 preceeding rows i.e. past 10 days
FROM DailyOHLCV
where Ticker in
(
 'AAPL.US'
,'META.US'
,'MSFT.US'
,'NVDA.US'
,'AMZN.US'
,'SOFI.US'
,'UBER.US'
,'SPOT.US'
,'SNAP.US'
,'EBAY.US'
,'DXCM.US'
)
;


-- 30-day Moving Average of Close Price
CREATE VIEW View_MovingAvg30 AS	
SELECT
    Ticker,
    TradeDate,
    AVG(ClosePrice) OVER (PARTITION BY Ticker ORDER BY TradeDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS MovingAvg30
	--current row and 29 preceeding rows i.e. past 30 days
FROM DailyOHLCV
where Ticker in
(
 'AAPL.US'
,'META.US'
,'MSFT.US'
,'NVDA.US'
,'AMZN.US'
,'SOFI.US'
,'UBER.US'
,'SPOT.US'
,'SNAP.US'
,'EBAY.US'
,'DXCM.US'
);

--3. Moving Averages
--Traders use moving averages (MA) to pinpoint trade areas, identify trends, and analyze markets.
--Two of the most common types are simple and exponential. 
-- Moving averages reveal the average price of a tradable instrument over a given period of time. 
-- To calculate a 10-day simple moving average (SMA), add the closing prices of the last 10 days and divide by 10.
-- Exponential Moving Average: The exponential moving average (EMA) focuses more on recent prices than on a long series of data points

--An exponentially weighted moving average reacts more significantly to recent price changes than 
--a simple moving average simple moving average (SMA), which applies an equal weight to all observations in the period.

--Refer: Investopedia for more explanations

-- 10-day SMA
SELECT
    Ticker,
    TradeDate,
    AVG(ClosePrice) OVER (PARTITION BY Ticker ORDER BY TradeDate ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS SMA10
FROM DailyOHLCV
where Ticker in ('AAPL.US')

and TradeDate >'2011-10-05';


-- 10-day EMA (simplified calculation)
-- Create a temporary table to hold the EMA results

-- Drop the existing view if it exists
IF OBJECT_ID('dbo.View_ExponenMovingAvg10', 'V') IS NOT NULL
    DROP VIEW dbo.View_ExponenMovingAvg10;
GO

CREATE TABLE #EMA (
    Ticker NVARCHAR(10),
    TradeDate DATE,
    ClosePrice DECIMAL(18, 2),
    EMA10 DECIMAL(18, 2)
);

-- Declare variables
DECLARE @Ticker NVARCHAR(10);
DECLARE @TradeDate DATE;
DECLARE @ClosePrice DECIMAL(18, 2);
DECLARE @PreviousEMA DECIMAL(18, 2);
DECLARE @EMA10 DECIMAL(18, 2);
DECLARE @Alpha DECIMAL(18, 4) = 2.0 / (10 + 1); -- Alpha for 10-day EMA

-- Declare a cursor for selecting the data in the order of Ticker and TradeDate
DECLARE price_cursor CURSOR FOR
SELECT Ticker, TradeDate, ClosePrice
FROM DailyOHLCV
--WHERE Ticker = 'AAPL.US' 
--AND TradeDate > '2011-10-05'
where Ticker in
(
 'AAPL.US'
,'META.US'
,'MSFT.US'
,'NVDA.US'
,'AMZN.US'
,'SOFI.US'
,'UBER.US'
,'SPOT.US'
,'SNAP.US'
,'EBAY.US'
,'DXCM.US'
)
ORDER BY Ticker, TradeDate;

-- Open the cursor
OPEN price_cursor;

-- Fetch the first row
FETCH NEXT FROM price_cursor INTO @Ticker, @TradeDate, @ClosePrice;

-- Initialize the EMA with the first close price
SET @EMA10 = @ClosePrice;

-- Insert the first row into the temporary table
INSERT INTO #EMA (Ticker, TradeDate, ClosePrice, EMA10)
VALUES (@Ticker, @TradeDate, @ClosePrice, @EMA10);

-- Fetch the subsequent rows and calculate the EMA
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Calculate the EMA
    SET @EMA10 = (@ClosePrice * @Alpha) + (@EMA10 * (1 - @Alpha));

    -- Insert the current row with the calculated EMA into the temporary table
    INSERT INTO #EMA (Ticker, TradeDate, ClosePrice, EMA10)
    VALUES (@Ticker, @TradeDate, @ClosePrice, @EMA10);

    -- Fetch the next row
    FETCH NEXT FROM price_cursor INTO @Ticker, @TradeDate, @ClosePrice;
END;

-- Close and deallocate the cursor
CLOSE price_cursor;
DEALLOCATE price_cursor;

-- Select the results into a new table (or directly into a view if no further manipulation is needed)
SELECT * INTO EMAResults10Days FROM #EMA;

-- Drop the temporary table
DROP TABLE #EMA;

-- Create a view for the EMA results
GO
CREATE VIEW View_ExponenMovingAvg10 AS	
SELECT * FROM EMAResults10Days;
--ORDER BY Ticker, TradeDate;

GO


--30Days EMA

-- 30-day EMA (simplified calculation)
-- Create a temporary table to hold the EMA results

-- Drop the existing view if it exists
IF OBJECT_ID('dbo.View_ExponenMovingAvg30', 'V') IS NOT NULL
    DROP VIEW dbo.View_ExponenMovingAvg30;
GO

CREATE TABLE #EMA (
    Ticker NVARCHAR(10),
    TradeDate DATE,
    ClosePrice DECIMAL(18, 2),
    EMA30 DECIMAL(18, 2)
);

-- Declare variables
DECLARE @Ticker NVARCHAR(10);
DECLARE @TradeDate DATE;
DECLARE @ClosePrice DECIMAL(18, 2);
DECLARE @PreviousEMA DECIMAL(18, 2);
DECLARE @EMA30 DECIMAL(18, 2);
DECLARE @Alpha DECIMAL(18, 4) = 2.0 / (30 + 1); -- Alpha for 30-day EMA

-- Declare a cursor for selecting the data in the order of Ticker and TradeDate
DECLARE price_cursor CURSOR FOR
SELECT Ticker, TradeDate, ClosePrice
FROM DailyOHLCV
--WHERE Ticker = 'AAPL.US' 
--AND TradeDate > '2011-10-05'
where Ticker in
(
 'AAPL.US'
,'META.US'
,'MSFT.US'
,'NVDA.US'
,'AMZN.US'
,'SOFI.US'
,'UBER.US'
,'SPOT.US'
,'SNAP.US'
,'EBAY.US'
,'DXCM.US'
)
ORDER BY Ticker, TradeDate;

-- Open the cursor
OPEN price_cursor;

-- Fetch the first row
FETCH NEXT FROM price_cursor INTO @Ticker, @TradeDate, @ClosePrice;

-- Initialize the EMA with the first close price
SET @EMA30 = @ClosePrice;

-- Insert the first row into the temporary table
INSERT INTO #EMA (Ticker, TradeDate, ClosePrice, EMA30)
VALUES (@Ticker, @TradeDate, @ClosePrice, @EMA30);

-- Fetch the subsequent rows and calculate the EMA
WHILE @@FETCH_STATUS = 0
BEGIN
    -- Calculate the EMA
    SET @EMA30 = (@ClosePrice * @Alpha) + (@EMA30 * (1 - @Alpha));

    -- Insert the current row with the calculated EMA into the temporary table
    INSERT INTO #EMA (Ticker, TradeDate, ClosePrice, EMA30)
    VALUES (@Ticker, @TradeDate, @ClosePrice, @EMA30);

    -- Fetch the next row
    FETCH NEXT FROM price_cursor INTO @Ticker, @TradeDate, @ClosePrice;
END;

-- Close and deallocate the cursor
CLOSE price_cursor;
DEALLOCATE price_cursor;

-- Select the results into a new table (or directly into a view if no further manipulation is needed)
SELECT * INTO EMAResults30Days FROM #EMA;

-- Drop the temporary table
DROP TABLE #EMA;

-- Create a view for the EMA results
GO
CREATE VIEW View_ExponenMovingAvg30 AS	
SELECT * FROM EMAResults30Days;
--ORDER BY Ticker, TradeDate;



select * from View_ExponenMovingAvg10
where Ticker='AAPL.US'

select 
Ticker,
AVG(HighPrice),
AVG(LowPrice)
from DailyOHLCV
--where Ticker='AAPL.US'
group by Ticker


-- Relative Strength Index (RSI): Measures the speed and change of price movements
-- https://www.investopedia.com/terms/r/rsi.asp
--to evaluate overvalued or undervalued conditions in the price of the stock.
-- Traditionally, an RSI reading of 70 or above indicates an overbought situation. 
-- A reading of 30 or below indicates an oversold condition.

CREATE TABLE AverageGainsAndLosses (
    Ticker VARCHAR(18),
    TradeDate DATE,
    AvgGain DECIMAL(18, 4),
    AvgLoss DECIMAL(18, 4)
);


WITH 
CTE_PreviousClose AS (
    SELECT
        Ticker,
        TradeDate,
        ClosePrice,
        LAG(ClosePrice, 1) OVER (PARTITION BY Ticker ORDER BY TradeDate) AS PrevClosePrice
    FROM DailyOHLCV
		where Ticker in
(
 'AAPL.US'
,'META.US'
,'MSFT.US'
,'NVDA.US'
,'AMZN.US'
,'SOFI.US'
,'UBER.US'
,'SPOT.US'
,'SNAP.US'
,'EBAY.US'
,'DXCM.US'
)
--and TradeDate >='2011-10-06'
),

CTE_GainsAndLosses AS (
    SELECT
        Ticker,
        TradeDate,
        ClosePrice,
		PrevClosePrice,
        CASE
            WHEN ClosePrice > PrevClosePrice THEN ClosePrice - PrevClosePrice
            ELSE 0
        END AS Gain,
        CASE
            WHEN ClosePrice < PrevClosePrice THEN PrevClosePrice - ClosePrice
            ELSE 0
        END AS Loss
    FROM CTE_PreviousClose
)
INSERT INTO AverageGainsAndLosses (Ticker, TradeDate, AvgGain, AvgLoss)
    SELECT
        Ticker,
        TradeDate,
		--Average Gain and Loss over past 14 days (2 weeks)
        AVG(Gain) OVER (PARTITION BY Ticker ORDER BY TradeDate ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS AvgGain,
        AVG(Loss) OVER (PARTITION BY Ticker ORDER BY TradeDate ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS AvgLoss
    FROM CTE_GainsAndLosses;



CREATE VIEW RSI_View AS
SELECT
    Ticker,
    TradeDate,
    100 - (100 / (1 + (AvgGain / AvgLoss))) AS RSI14,
	CASE
		WHEN 100 - (100 / (1 + (AvgGain / AvgLoss))) >=70 then 'Overbought'
		WHEN 100 - (100 / (1 + (AvgGain / AvgLoss)))<=30 then 'Oversold'
		else 'Stable'
	END AS Market_Stock_Condition
FROM AverageGainsAndLosses
WHERE AvgLoss != 0;


select * from RSI_View
where Ticker='AAPL.US'
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

--4. Pattern Recognition
-- Candlestick Pattern
--  Doji Candlestick
-- Example: Identify Doji candlesticks (where the Open and Close prices are almost equal)
CREATE VIEW DojiCandlesticks AS
SELECT
    Ticker,
    TradeDate,
    OpenPrice,
    ClosePrice,
    HighPrice,
    LowPrice
FROM DailyOHLCV
WHERE ABS(OpenPrice - ClosePrice) < (HighPrice - LowPrice) * 0.1;
--we dont directly do OpenPrice=ClosePrice as this is super rare or impossible in market
--hence see if its less than one-tenth of that day's range i.e Max-Min

--5 Temporal Analysis
-- 5a Time-Based Slicing and Merging: Compare data between different Date and time slices.
-- Compare performance of the stock on different days
SELECT
    a.Ticker,
    a.TradeDate AS TradeDate1,
    b.TradeDate AS TradeDate2,
    a.ClosePrice AS ClosePrice1,
    b.ClosePrice AS ClosePrice2,
    b.ClosePrice - a.ClosePrice AS PriceDifference

FROM DailyOHLCV a
JOIN DailyOHLCV b ON a.Ticker = b.Ticker AND a.TradeDate < b.TradeDate
where a.Ticker in ('AAPL.US')
and a.TradeDate='2024-01-05'
;



--5b Yearly Avg 
SELECT
    Ticker,
    YEAR(TradeDate) AS Year,
    AVG(ClosePrice) AS YearlyAvgPrice
FROM DailyOHLCV
WHERE Ticker = 'AAPL.US'
GROUP BY Ticker, YEAR(TradeDate)
ORDER BY Year;

--5c YearOnYear Growth
WITH YearlyData AS (
    SELECT
        Ticker,
        YEAR(TradeDate) AS Year,
        AVG(ClosePrice) AS YearlyAvgPrice
    FROM DailyOHLCV
    WHERE Ticker = 'AAPL.US'
    GROUP BY Ticker, YEAR(TradeDate)
)
SELECT
    CurrentYear.Year AS Year,
    PreviousYear.YearlyAvgPrice AS PreviousYearAvgPrice,
	 CurrentYear.YearlyAvgPrice AS CurrentYearAvgPrice,
    ((CurrentYear.YearlyAvgPrice - PreviousYear.YearlyAvgPrice) / PreviousYear.YearlyAvgPrice) * 100 AS YoYGrowth
FROM
    YearlyData AS CurrentYear
LEFT JOIN
    YearlyData AS PreviousYear ON CurrentYear.Year-PreviousYear.Year= 1
ORDER BY
    CurrentYear.Year;
--5c VIEW
CREATE VIEW YearOnYearGrowth AS
WITH YearlyData AS (
    SELECT
        Ticker,
        YEAR(TradeDate) AS Year,
        AVG(ClosePrice) AS YearlyAvgPrice
    FROM DailyOHLCV
    GROUP BY Ticker, YEAR(TradeDate)
)
SELECT
    CurrentYear.Ticker AS Ticker,
    CurrentYear.Year AS Year,
    PreviousYear.YearlyAvgPrice AS PreviousYearAvgPrice,
    CurrentYear.YearlyAvgPrice AS CurrentYearAvgPrice,
    ((CurrentYear.YearlyAvgPrice - PreviousYear.YearlyAvgPrice) / PreviousYear.YearlyAvgPrice) * 100 AS YoYGrowth
FROM
    YearlyData AS CurrentYear
LEFT JOIN
    YearlyData AS PreviousYear ON CurrentYear.Ticker = PreviousYear.Ticker AND CurrentYear.Year = PreviousYear.Year + 1
;

--5d Avg Monthly Volumes
SELECT
    Ticker,
    YEAR(TradeDate) AS Year,
   -- MONTH(TradeDate) AS Month,
    AVG(Volume) AS AvgMonthlyVolume
FROM DailyOHLCV
WHERE Ticker = 'AAPL.US'
GROUP BY Ticker, YEAR(TradeDate)
--, MONTH(TradeDate)
ORDER BY Year
--, Month;


--6 Comparitive analysis
--Compare the performance of different stocks or ETFs.
SELECT
    a.Ticker AS Ticker1,
    b.Ticker AS Ticker2,
    a.TradeDate,
    a.ClosePrice AS ClosePrice1,
    b.ClosePrice AS ClosePrice2,
    ABS(b.ClosePrice - a.ClosePrice) AS PriceDifference,
	CASE
		WHEN a.ClosePrice> b.ClosePrice then a.Ticker
		else b.Ticker
	END AS WinnerOfTheDay
FROM DailyOHLCV a
JOIN DailyOHLCV b ON a.TradeDate = b.TradeDate
WHERE a.Ticker = 'TSLA.US' AND b.Ticker = 'AAPL.US'
and a.TradeDate >='2011-10-05'
;

-- Query to find Key stats over a period using `BETWEEEN`
SELECT 
    Ticker, 
     AVG(OpenPrice) AS AvgOpenPrice,
    AVG(ClosePrice) AS AvgClosePrice,
    MAX(HighPrice) AS MaxHighPrice,
    MIN(LowPrice) AS MinLowPrice,
    SUM(Volume) AS TotalVolume
FROM 
    DailyOHLCV 
WHERE 
	Ticker='AAPL.US'
	and
	TradeDate BETWEEN '2020-09-01' AND '2024-09-01'
GROUP BY 
    Ticker;

	


--New Events
select * from newsEvents
order by 1
--newsEvents is very slow (almost 2 mins)
--Let's do indexing
-- Create an index on newsID in table newsEvents for queries that filter by exchange
CREATE INDEX idx_newsID ON dbo.newsEvents(newsID);

--Rebuild and Reorganize indexes as they become Fragmented with time
ALTER INDEX ALL ON dbo.newsEvents REBUILD;
ALTER INDEX ALL ON dbo.newsEvents REORGANIZE;

-- Create an index on newsDateTime in table newsEvents for queries that filter by exchange
CREATE INDEX idx_newsDateTime ON dbo.newsEvents(newsDateTime);

--Rebuild and Reorganize indexes as they become Fragmented with time
ALTER INDEX ALL ON dbo.newsEvents REBUILD;
ALTER INDEX ALL ON dbo.newsEvents REORGANIZE;


UPDATE newsEvents
 SET newsDate= CAST(newsDateTime as DATE)
--Exec time only 2 secs (190.2K records loaded)
select   *
from newsEvents
order by newsDateTime

--Analyze impact of News on Stock Prices:
-- Analyze impact of News on Stock Prices and compare with previous day's prices:
-- % change if > +2% then Bull-ish (BUY)
-- % change if < -2% then Bear-ish (SELL)
-- % change is between +2% and -2% then NEUTRAL (HOLD)

-- CTE to calculate the previous day's close price and trade date
WITH DailyLagged AS (
    SELECT 
        Ticker,
        TradeDate,
        OpenPrice,
        HighPrice,
        LowPrice,
        ClosePrice,
        -- LAG function to get previous day's ClosePrice and TradeDate
        LAG(TradeDate) OVER (PARTITION BY Ticker ORDER BY TradeDate) AS PrevTradeDate,
        LAG(ClosePrice) OVER (PARTITION BY Ticker ORDER BY TradeDate) AS PrevClosePrice
    FROM 
        DailyOHLCV
    WHERE 
        Ticker = 'TSLA.US'
)

-- Join the CTE result with the newsEvents table
SELECT 
    d.TradeDate, 
    d.OpenPrice,
    d.HighPrice,
    d.LowPrice,
    d.ClosePrice, 
    n.newsTitle, 
    n.newsDateTime,
    d.Ticker,
    d.PrevTradeDate,
    d.PrevClosePrice,
    (d.ClosePrice - d.PrevClosePrice) AS PriceChange,
    ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) AS PriceChangePercent,
    CASE
        WHEN ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) > 2 THEN 'Bullish'
        WHEN ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) < -2 THEN 'Bearish'
        ELSE 'Neutral'
    END AS News_IMPACT,
    CASE
        WHEN ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) > 2 THEN 'BUY'
        WHEN ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) < -2 THEN 'SELL'
        ELSE 'HOLD'
    END AS Investment_Advice
FROM 
    DailyLagged d
JOIN 
    newsEvents n 
ON 
    d.Ticker COLLATE SQL_Latin1_General_CP1_CI_AS = n.Ticker COLLATE SQL_Latin1_General_CP1_CI_AS
    AND d.TradeDate = CAST(n.newsDateTime AS DATE)
WHERE 
    n.newsDateTime BETWEEN '2014-01-01' AND '2021-01-01'
ORDER BY 
    PriceChangePercent;

--	>>>>>>>>>>>>>>>>>>>>>>>>>>>>------<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

 select  * from DailyOHLCV
WHERE 
    CAST(Ticker as VARCHAR) = CAST('TSLA.US' as VARCHAR)
 order by 1

 select distinct TickerID, TickerSymbol from Tickers


 select distinct TICKER from newsEvents


--INSIDER TRADING Table
-- Legal Insider Trading:Corporate insiders (directors, officers, employees) buy or sell their company’s stock following regulations, typically disclosed via SEC Form 4.

--Illegal Insider Trading:Insiders trade based on non-public material information (MNPI), gaining an unfair advantage over regular investors.

--Let's pick few stocks for Insider Trading Usecase
-- Stocks: 
-- Tesla, Inc. (TSLA) - Frequent insider trading by CEO Elon Musk and other executives.
-- Amazon.com, Inc. (AMZN) - Regular insider trades by key executives.
-- Microsoft Corporation (MSFT) - Significant insider trading by top executives.
-- Apple Inc. (AAPL) - Regular insider trading activities by executives and directors

--Before Data Import- had to do Data_cleaning and formatting in csv's before importing

--AAPL
select * from InsiderTrading_AAPL

--AMZN
select * from InsiderTrading_AMZN

select * from DailyOHLCV
where Ticker='AMZN.US'
--MSFT
select * from InsiderTrading_MSFT

--TSLA
select * from InsiderTrading_TSLA

--Insider Trades (all 4 combined)
select * from InsiderTrades


--Total number of shares traded by each insider
-- (INSIDER_name, JOB-ROLE, TotlaShared_Traded)
CREATE VIEW InsiderSharesTraded AS
SELECT 
    i.Insider_Trading as INSIDER_TRADER, 
	i.Relationship as Insider_Role,
    SUM(i.Shares) AS TotalSharesTraded,
	AVG(Value) AS AvgTransactionValue,
	i.Ticker as Ticker,
	t.StockName as Company
FROM 
    InsiderTrades i
JOIN Tickers t
ON i.Ticker=t.TickerSymbol

GROUP BY 
    Insider_Trading, Relationship, i.Ticker, t.StockName;

GO
--Monthly Volume of Insider Trades
-- Compare the volume of shares traded by insiders over different periods:
CREATE VIEW MonthlyVolumeOfInsiderTrades AS
	SELECT 
	Ticker,
    DATEPART(YEAR, Date) AS TradingYear, 
    DATEPART(MONTH, Date) AS TradingMonth,
    SUM(Shares) AS TotalSharesTraded,
	t.StockName as Company
FROM 
    InsiderTrades i
JOIN Tickers t
ON i.Ticker=t.TickerSymbol
GROUP BY 
    Ticker, t.StockName, DATEPART(YEAR, Date), DATEPART(MONTH, Date);

GO

--Insider Trading WHEN any NEWS/EVENT in Market
--with an investment advice
		--insider trade are mostly reported within 2 days to SEC 
		--so lets check trade date on-next day to see how the market reacts
CREATE VIEW InsiderTradeNewsImpact AS
WITH DailyLagged AS (
    SELECT 
        Ticker,
        TradeDate,
        OpenPrice,
        HighPrice,
        LowPrice,
        ClosePrice,
        -- LAG function to get previous day's ClosePrice and TradeDate
        LAG(TradeDate) OVER (PARTITION BY Ticker ORDER BY TradeDate) AS PrevTradeDate,
        LAG(ClosePrice) OVER (PARTITION BY Ticker ORDER BY TradeDate) AS PrevClosePrice
    FROM 
        DailyOHLCV
)

SELECT 
	 d.PrevTradeDate,
    d.PrevClosePrice,
    (d.ClosePrice - d.PrevClosePrice) AS PriceChange,
    ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) AS PriceChangePercent,
	CASE
        WHEN ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) > 2 THEN 'Bullish'
        WHEN ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) < -2 THEN 'Bearish'
        ELSE 'Neutral'
    END AS News_IMPACT,
    CASE
        WHEN ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) > 2 THEN 'BUY'
        WHEN ((d.ClosePrice - d.PrevClosePrice) / d.PrevClosePrice * 100) < -2 THEN 'SELL'
        ELSE 'HOLD'
    END AS Investment_Advice,
    d.TradeDate, 
    d.ClosePrice, 
    n.newsTitle, 
    n.newsDate,
    i.Ticker,
    i.Insider_Trading,
    i.Relationship,
    i.[Date]  AS InsiderTradingDate,
    i.[Transaction],
    i.Shares  AS InsiderShares,
    i.[Value] AS InsiderValue
FROM 
    DailyLagged d
JOIN 
    newsEvents n 
ON 
		d.Ticker COLLATE SQL_Latin1_General_CP1_CI_AS = n.Ticker COLLATE SQL_Latin1_General_CP1_CI_AS
    AND d.TradeDate = n.newsDate
 JOIN 
    InsiderTrades i
ON 
		d.Ticker = i.Ticker
		--insider trade is mostly done on closed-market dates with SEC reporting
		--so lets check trade date on-next day to see how the market reacts
    AND d.TradeDate = i.Date;

--Check views created:
select * from InsiderSharesTraded
select * from MonthlyVolumeOfInsiderTrades

select * from InsiderTradeNewsImpact
where Ticker='TSLA.US'
ORDER BY TradeDate








