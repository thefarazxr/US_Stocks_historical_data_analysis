--Deliverables for Poject Design and Implementation
CREATE DATABASE Sample_US_Stocks_DB
GO

USE Sample_US_Stocks_DB
GO

-- Create the Exchanges table
CREATE TABLE Exchanges (
    ExchangeID INT PRIMARY KEY,
    ExchangeName VARCHAR(100) NOT NULL,
    ExchangeType CHAR(6) NOT NULL
);

-- Create the Tickers table
CREATE TABLE Tickers (
    TickerID INT PRIMARY KEY,
    Ticker VARCHAR(18) NOT NULL UNIQUE,
    StockName VARCHAR(100) NOT NULL,
    ExchangeID INT NOT NULL,
    FOREIGN KEY (ExchangeID) REFERENCES Exchanges(ExchangeID)
);

-- Create the DailyOHLCV table
CREATE TABLE DailyOHLCV (
    ExchangeName VARCHAR(100) NOT NULL,
    ExchangeType CHAR(6) NOT NULL,
    Ticker VARCHAR(18) NOT NULL,
    Period VARCHAR(5) NOT NULL,
    TradeDate DATE NOT NULL,
    TradeTime TIME(0) NOT NULL,
    OpenPrice DECIMAL(18, 4) NOT NULL,
    HighPrice DECIMAL(18, 4) NOT NULL,
    LowPrice DECIMAL(18, 4) NOT NULL,
    ClosePrice DECIMAL(18, 4) NOT NULL,
    Volume DECIMAL(22, 9) NOT NULL,
    OpenInterest BIGINT NULL,
    ExchangeID INT NOT NULL,
    PRIMARY KEY (ExchangeName, ExchangeType, Ticker, TradeDate),
    FOREIGN KEY (ExchangeID) REFERENCES Exchanges(ExchangeID)
);

-- Create the newsEvents table
CREATE TABLE newsEvents (
    newsID BIGINT PRIMARY KEY,
    newsDateTime DATETIMEOFFSET(7) NULL,
    newsTitle NVARCHAR(4000) NOT NULL,
    Ticker VARCHAR(50) NOT NULL,
    TickerID INT NOT NULL,
    newsDate DATE NOT NULL,
    FOREIGN KEY (TickerID) REFERENCES Tickers(TickerID)
);

-- Create the InsiderTrades table
CREATE TABLE InsiderTrades (
    Insider_Trading NVARCHAR(50) NOT NULL,
    Relationship NVARCHAR(50) NOT NULL,
    TradeDate DATE NOT NULL,
    [Transaction] NVARCHAR(50) NOT NULL,
    Cost FLOAT NOT NULL,
    Shares INT NOT NULL,
    Value BIGINT NOT NULL,
    Shares_Total INT NOT NULL,
    SEC_Form_4 DATETIME2(7) NOT NULL,
    Ticker VARCHAR(18) NULL,
    PRIMARY KEY (Insider_Trading, Date),
    FOREIGN KEY (Ticker) REFERENCES Tickers(TickerSymbol)
);


-- Insert sample data into Exchanges table
INSERT INTO Exchanges (ExchangeID, ExchangeName, ExchangeType)
VALUES 
(1, 'NASDAQ', 'Stocks'),
(2, 'NYSE', 'Stocks');

-- Insert sample data into Tickers table
INSERT INTO Tickers (TickerID, TickerSymbol, StockName, ExchangeID)
VALUES 
(1, 'AAPL.US', 'Apple Inc.', 1),
(2, 'MSFT.US', 'Microsoft Corp.', 1);

-- Insert sample data into DailyOHLCV table
INSERT INTO DailyOHLCV (ExchangeName, ExchangeType, Ticker, Period, TradeDate, TradeTime, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, OpenInterest, ExchangeID)
VALUES 
('NASDAQ', 'Stocks', 'AAPL.US', 'D', '2022-01-02', '00:00:00', 181.63, 182.34, 184.12, 184.01, 104489400.0, NULL, 1),
('NASDAQ', 'Stocks', 'MSFT.US', 'D', '2022-01-02', '00:00:00', 234.75, 335.00, 330.66, 338.32, 22148800.0, NULL, 1);

-- Insert sample data into DailyOHLCV table
INSERT INTO DailyOHLCV (ExchangeName, ExchangeType, Ticker, Period, TradeDate, TradeTime, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, OpenInterest, ExchangeID)
VALUES 
('NASDAQ', 'Stocks', 'AAPL.US', 'D', '2022-01-03', '00:00:00', 182.63, 182.94, 179.12, 182.01, 104487900.0, NULL, 1),
('NASDAQ', 'Stocks', 'MSFT.US', 'D', '2022-01-03', '00:00:00', 334.75, 348.00, 329.66, 336.32, 22148100.0, NULL, 1);

-- Insert sample data into newsEvents table
INSERT INTO newsEvents (newsID, newsDateTime, newsTitle, Ticker, TickerID, newsDate)
VALUES 
(1, '2022-01-03 14:30:00 +00:00', 'Apple Releases New Product', 'AAPL.US', 1, '2022-01-03');

-- Insert sample data into InsiderTrades table
INSERT INTO InsiderTrades (Insider_Trading, Relationship, TradeDate, [Transaction], Cost, Shares, Value, Shares_Total, SEC_Form_4, Ticker)
VALUES 
('Tim Cook', 'CEO', '2022-01-03', 'Sale', 182.01, 1000, 182010, 2000, '2022-01-03 18:00:00.0000000', 'AAPL.US');


-- Create an index on the Ticker column
CREATE INDEX idx_ticker ON dbo.DailyOHLCV(Ticker);

-- Create an index on ExchangeName for queries that filter by exchange
CREATE INDEX idx_exchangeName ON dbo.DailyOHLCV(ExchangeName);

-- Create an index on ExchangeType for queries that filter by exchange
CREATE INDEX idx_exchangeType ON dbo.DailyOHLCV(ExchangeType);

-- Select all data from Tickers table
SELECT * FROM DailyOHLCV;

--select all newsData
select * from newsEvents

-- Select all data from Tickers table
SELECT * FROM Tickers;

-- Select all data from InsiderTrades table
SELECT * FROM InsiderTrades;

-- Join Tickers and InsiderTrades to get detailed information about each trade
SELECT 
    i.Insider_Trading,
    i.Insider_Trading,
    i.Relationship,
    i.Shares,
    i.Ticker,
    t.StockName,
    t.ExchangeID
FROM 
    InsiderTrades i
JOIN 
    Tickers t ON i.Ticker = t.TickerSymbol;

-- Count the number of trades by each insider
SELECT 
    Insider_Trading,
    COUNT(*) AS NumberOfTrades
FROM 
    InsiderTrades
GROUP BY 
    Insider_Trading;

-- Get total shares traded by each insider
SELECT 
    Insider_Trading,
    SUM(Shares) AS TotalSharesTraded
FROM 
    InsiderTrades
GROUP BY 
    Insider_Trading;

-- Get total shares traded for each ticker symbol
SELECT 
    Ticker,
    SUM(Shares) AS TotalSharesTraded
FROM 
    InsiderTrades
GROUP BY 
    Ticker;

-- Get trades for a specific ticker symbol (e.g., AAPL)
SELECT 
    i.Insider_Trading,
    i.Relationship,
    i.Shares,
    i.TradeDate,
    i.Ticker,
    t.StockName,
    t.ExchangeID
FROM 
    InsiderTrades i
JOIN 
    Tickers t ON i.Ticker = t.TickerSymbol
WHERE 
    i.Ticker = 'AAPL.US';

-- Get trades made on a specific date
--Jan 1, 2022 (should be empty)
SELECT 
    i.Insider_Trading,
    i.Relationship,
    i.Shares,
    i.TradeDate,
    i.Ticker,
    t.StockName,
    t.ExchangeID
FROM 
    InsiderTrades i
JOIN 
    Tickers t 
ON i.Ticker = t.TickerSymbol
WHERE 
    i.TradeDate = '2022-01-01';


-- Get trades made on a specific date
--Jan 3, 2022 (should be have Tim's details)
SELECT 
    i.Insider_Trading,
    i.Relationship,
    i.Shares,
    i.TradeDate,
    i.Ticker,
    t.StockName,
    t.ExchangeID
FROM 
    InsiderTrades i
JOIN 
    Tickers t 
ON i.Ticker = t.TickerSymbol
WHERE 
    i.TradeDate = '2022-01-03';

--Insider Trading WHEN any NEWS/EVENT in Market
--with an investment advice
--so lets check trade date on-next day to see how the market reacts
-- Analyze impact of News on Stock Prices and compare with previous day's prices:
-- % change if > +2% then Bull-ish (BUY)
-- % change if < -2% then Bear-ish (SELL)
-- % change is between +2% and -2% then NEUTRAL (HOLD)

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
    i.[TradeDate]  AS InsiderTradingDate,
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
    AND d.TradeDate = i.[TradeDate];