-- Anhang 1_ Analyseskript SQL Abfrage 

-- Neue Tablle mit Anzahl Abrufdaten pro Webseite 
CREATE TABLE count_days_scrapes
AS
SELECT paper,
COUNT (DISTINCT date) AS "total count days scraped",
COUNT (DISTINCT date) * 100 / (SELECT COUNT(DISTINCT date) FROM wordcount)
AS percentage 
FROM wordcount 
GROUP BY paper
ORDER BY percentage DESC


--Neue Tabelle mit Summe der verwendeten Einzelwörter "Klimakleber" / "Klima-Kleber" pro Nachrichtenwebseite 
CREATE TABLE KK_paper
AS
SELECT paper AS paper_KK, sum(count)AS "sum_KK" 
FROM wordcount 
WHERE word like "%klimakleber" OR lower(word) = "klima-kleber"
GROUP BY paper_KK
ORDER BY "sum_KK"

--Neue Tabelle mit Summe des verwendeten Einzelwortes "Klimaprotest" pro Nachrichtenwebseite  
CREATE TABLE KP_paper
AS
SELECT paper AS paper_KP, sum(count)AS &quot;sum_KP&quot; 
FROM wordcount 
WHERE word like "klimaprotest"
GROUP BY paper_KP
ORDER BY "sum_KP"

--Neue Tabelle mit Summe der verwendeten Einzelwörter "Klimakleber" / "Klima-Kleber" und "Klimaprotest" pro Nachrichtenwebseite 
CREATE TABLE KK_KP_paper
AS
SELECT 
	paper_KP AS paper,
	sum_KK,
	sum_KP
FROM KP_paper
INNER JOIN KK_paper ON paper_KK = paper_KP
ORDER BY "sum_KK"
	
--Neue Tabelle mit Verwendung der Einzelwörter "Klimakleber" / "Klima-Kleber" über den Zeitraum der Datenerfassung, gruppiert nach Monaten

CREATE TABLE KK_month
AS
SELECT
  STRFTIME('%Y-%m', date) AS month_KK,
  sum(count)AS "sum_month_KK"
FROM wordcount 
WHERE word like "%klimakleber"; OR lower(word) = "klima-kleber"
GROUP BY
  STRFTIME('%Y-%m', date)
ORDER by month_KK

--Neue Tabelle mit Verwendung des Einzelwortes "Klimaprotest" über den Zeitraum der Datenerfassung, gruppiert nach Monaten

CREATE TABLE KP_month
AS
SELECT
  STRFTIME('%Y-%m', date) AS month_KP,
  sum(count)AS "sum_month"
FROM wordcount 
WHERE word like "%klimaprotest%"
GROUP BY
  STRFTIME('%Y-%m', date)
ORDER by month_KP

--Neue Tabelle mit Verwendung der Einzelwörter "Klimakleber" / "Klima-Kleber" und "Klimaprotest" über den Zeitraum der Datenerfassung, gruppiert nach Monaten
CREATE TABLE KK_KP_month
AS
SELECT 
	month_KP AS publication_month,
	sum_month_KK,
	sum_month_KP
FROM KP_month
INNER JOIN KK_month ON month_KK = month_KP
ORDER BY publication_month 


