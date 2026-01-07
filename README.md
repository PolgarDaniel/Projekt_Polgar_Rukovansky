# **ELT proces datasetu Soccer Fixtures (OPTA)**

Tento projekt predstavuje implementáciu **ELT procesu v Snowflake** a návrh **dátového skladu vo formáte Star Schema** pre analytiku futbalových zápasov. Dataset obsahuje zápasy, tímy, súťaže, sezóny a štadióny, vrátane výsledkov a dátumov.

Projekt umožňuje:
- sledovanie výkonu tímov v sezónach,
- analýzu gólovej produktivity,
- porovnanie zápasov v rámci kôl,
- tvorbu historických reportov a BI dashboardov.

---

## **Popis témy, dát a účelu analýzy**

### Prečo sme si vybrali dataset
Dataset **Soccer Fixtures & Results (OPTA)** bol zvolený preto, že:
- ide o **reálne športové dáta** vhodné na analýzu výsledkov zápasov,
- obsahuje **časové a transakčné dáta**, ideálne pre dimenzionálny model,
- umožňuje demonštrovať využitie **window functions** vo faktových tabuľkách,
- podporuje tvorbu **reportov pre športovú analytiku a BI**.

### Biznis proces, ktorý dáta podporujú
- Sledovanie výsledkov zápasov a výkonu tímov v sezónach.
- Analýza trendov gólovej produktivity.
- Porovnanie zápasov podľa kôl a súťaží.
- Historické sledovanie zmien tímov a lokalít.

### Typy údajov v datasete
- **Identifikačné údaje:** UUID zápasov, tímov, súťaží, štadiónov.
- **Časové údaje:** dátum a čas zápasu.
- **Textové údaje:** názvy tímov, súťaží, štadiónov.
- **Číselné údaje:** skóre domácich a hostí.
- **Geografické údaje:** krajina, región.

### Na čo bude analýza zameraná
- Vývoj skóre a gólov v sezóne.
- Porovnanie zápasov v rámci kola a súťaže.
- Sledovanie výkonu tímov a súťaží historicky.

---

### Zdrojová tabuľka

#### FIXTURES
- Obsahuje všetky zápasy vrátane: dátumu, času, kolo, domáceho a hosťujúceho tímu, skóre, súťaže, sezóny a štadiónu.
---
### **ERD diagram**

Pôvodná dátová štruktúra (entitno-relačný diagram):

<p align="center">
  <img src="img/ERD_dia.png" alt="ERD Schema">
  <br>
  <em>Obrázok 1: ERD diagram zdrojového datasetu</em>
</p>

---
# **Návrh dimenzionálneho modelu**

Navrhnutá je **Schéma hviezdy**, pozostávajúca z jednej faktovej tabuľky a 5 dimenzií.

### Faktová tabuľka: `FACT_MATCH_RESULTS`
- **Primárny kľúč:** `match_fact_id`
- **Cudzie kľúče:** `date_id`, `home_team_id`, `away_team_id`, `competition_id`, `venue_id`, `season_id`
- **Hlavné metriky:**
  - `home_score`, `away_score`
  - `goal_difference`
  - `cumulative_goals_season`
  - `match_rank_in_round` 

### Dimenzie
- **`DIM_DATE`**
  - Obsah: dátum, deň, mesiac, rok, názov dňa
  - Vzťah: `date_id` vo faktovej tabuľke
  - Typ SCD: **Typ 0**

- **`DIM_TEAM`**
  - Obsah: názov tímu, skratka, časová platnosť (`valid_from`, `valid_to`, `is_current`)
  - Vzťah: `home_team_id`, `away_team_id`
  - Typ SCD: **Typ 2**

- **`DIM_COMPETITION`**
  - Obsah: názov súťaže, krajina, región
  - Vzťah: `competition_id`
  - Typ SCD: **Typ 0**

- **`DIM_LOCATION`**
  - Obsah: názov štadióna, krajina, región, časová platnosť
  - Vzťah: `venue_id`
  - Typ SCD: **Typ 2**

- **`DIM_SEASON`**
  - Obsah: názov sezóny, identifikátor
  - Vzťah: `season_id`
  - Typ SCD: **Typ 0**

---

### Schéma hviezdy

<p align="center">
  <img src="img/STAR_dia.png" alt="Star Schema">
  <br>
  <em>Obrázok 2: Schéma hviezdy</em>
</p>

---
# **ELT proces datasetu**

Tento dokument popisuje jednotlivé kroky ELT procesu pre dataset **Soccer Fixtures** zo **Snowflake Marketplace**.  
Cieľom je pripraviť dáta pre analytiku futbalových zápasov pomocou dimenzionálneho modelu schémy hviezdy.

---

## **Extract**

- **Zdroj dát:** Snowflake Marketplace  
- **Databáza a schéma:** `OPTA_DATA_SOCCER_SCHEDULE_AND_RESULTS_DATA__SAMPLE.SOCCER`  
- **Účel:** importovať surové dáta a uložiť ich do staging tabuľky
  
Vytvorili sme staging tabuľku, ktorá izoluje surové dáta pre ďalšie transformácie.
Zachovávame pôvodné stavy, aby bolo možné overiť integritu a porovnať s čistenými dátami.
#### Príklad kódu:
```sql
CREATE OR REPLACE TABLE FIXTURES_STAGING AS
SELECT *
FROM OPTA_DATA_SOCCER_SCHEDULE_AND_RESULTS_DATA__SAMPLE.SOCCER.FIXTURES;
```
## **Deduplikácia a čistenie dát**
```sql
CREATE OR REPLACE TABLE FIXTURES_STAGING_CLEAN AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY GAME_UUID ORDER BY DATE_TIME DESC) AS rn
    FROM FIXTURES_STAGING
)
WHERE rn = 1;
```
Zachováva sa len jeden záznam pre každý zápas (najnovší podľa DATE_TIME)
Odstraňujeme duplicity a nekonzistentné záznamy.
## **Typové konverzie**
```sql
CREATE OR REPLACE TABLE FIXTURES_STAGING_TYPED AS
SELECT
    * EXCLUDE (DATE_TIME, HOME_SCORE, AWAY_SCORE),
    CAST(DATE_TIME AS TIMESTAMP_NTZ) AS match_datetime,
    CAST(HOME_SCORE AS NUMBER) AS home_score,
    CAST(AWAY_SCORE AS NUMBER) AS away_score
FROM FIXTURES_STAGING_CLEAN;
```
Prevedenie dát do správnych dátových typov
Príprava na naplnenie dimenzií a faktovej tabuľky
## **Tvorba dimenzií**
### **DIM_DATE**
```sql
CREATE OR REPLACE TABLE DIM_DATE AS
SELECT DISTINCT
    TO_DATE(match_datetime) AS date_id,
    TO_DATE(match_datetime) AS full_date,
    DAY(match_datetime) AS day,
    MONTH(match_datetime) AS month,
    YEAR(match_datetime) AS year,
    DAYNAME(match_datetime) AS weekday
FROM FIXTURES_STAGING_TYPED;
```
Typ SCD: Typ 0 (nemenná)
Obsahuje dátumové informácie potrebné vo faktovej tabuľke
### **DIM_TEAM**
```sql
CREATE OR REPLACE TABLE DIM_TEAM AS
SELECT
    UUID_STRING() AS team_id,
    team_uuid,
    team_name,
    team_short,
    CURRENT_DATE AS valid_from,
    '9999-12-31' AS valid_to,
    TRUE AS is_current
FROM (
    SELECT HOME_UUID AS team_uuid,
           HOME AS team_name,
           HOME_SHORT AS team_short
    FROM FIXTURES_STAGING_TYPED
    UNION
    SELECT AWAY_UUID,
           AWAY,
           AWAY_SHORT
    FROM FIXTURES_STAGING_TYPED
);
```
Typ SCD: Typ 2 (historická platnosť tímov)
### **DIM_COMPETITION**
```sql
CREATE OR REPLACE TABLE DIM_COMPETITION AS
SELECT DISTINCT
    COMPETITION_UUID AS competition_id,
    COMPETITION AS competition_name,
    COUNTRY,
    REGION
FROM FIXTURES_STAGING_TYPED;
```
Typ SCD: Typ 0 (nemenná)
### **DIM_LOCATION**
```sql
CREATE OR REPLACE TABLE DIM_LOCATION AS
SELECT DISTINCT
    VENUE_UUID AS venue_id,
    VENUE AS venue_name,
    COUNTRY,
    COUNTRY_CODE,
    REGION,
    CURRENT_DATE AS valid_from,
    '9999-12-31' AS valid_to
FROM FIXTURES_STAGING_TYPED;
```
Typ SCD: Typ 2 (historická platnosť)
### **DIM_SEASON**
```sql
CREATE OR REPLACE TABLE DIM_SEASON AS
SELECT DISTINCT
    SEASON_UUID AS season_id,
    SEASON AS season_name
FROM FIXTURES_STAGING_TYPED;
```
Typ SCD: Typ 0 (nemenná)
### **Naplnenie faktovej tabuľky pomocou dimenzií a staging tabuliek**
```sql
CREATE OR REPLACE TABLE FACT_MATCH_RESULTS AS
SELECT
    UUID_STRING() AS match_fact_id,
    d.date_id,
    ht.team_id AS home_team_id,
    at.team_id AS away_team_id,
    c.competition_id,
    v.venue_id,
    s.season_id,

    m.home_score,
    m.away_score,
    (m.home_score - m.away_score) AS goal_difference,
    
    SUM(m.home_score + m.away_score)
        OVER (
            PARTITION BY s.season_id
            ORDER BY m.match_datetime
        ) AS cumulative_goals_season,

    RANK()
        OVER (
            PARTITION BY m.ROUND
            ORDER BY (m.home_score + m.away_score) DESC
        ) AS match_rank_in_round

FROM FIXTURES_STAGING_TYPED m
JOIN DIM_DATE d ON TO_DATE(m.match_datetime) = d.date_id
JOIN DIM_TEAM ht ON m.HOME_UUID = ht.team_uuid AND ht.is_current = TRUE
JOIN DIM_TEAM at ON m.AWAY_UUID = at.team_uuid AND at.is_current = TRUE
JOIN DIM_COMPETITION c ON m.COMPETITION_UUID = c.competition_id
JOIN DIM_LOCATION v ON m.VENUE_UUID = v.venue_id
JOIN DIM_SEASON s ON m.SEASON_UUID = s.season_id;
```
Použitie window functions:
- `SUM()` `OVER()` pre kumulatívne góly v sezóne
- `RANK()` `OVER()` pre poradie zápasov v kole

# **Vizualizácia dát – Soccer Fixtures**

V tejto sekcii prezentujeme vybrané vizualizácie na základe dát z FACT_MATCH_RESULTS a dimenzií. Každý graf odpovedá na dôležité otázky o zápasoch, tímoch a góloch.
<p align="center">
  <img src="img/grafy.png" alt="ERD Schema">
  <br>
  <em>Obrázok 3: Dashboard</em>
</p>

## GRAF 1: Rozdelenie výsledkov futbalových zápasov
Ukazuje zastúpenie domácich výhier, výhier hosťov a remíz.
Pomáha analyzovať, či domáci tím má štatistickú výhodu.
#### SQL:
```sql
SELECT
    CASE
        WHEN home_score > away_score THEN 'Home Win'
        WHEN home_score < away_score THEN 'Away Win'
        ELSE 'Draw'
    END AS match_result,
    COUNT(*) AS match_count
FROM FACT_MATCH_RESULTS
GROUP BY match_result;
```
## GRAF 2: Vývoj kumulatívneho počtu gólov v priebehu sezóny
Zobrazuje, ako sa kumulatívne góly vyvíjajú počas sezóny.
Umožňuje identifikovať obdobia so zvýšeným počtom gólov.
#### SQL:
```sql
SELECT
    d.full_date,
    f.cumulative_goals_season
FROM FACT_MATCH_RESULTS f
JOIN DIM_DATE d ON f.date_id = d.date_id
ORDER BY d.full_date;
```
## GRAF 3: Najúspešnejšie domáce tímy
Zobrazuje 10 tímov s najvyšším počtom domácich víťazstiev.
Pomáha identifikovať najsilnejšie tímy doma.
#### SQL:
```sql
SELECT
    ht.team_name AS home_team,
    COUNT(*) AS home_wins
FROM FACT_MATCH_RESULTS f
JOIN DIM_TEAM ht ON f.home_team_id = ht.team_id
WHERE f.home_score > f.away_score
GROUP BY ht.team_name
ORDER BY home_wins DESC
LIMIT 10;
```
## GRAF 4: Priemerný počet gólov domácich a hosťov
Porovnáva priemerný počet gólov domácich a hosťov.
Pomáha posúdiť výkonnosť tímov doma a vonku
#### SQL:
```sql
SELECT
    'Home Goals' AS goal_type,
    AVG(home_score) AS avg_goals
FROM FACT_MATCH_RESULTS
UNION ALL
SELECT
    'Away Goals' AS goal_type,
    AVG(away_score) AS avg_goals
FROM FACT_MATCH_RESULTS;
```
## GRAF 5: Rozdelenie výsledkov zápasov podľa štadióna
Ukazuje, ako sa výsledky zápasov líšia podľa štadióna.
Pomáha identifikovať štadióny, kde domáci tím hrá lepšie alebo horšie.
#### SQL:
```sql
SELECT
    v.venue_name AS stadium_name,
    CASE
        WHEN f.home_score > f.away_score THEN 'Home Win'
        WHEN f.home_score < f.away_score THEN 'Away Win'
        ELSE 'Draw'
    END AS match_result,
    COUNT(*) AS count_result
FROM FACT_MATCH_RESULTS f
JOIN DIM_LOCATION v ON f.venue_id = v.venue_id
GROUP BY v.venue_name, match_result
ORDER BY stadium_name, match_result;
```
Autori: Dániel Polgár, Ármin Rukovánsky
