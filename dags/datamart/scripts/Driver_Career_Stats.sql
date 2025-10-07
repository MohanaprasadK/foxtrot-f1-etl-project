-- Drop existing table if exists (idempotent operation)
DROP TABLE IF EXISTS dm_driver_career_stats;

-- Create Driver Career Stats Datamart
CREATE TABLE dm_driver_career_stats AS
WITH driver_season_stats AS (
    SELECT 
        d.driverid,
        d.driverref,
        d.number AS driver_number,
        d.code AS driver_code,
        d.forename || ' ' || d.surname AS driver_name,
        d.nationality AS driver_nationality,
        r.year AS season,
        c.name AS constructor_name,
        c.nationality AS constructor_nationality,
        
        -- Race Counts (handle string positions with decimals)
        COUNT(DISTINCT res.raceid) AS races_entered,
        COUNT(DISTINCT CASE WHEN res.position::TEXT = '1.0' THEN res.raceid END) AS races_won,
        COUNT(DISTINCT CASE WHEN res.position::TEXT IN ('1.0','2.0','3.0') THEN res.raceid END) AS podiums,
        COUNT(DISTINCT CASE WHEN res.position::TEXT NOT IN ('1.0','2.0','3.0','4.0','5.0','6.0','7.0','8.0','9.0','10.0','11.0','12.0','13.0','14.0','15.0','16.0','17.0','18.0','19.0','20.0') THEN res.raceid END) AS dnfs,
        
        -- Points and Positions
        SUM(res.points) AS total_points,
        AVG(res.points) AS avg_points_per_race,
        AVG(CASE WHEN res.position::TEXT ~ '^[0-9]+\.[0-9]+$' THEN res.position::FLOAT END) AS avg_finishing_position,
        MIN(CASE WHEN res.position::TEXT ~ '^[0-9]+\.[0-9]+$' THEN res.position::FLOAT END) AS best_finish,
        
        -- Grid vs Finish Performance
        AVG(res.grid) AS avg_grid_position,
        AVG(
            CASE 
                WHEN res.position::TEXT ~ '^[0-9]+\.[0-9]+$' 
                THEN res.grid - res.position::FLOAT 
            END
        ) AS avg_position_gain,
        
        -- Fastest Laps
        COUNT(DISTINCT CASE WHEN res.fastestlap::TEXT = '1' THEN res.raceid END) AS fastest_laps
        
    FROM lake_results res
    JOIN lake_drivers d ON res.driverid = d.driverid
    JOIN lake_races r ON res.raceid = r.raceid
    JOIN lake_constructors c ON res.constructorid = c.constructorid
    GROUP BY 
        d.driverid, d.driverref, d.number, d.code, d.forename, d.surname, 
        d.nationality, r.year, c.name, c.nationality
),

career_totals AS (
    SELECT 
        driverid,
        COUNT(DISTINCT season) AS seasons_competed,
        MIN(season) AS first_season,
        MAX(season) AS last_season,
        SUM(races_entered) AS career_races,
        SUM(races_won) AS career_wins,
        SUM(podiums) AS career_podiums,
        SUM(total_points) AS career_points,
        SUM(fastest_laps) AS career_fastest_laps
    FROM driver_season_stats
    GROUP BY driverid
)

SELECT 
    ds.*,
    ct.seasons_competed,
    ct.first_season,
    ct.last_season,
    ct.career_races,
    ct.career_wins,
    ct.career_podiums,
    ct.career_points,
    ct.career_fastest_laps,
    
    -- Calculated Metrics (fixed ROUND function)
    ROUND((ct.career_wins * 100.0 / NULLIF(ct.career_races, 0))::NUMERIC, 2) AS win_percentage,
    ROUND((ct.career_podiums * 100.0 / NULLIF(ct.career_races, 0))::NUMERIC, 2) AS podium_percentage,
    ROUND(ds.avg_points_per_race::NUMERIC, 2) AS avg_points_per_race_rounded,
    ROUND(ds.avg_finishing_position::NUMERIC, 2) AS avg_finish_rounded
    
FROM driver_season_stats ds
JOIN career_totals ct ON ds.driverid = ct.driverid;

-- Create indexes for better query performance
CREATE INDEX idx_dm_driver_career_driverid ON dm_driver_career_stats(driverid);
CREATE INDEX idx_dm_driver_career_season ON dm_driver_career_stats(season);
CREATE INDEX idx_dm_driver_career_name ON dm_driver_career_stats(driver_name);
CREATE INDEX idx_dm_driver_career_nationality ON dm_driver_career_stats(driver_nationality);

-- Add primary key
ALTER TABLE dm_driver_career_stats ADD COLUMN id SERIAL PRIMARY KEY;