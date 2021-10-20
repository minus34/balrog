
-- From the Geoscience Australia BAL toolkit:

-- There are many classes in the original vegetation. To derive the
-- target vegetation classes described in the Standard (1: Forest, 2:
-- Woodland, 3: Shrubland, 4: Scrub, 5: Mallee/Mulga, 6: Rainforest,
-- 7: Grassland/Tussock moorland.), the remap string is defined as:
-- "1 6;2 1;3 1;4 1;5 13 2;14 5;15 18 3;19 22 7;23 4;24 25 NODATA;26
-- 4;27 28 NODATA;29 4;30 1;31 2;32 5;99 NODATA".

-- add BAL columns to NVIS lookup table
ALTER TABLE bushfire.nvis6_lookup ADD COLUMN IF NOT EXISTS bal_number smallint;
ALTER TABLE bushfire.nvis6_lookup ADD COLUMN IF NOT EXISTS bal_name name;

-- add mappings
update bushfire.nvis6_lookup
    set bal_number = 1,
        bal_name = 'forest'
where mvg_number in (2, 3, 4, 30)
;

update bushfire.nvis6_lookup
set bal_number = 2,
    bal_name = 'woodland'
where mvg_number in (5, 6, 7, 8, 9, 10, 11, 12, 13, 31)
;

update bushfire.nvis6_lookup
set bal_number = 3,
    bal_name = 'shrubland'
where mvg_number in (15, 16, 17, 18)
;

update bushfire.nvis6_lookup
set bal_number = 4,
    bal_name = 'scrub'
where mvg_number in (26, 29)
;

update bushfire.nvis6_lookup
set bal_number = 5,
    bal_name = 'mallee or mulga'
where mvg_number in (14, 32)
;

update bushfire.nvis6_lookup
set bal_number = 6,
    bal_name = 'rainforest'
where mvg_number = 1
;

update bushfire.nvis6_lookup
set bal_number = 7,
    bal_name = 'grassland or tussock moorland'
where mvg_number in (19, 20, 21, 22)
;

-- set these to values so we can see inthe main table if there are missing values (NULLS)
update bushfire.nvis6_lookup
set bal_number = -9999,
    bal_name = 'not applicable'
where mvg_number in (23, 24, 25, 27, 28, 99)
;

analyse bushfire.nvis6_lookup;

-- check values are correct
select distinct mvg_number,
                mvg_name,
                bal_number,
                bal_name
from bushfire.nvis6_lookup
order by bal_number,
         mvg_number;


-- -- one off fix for main table
-- ALTER TABLE bushfire.nvis6_exploded ADD COLUMN IF NOT EXISTS bal_number smallint;
-- ALTER TABLE bushfire.nvis6_exploded ADD COLUMN IF NOT EXISTS bal_name name;

-- ALTER SYSTEM SET max_parallel_workers = 8;
-- ALTER SYSTEM SET max_parallel_workers_per_gather = 8;
-- ALTER SYSTEM SET shared_buffers = '8GB';
-- ALTER SYSTEM SET work_mem = '4GB';
-- ALTER SYSTEM SET wal_buffers = '512MB';
-- ALTER SYSTEM SET max_wal_size = '2GB';
-- ALTER SYSTEM SET wal_level = 'minimal';
-- ALTER SYSTEM SET max_wal_senders = 0;
-- ALTER SYSTEM SET archive_mode = 'off';
-- ALTER SYSTEM SET fsync = 'off';
-- ALTER SYSTEM SET full_page_writes = 'off';
-- ALTER SYSTEM SET synchronous_commit = 'off';

-- --  9,408,852 rows affected in 12 m 0 s 675 ms
-- with lkp as (
--     select distinct mvg_number,
--                     bal_number,
--                     bal_name
--     from bushfire.nvis6_lookup
-- )
-- update bushfire.nvis6_exploded as veg
--     set bal_number = lkp.bal_number,
--         bal_name = lkp.bal_name
-- from lkp
-- where veg.veg_group = lkp.mvg_number
-- ;
-- analyse bushfire.nvis6_exploded;
--
-- -- fix mangrove error (was assigned to 'scrub')
-- update bushfire.nvis6_exploded as veg
--     set bal_number = -9999,
--         bal_name = 'not applicable'
-- where veg_group = 23;
--
-- vacuum analyse bushfire.nvis6_exploded;