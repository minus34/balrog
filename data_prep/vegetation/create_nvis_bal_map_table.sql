
-- From the Geoscience Austrlia BAL toolkit:

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
where mvg_number in (23, 26, 29)
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

update bushfire.nvis6_lookup
set bal_number = NULL,
    bal_name = NULL
where mvg_number in (24, 25, 27, 28, 99)
;


-- check values are correct
select distinct mvg_number,
                mvg_name,
                bal_number,
                bal_name
from bushfire.nvis6_lookup
order by bal_number,
         mvg_number;


-- one off fix