/*
Step 1. Environment setup.
Creation of database 'POKEMONS' and warehouse for queries.
*/
use role sysadmin;

create database POKEMONS;
use database POKEMONS;

create warehouse if not exists deployment_wh
with WAREHOUSE_SIZE='X-SMALL'
	MAX_CLUSTER_COUNT = 1
	MIN_CLUSTER_COUNT = 1;

use warehouse deployment_wh;

/*
Step 2. Staging schema.
Creation of staging schema, stage, data formats, tables, streams and pipes for load data.
*/ 
create schema POKEMONS.staging;
use schema POKEMONS.staging;

create stage sf_stage
  url='s3://de-school-snowflake/'
  credentials=(aws_key_id='AKIATZFZDSGK55BO7KCO' aws_secret_key='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');

create file format JSON_DATA
type = 'JSON'
STRIP_OUTER_ARRAY = TRUE;

create table stg_generation (json_data variant,
                            copied_at timestamp default current_timestamp(),
                            copied_by varchar default current_user()); 
create table stg_move (json_data variant,
                            copied_at timestamp default current_timestamp(),
                            copied_by varchar default current_user());
create table stg_pokemon (json_data variant,
                            copied_at timestamp default current_timestamp(),
                            copied_by varchar default current_user());
create table stg_type (json_data variant,
                            copied_at timestamp default current_timestamp(),
                            copied_by varchar default current_user());

create stream stg_generation_stream on table stg_generation;
create stream stg_move_stream on table stg_move;
create stream stg_pokemon_stream on table stg_pokemon;
create stream stg_type_stream on table stg_type;

create pipe generation_pipe auto_ingest=true as
    copy into POKEMONS.staging.stg_generation(json_data)
    from @sf_stage/snowpipe/Imankulov/
    file_format = (FORMAT_NAME = 'JSON_DATA')
    pattern='generation.json';

create pipe move_pipe auto_ingest=true as
    copy into POKEMONS.staging.stg_move(json_data)
    from @sf_stage/snowpipe/Imankulov/
    file_format = (FORMAT_NAME = 'JSON_DATA')
    pattern='move.json';

create pipe pokemon_pipe auto_ingest=true as
    copy into POKEMONS.staging.stg_pokemon(json_data)
    from @sf_stage/snowpipe/Imankulov/
    file_format = (FORMAT_NAME = 'JSON_DATA')
    pattern='pokemon.json';

create pipe type_pipe auto_ingest=true as
    copy into POKEMONS.staging.stg_type(json_data)
    from @sf_stage/snowpipe/Imankulov/
    file_format = (FORMAT_NAME = 'JSON_DATA')
    pattern='type.json';

/*
Step 3. Storage schema.
Creation a star storage schema with multiple dimension-tables and one fact-table.
*/ 
create schema POKEMONS.storage;
use schema POKEMONS.storage;

create table generations(
    generation_id number primary key,
    generation_name varchar);
    
create table species(
    species_id number primary key,
    species_name varchar);

create table moves(
    move_id number primary key,
    move_name varchar);

create table types(
    type_id number primary key,
    type_name varchar);

create table pokemons(
    pokemon_id number primary key,
    pokemon_name varchar);

create table stats(
    stat_id number primary key,
    stat_name varchar);
    
create table fact_table(
    pokemon_id number,
    species_id number,
    generation_id number,
    move_id number,
    type_id number,
    past_type_id number,
    past_types_generation_id number,
    stat_id number,
    stat_value number);
    
insert into generations
select generation.key as generation_id,
       parse_json(generation.value):name as generation_name
from staging.stg_generation src,
lateral flatten( input => src.JSON_DATA) generation;

insert into species
select parse_json(pokemon_species.value):id as species_id,
       parse_json(pokemon_species.value):name as species_name
from staging.stg_generation src,
lateral flatten( input => src.JSON_DATA) generation,
lateral flatten( input => generation.value:pokemon_species) pokemon_species;

insert into moves
select move.value:id::number as move_id,
       move.value:name::varchar as move_name
from staging.stg_move src,
lateral flatten( input => src.JSON_DATA:move) move;

insert into types
select type.value:id::number as type_id,
       type.value:name::varchar as type_name
from staging.stg_type src,
lateral flatten( input => src.JSON_DATA:type) type;

insert into pokemons
select pokemons.key::number as pokemon_id,
       pokemons.value:name::varchar as pokemon_name
from staging.stg_pokemon src,
lateral flatten( input => src.JSON_DATA) pokemons;

-- stats that at least one pokemon has
insert into stats
select distinct parse_json(pokemons_stats.value):id::number as stat_id,
       parse_json(pokemons_stats.value):name::varchar as stat_name
from staging.stg_pokemon src,
lateral flatten( input => src.JSON_DATA) pokemons,
lateral flatten( input => pokemons.value:stats) pokemons_stats;

insert into fact_table
with generation_species as (
    select generation.key::number as generation_id, parse_json(pokemon_species.value):id::number as species_id
    from staging.stg_generation src,
    lateral flatten( input => src.JSON_DATA) generation,
    lateral flatten( input => generation.value:pokemon_species) pokemon_species
),
pokemon_species as (
    select pokemons.key::number as pokemon_id,
           parse_json(pokemons.value:species):id::number as species_id
    from staging.stg_pokemon src,
    lateral flatten( input => src.JSON_DATA) pokemons
),
pokemon_moves as (
    select pokemons.key::number as pokemon_id,
           parse_json(pokemons_moves.value):id::number as move_id
    from staging.stg_pokemon src,
    lateral flatten( input => src.JSON_DATA) pokemons,
    lateral flatten( input => pokemons.value:moves) pokemons_moves
),
pokemon_types as (
    select pokemons.key::number as pokemon_id,
           parse_json(pokemons_types.value):id::number as type_id
    from staging.stg_pokemon src,
    lateral flatten( input => src.JSON_DATA) pokemons,
    lateral flatten( input => pokemons.value:types) pokemons_types
),
pokemon_past_types as (
    select pokemons.key::number as pokemon_id,
           parse_json(parse_json(pokemons_past_types.value):generation):id::number as past_types_generation_id,
           parse_json(pokemons_past_types_ids.value):id::number as past_type_id
    from staging.stg_pokemon src,
    lateral flatten( input => src.JSON_DATA) pokemons,
    lateral flatten( input => pokemons.value:past_types) pokemons_past_types,
    lateral flatten( input => pokemons_past_types.value:types) pokemons_past_types_ids
),
pokemon_stats as (
    select pokemons.key::number as pokemon_id,
           parse_json(pokemons_stats.value):id::number as stat_id,
           parse_json(pokemons_stats.value):base_stat::number as stat_value
    from staging.stg_pokemon src,
    lateral flatten( input => src.JSON_DATA) pokemons,
    lateral flatten( input => pokemons.value:stats) pokemons_stats
)
select p_s.pokemon_id, p_s.species_id, g_s.generation_id, p_m.move_id, p_t.type_id,
       p_p_t.past_type_id, p_p_t.past_types_generation_id, p_st.stat_id, p_st.stat_value
from pokemon_species p_s
full join generation_species g_s
    on p_s.species_id = g_s.species_id
full join pokemon_moves p_m
    on p_s.pokemon_id = p_m.pokemon_id
full join pokemon_types p_t
    on p_s.pokemon_id = p_t.pokemon_id
full join pokemon_past_types p_p_t
    on p_s.pokemon_id = p_p_t.pokemon_id
full join pokemon_stats p_st
    on p_s.pokemon_id = p_st.pokemon_id;

/*
Step 4. Data marts schema.
Creation a views to answer questions.
*/ 
create schema POKEMONS.DATA_MARTS;
use schema POKEMONS.DATA_MARTS;

--1.a. Pokémon and their types
create or replace view pokemon_types as
with pokemon_types as (
    select pokemon_id, type_id
    from storage.fact_table
    group by pokemon_id, type_id
),
pokemon_types_cnt as (
select t.type_name,
       count(pokemon_id) over(partition by t.type_id) as type_pokemons_cnt,
       row_number() over(partition by t.type_id order by t.type_id) as types_cnt
from pokemon_types p_t
full join storage.types t
    on p_t.type_id = t.type_id
order by type_pokemons_cnt desc 
),
next_prev_type as (
    select type_name, type_pokemons_cnt,
    dense_rank() over(order by type_pokemons_cnt) type_rank
    from pokemon_types_cnt
    where types_cnt = 1
    order by type_pokemons_cnt desc
)
select distinct n_p_t.type_name, n_p_t.type_pokemons_cnt,
       n_p_t_next.type_pokemons_cnt - n_p_t.type_pokemons_cnt as next_diff,
       n_p_t.type_pokemons_cnt - n_p_t_prev.type_pokemons_cnt as prev_diff
from next_prev_type n_p_t
left join next_prev_type n_p_t_prev
    on n_p_t.type_rank = n_p_t_prev.type_rank + 1
left join next_prev_type n_p_t_next
    on n_p_t.type_rank = n_p_t_next.type_rank - 1
order by n_p_t.type_pokemons_cnt desc

--1.b. Pokémon and their moves
create or replace view moves_popularity as 
with pokemon_moves as (
select pokemon_id, move_id
from storage.fact_table
group by pokemon_id, move_id
),
pokemon_moves_cnt as (
select m.move_id, m.move_name,
        count(p_m.pokemon_id) over(partition by m.move_id) as move_pokemons_cnt,
        row_number() over(partition by m.move_id order by m.move_id) as moves_cnt
from pokemon_moves p_m
full join storage.moves m
    on p_m.move_id = m.move_id
order by move_pokemons_cnt desc
),
next_prev_move as (
    select move_name, move_pokemons_cnt,
            lag(move_pokemons_cnt) over(order by move_pokemons_cnt desc) as next_move,
            lead(move_pokemons_cnt) over(order by move_pokemons_cnt desc) as prev_move
    from pokemon_moves_cnt
    where moves_cnt = 1
)
select move_name, move_pokemons_cnt,
next_move - move_pokemons_cnt as next_diff,
move_pokemons_cnt - prev_move as prev_diff
from next_prev_move
where move_pokemons_cnt > 0

--1.c. Rating Pokémon based on their stats.
create or replace view pokemons_rating as 
with pokemon_stats as (
select pokemon_id, stat_id, stat_value
from storage.fact_table
group by pokemon_id, stat_id, stat_value
),
pokemon_rating as (
select pokemon_id, sum(stat_value) as rating
from pokemon_stats
group by pokemon_id
)
select p.pokemon_name, p_r.rating
from pokemon_rating p_r
left join storage.pokemons p
    on p_r.pokemon_id = p.pokemon_id
order by rating desc

--1.d. Number of Pokémon by type and generation
declare
  generations varchar;
  select_statement varchar;
  c1 cursor for
  select * from storage.generations;
begin
    generations := '';
    for cur in c1 do
        case (generations)
            when '' then
                generations := '''' || cur.generation_name || '''';
            else
                generations := generations || ', ''' || cur.generation_name || '''';
        end;
    end for;
    
    select_statement := '
  create or replace view pokemon_types_generations as 
  with pokemon_types as (
      select pokemon_id,
             case
                when past_type_id is not null then past_types_generation_id + 1
                else generation_id
             end generation_id_act,
             type_id, generation_id, past_type_id, past_types_generation_id
      from storage.fact_table
        group by pokemon_id, generation_id, type_id, past_types_generation_id, past_type_id, generation_id_act
  ),
  pokemon_past_types as (
      select pokemon_id, generation_id, past_type_id
      from pokemon_types
      where past_type_id is not null
      --since it is necessary to track changes between generations, changes within one generation are excluded
      and past_types_generation_id <> generation_id
      group by pokemon_id, generation_id, past_type_id
  ),
  pokemon_types_union as (
      select pokemon_id, generation_id_act as generation_id, type_id
      from pokemon_types
      union all
      select pokemon_id, generation_id, past_type_id as type_id
      from pokemon_past_types
  ),
  pokemon_types_union_processed as (
      select pokemon_id, generation_id, type_id
      from pokemon_types_union
      group by pokemon_id, generation_id, type_id
  ),
  pokemon_types_generations as (
      select p_t.pokemon_id, t.type_name, g.generation_name
      from pokemon_types_union_processed p_t
      join storage.types t
          on p_t.type_id = t.type_id
      join storage.generations g
          on p_t.generation_id = g.generation_id
  )
  select * from pokemon_types_generations
  pivot(count(pokemon_id) for generation_name in ';
  
  execute immediate select_statement || '(' || generations || '))';
end;

/*
Step 5. Display result.
*/ 
--1.a. Pokémon and their types
select * from pokemon_types
--1.b. Pokémon and their moves
select * from moves_popularity
--1.c. Rating Pokémon based on their stats.
select * from pokemons_rating
--1.d. Number of Pokémon by type and generation
select * from pokemon_types_generations

