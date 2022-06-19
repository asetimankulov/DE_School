# DE_School
## Project content
Directories:
* AIRFLOW contains dags and a log file
* SNOWFLAKE contains an sql-script with problems solving and final datasets.
Files:
* PIPELINE - pipeline of process
## TOP 10 rows of tables from tasks a-d

### 1.a. Pokémon and their types
|TYPE_NAME|TYPE_POKEMONS_CNT|NEXT_DIFF|PREV_DIFF|
| --- | --- | --- | --- |
|water	|164		|27 | |
|flying	|137	|27	|6|
|normal	|131	|6	|6|
|grass	|125	|6	|2|
|psychic	|123	|2	|27|
|bug	|96	|27	|4|
|electric	|92	|4	|3|
|rock	|89	|3	|1|
|fire	|88	|1	|3|
|poison	|85	|3	|2|

### 1.b. Pokémon and their moves
|MOVE_NAME|MOVE_POKEMONS_CNT|NEXT_DIFF|PREV_DIFF|
| --- | --- | --- | --- |
|snore	|1,070	|	|0|
|protect	|1,070	|0	|2|
|substitute	|1,068	|2	|1|
|rest	|1,067	|1	|2|
|round	|1,065	|2	|1|
|sleep-talk	|1,064	|1	|3|
|facade	|1,061	|3	|107|
|swagger	|954	|107	|4|
|toxic	|950	|4	|6|
|double-team	|944	|6	|0|

### 1.c. Rating Pokémon based on their stats
|POKEMON_NAME|RATING|
| --- | --- |
|eternatus-eternamax	|1,125|
|mewtwo-mega-x	|780|
|mewtwo-mega-y	|780|
|rayquaza-mega	|780|
|groudon-primal	|770|
|kyogre-primal	|770|
|necrozma-ultra	|754|
|arceus	|720|
|zacian-crowned	|720|
|zamazenta-crowned	|720|

### 1.d. Number of Pokémon by type and generation
|TYPE_NAME|'generation-i'|'generation-ii'|'generation-iii'|'generation-iv'|'generation-v'|'generation-vi'|'generation-vii'|'generation-viii'|
|---|---|---|---|---|---|---|---|---|
|grass|17|10|18|16|19|17|14|14|
|poison|42|5|5|8|8|2|8|7|
|bug|15|12|14|11|18|3|14|9|
|electric|27|11|5|7|17|3|9|13|
|psychic|25|11|28|10|16|13|12|13|
|fire|17|11|10|5|18|8|9|10|
|ice|11|5|8|7|13|4|1|9|
|ghost|7|2|8|9|10|15|13|9|
|water|38|18|32|14|20|13|15|16|
|dark|9|8|15|7|16|8|2|13|
