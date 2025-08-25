-- Sessions - SQL brush-up

/* About session definition

It assumes a session starts with the "session_started" event. Which is very flimsy :(

For a better definition
- count between start and end
- add a timeout if its been too long
- group by other features e.g. platform

*/
with session_events as (
    select 
      *
      , concat(user_id, "_", countif(event = "session_started") over (partition by user_id order by timestamp )) as session
    from 
      `gcp-learning-joaopcoelho.philip_case.streaming_events` 
) 

, session_gates as (
  SELECT  
    session
    , user_id
    , min(if(event = "session_started", timestamp, null)) as session_start
    , min(timestamp) as alternative_session_start
    , max(if(event="session_ended", timestamp, null)) as session_end
    , max(timestamp) as alternative_session_end

    -- additional features
    , platform

    
  FROM 
    session_events

  group by
    session
    , user_id
    , platform

  -- For platform. We shouldn't need it if the session definition was done properly (1 session = 1 device)
  -- But for now I just wanna get 1 device value per session
  qualify row_number() over (partition by session order by count(*) desc) = 1
)

, coalesced as (
  select 
    session
    , user_id
    , platform
    , coalesce(session_start, alternative_session_start) as session_start
    , coalesce(session_end, alternative_session_end) as session_end
  from
    session_gates
)

, sessions as (
  select 
    session
    , user_id
    , platform

    -- 1. Session duration
    , session_start
    , session_end
    , timestamp_diff(session_end, session_start, minute) as session_duration

    -- 2. Time between sessions
    -- , lag(session_end, 1) over (partition by user_id order by session_start) as end_of_previous_session -- just to see
    , timestamp_diff(session_start, lag(session_end, 1) over (partition by user_id order by session_start), minute) as time_between_sessions

    
    
  from 
    coalesced
  order by 
    session
    , user_id
)

-- 3. Average session length per device

, devices as (
    select
      platform
      , count(*) as num_sessions
      , avg(session_duration) as avg_session_duration
    from 
      sessions
    where 
      session_duration > 0 -- exclude weird cases from average
    group by
      platform
    
)


-- Everything
select
  *  
from 
  --devices
  sessions


