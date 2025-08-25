-- Session lengths by user_id
-- It assumes a session starts with the "session_started" event. Which is very flimsy :(

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
    
  FROM 
    session_events

  group by
    session
    , user_id
)

, coalesced as (
  select 
    session
    , user_id
    , coalesce(session_start, alternative_session_start) as session_start
    , coalesce(session_end, alternative_session_end) as session_end
  from
    session_gates
)

, sessions as (
  select 
    session
    , user_id
    , session_start
    , session_end
    , timestamp_diff(session_end, session_start, minute) as session_duration
  from 
    coalesced
  order by 
    session
    , user_id
)

select
  *
  -- , lag(session_end, 1) over (partition by user_id order by session_start) as end_of_previous_session -- just to see
  , timestamp_diff(session_start, lag(session_end, 1) over (partition by user_id order by session_start), minute) as time_between_sessions
from 
  sessions
order by
  session

