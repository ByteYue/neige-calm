INSERT INTO areas (id,name,color,sort,created_at,updated_at) VALUES ('a','a','#000',0,1,1);
        INSERT INTO tracks (id,area_id,title,sort,created_at,updated_at) VALUES ('t','a','t',0,1,1);
        INSERT INTO cards (id,track_id,kind,sort,payload,created_at,updated_at,role)
          VALUES ('c','t','codex',0,'{}',1,1,'planner');
        INSERT INTO worker_sessions (id,track_id,provider,mode,contract,state,card_id,
          mcp_token_hash,thread_id,agent_session_id,handle_state_json,last_activity_ms,
          last_thread_status,queue_harvested_at_ms,last_turn_completed_ms,created_at_ms,updated_at_ms)
          VALUES ('s','t','claude','resumable','planner','idle','c','hash','thread','native',
                  '{"pending_queue":["hello"]}',7,'idle',8,9,1,2);
        INSERT INTO worker_sessions (id,track_id,provider,mode,contract,state,
          parent_session_id,requester_session_id,created_at_ms,updated_at_ms,completed_at_ms)
          VALUES ('child','t','codex','resumable','executor','exited','s','s',3,4,5);
        UPDATE worker_sessions SET parent_session_id = 's' WHERE id = 's';
        UPDATE cards SET session_id = 's' WHERE id = 'c';
        UPDATE tracks SET root_session_id = 's' WHERE id = 't';
        INSERT INTO worker_flow_items (card_id,worker_session_id,kind,payload,created_at_ms)
          VALUES ('c','s','text','{}',6);
