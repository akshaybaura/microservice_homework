create table postgres.public.user_status (userId varchar, createdAt varchar, status varchar, lastVisitedAt int, isBlacklisted boolean, ingested_at timestamp);

create table postgres.public.user_status_log (userId varchar, updatedon varchar, status varchar, ingested_at timestamp);

create table postgres.public.user_status_error_rec (error_rec varchar);
