-- Table: public.test_batch

-- DROP TABLE IF EXISTS public.test_batch;

CREATE TABLE IF NOT EXISTS public.test_batch
(
    test_string character varying COLLATE pg_catalog."default",
    identifier character varying COLLATE pg_catalog."default",
    jurisdiction character varying COLLATE pg_catalog."default",
    id1 integer,
    date date,
    CONSTRAINT "unique" UNIQUE (identifier, id1)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.test_batch
    OWNER to test;