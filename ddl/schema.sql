--
-- PostgreSQL database dump
--

\restrict WldfAKm7TQwQ6HYsfnvf2hWhXWd7NatDcWQxtA9GvcPWYhMVLSobZUfLloJqpSF

-- Dumped from database version 17.6
-- Dumped by pg_dump version 17.9 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA public;


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- Name: match_restaurant_by_embedding(public.vector, double precision, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.match_restaurant_by_embedding(query_embedding public.vector, match_threshold double precision, match_count integer) RETURNS TABLE(place_id text, similarity double precision)
    LANGUAGE sql STABLE
    AS $$
    SELECT place_id, 1 - (embedding <=> query_embedding) AS similarity
    FROM silver_restaurants
    WHERE embedding IS NOT NULL
      AND 1 - (embedding <=> query_embedding) >= match_threshold
    ORDER BY embedding <=> query_embedding
    LIMIT match_count;
$$;


--
-- Name: set_updated_at(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: bronze_ig_comments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_ig_comments (
    comment_id text NOT NULL,
    post_id text,
    restaurant_id text,
    text text,
    owner_username text,
    owner_id text,
    owner_full_name text,
    owner_is_verified boolean,
    owner_is_private boolean,
    owner_latest_reel bigint,
    likes integer,
    replies_count integer,
    is_reply boolean DEFAULT false,
    parent_comment_id text,
    is_restaurant_account boolean,
    posted_at timestamp with time zone,
    fetched_at timestamp with time zone DEFAULT now()
);


--
-- Name: bronze_ig_hashtag_posts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_ig_hashtag_posts (
    post_id text NOT NULL,
    hashtag_queried text NOT NULL,
    owner_username text,
    owner_id text,
    caption text,
    hashtags text[],
    likes integer,
    comments_count integer,
    location_name text,
    posted_at timestamp with time zone,
    fetched_at timestamp with time zone DEFAULT now()
);


--
-- Name: bronze_ig_posts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_ig_posts (
    post_id text NOT NULL,
    restaurant_id text,
    ig_handle text,
    shortcode text,
    type text,
    caption text,
    hashtags text[],
    mentions text[],
    tagged_users jsonb,
    likes integer,
    comments_count integer,
    is_comments_disabled boolean,
    video_url text,
    display_url text,
    location_name text,
    location_id text,
    posted_at timestamp with time zone,
    fetched_at timestamp with time zone DEFAULT now()
);


--
-- Name: bronze_ig_profiles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_ig_profiles (
    restaurant_id text NOT NULL,
    ig_handle text,
    followers integer,
    following integer,
    verified boolean,
    business_account boolean,
    category text,
    bio text,
    website text,
    post_count_returned integer,
    avg_likes numeric,
    avg_comments numeric,
    snapshot_date date NOT NULL,
    fetched_at timestamp with time zone DEFAULT now()
);


--
-- Name: bronze_search_rankings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_search_rankings (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    place_id text NOT NULL,
    query text NOT NULL,
    "position" integer NOT NULL,
    scraped_at timestamp with time zone DEFAULT now() NOT NULL,
    scraped_date date DEFAULT CURRENT_DATE NOT NULL,
    rating numeric,
    review_count integer
);


--
-- Name: bronze_serp; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_serp (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    query text NOT NULL,
    query_type text NOT NULL,
    place_id text,
    raw_json jsonb NOT NULL,
    fetched_at timestamp without time zone DEFAULT now()
);


--
-- Name: bronze_social_posts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_social_posts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    platform text NOT NULL,
    post_id text NOT NULL,
    query text,
    author_handle text,
    caption text,
    hashtags text[],
    likes integer,
    comments_count integer,
    shares integer,
    plays integer,
    location_name text,
    posted_at timestamp with time zone,
    fetched_at timestamp with time zone DEFAULT now(),
    processed boolean DEFAULT false
);


--
-- Name: bronze_tiktok_comments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_tiktok_comments (
    comment_id text NOT NULL,
    post_id text,
    restaurant_id text,
    text text,
    owner_username text,
    owner_id text,
    likes integer,
    replies_count integer,
    is_reply boolean DEFAULT false,
    parent_comment_id text,
    is_restaurant_account boolean,
    posted_at timestamp with time zone,
    fetched_at timestamp with time zone DEFAULT now()
);


--
-- Name: bronze_tiktok_posts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_tiktok_posts (
    post_id text NOT NULL,
    restaurant_id text,
    tiktok_handle text,
    text text,
    hashtags text[],
    location_name text,
    location_city text,
    location_address text,
    likes integer,
    shares integer,
    plays integer,
    collects integer,
    comments_count integer,
    is_sponsored boolean,
    is_slideshow boolean,
    has_transcription boolean,
    web_video_url text,
    created_at timestamp with time zone,
    fetched_at timestamp with time zone DEFAULT now()
);


--
-- Name: bronze_tiktok_profiles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_tiktok_profiles (
    restaurant_id text NOT NULL,
    tiktok_handle text,
    fans integer,
    following integer,
    verified boolean,
    heart_total bigint,
    video_count integer,
    signature text,
    bio_link text,
    snapshot_date date NOT NULL,
    fetched_at timestamp with time zone DEFAULT now()
);


--
-- Name: bronze_wolt; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bronze_wolt (
    wolt_id text NOT NULL,
    wolt_slug text,
    raw_json jsonb,
    fetched_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    wolt_name text
);


--
-- Name: competitors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.competitors (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    restaurant_id uuid,
    name text NOT NULL,
    place_id text,
    google_id text,
    rating numeric(2,1),
    total_reviews integer,
    cuisine text,
    address text,
    distance_km numeric(4,2),
    score integer,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: contact_enrichments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.contact_enrichments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    raw_ingestion_id uuid,
    slug text,
    website text,
    email text,
    phone text,
    instagram text,
    facebook text,
    tiktok text,
    twitter text,
    linkedin text,
    raw_response jsonb,
    scraped_at timestamp with time zone DEFAULT now(),
    place_id text,
    scrape_date date
);


--
-- Name: opportunities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.opportunities (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    restaurant_id uuid,
    opportunity_id text NOT NULL,
    title text NOT NULL,
    category text,
    problem text,
    action text,
    impact text,
    effort text,
    estimated_result text,
    cta text,
    status text DEFAULT 'proposed'::text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: raw_ingestions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.raw_ingestions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    source text NOT NULL,
    query text NOT NULL,
    slug text NOT NULL,
    raw_data jsonb NOT NULL,
    ingested_at timestamp with time zone DEFAULT now(),
    place_id text
);


--
-- Name: report_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.report_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    restaurant_id uuid,
    event_type text NOT NULL,
    properties jsonb,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: restaurant_pipeline; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.restaurant_pipeline (
    place_id text NOT NULL,
    tier integer DEFAULT 1 NOT NULL,
    tier_source text DEFAULT 'auto_score'::text NOT NULL,
    query_appearances integer,
    score_band text,
    notes text,
    reviews_last_enriched_at timestamp with time zone,
    social_last_enriched_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    sales_score numeric,
    sales_score_updated_at timestamp with time zone
);


--
-- Name: restaurant_social_handles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.restaurant_social_handles (
    restaurant_id text NOT NULL,
    ig_handle text,
    tiktok_handle text,
    added_by text DEFAULT 'manual'::text,
    added_at timestamp with time zone DEFAULT now()
);


--
-- Name: restaurants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.restaurants (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    slug text NOT NULL,
    name text NOT NULL,
    cuisine text,
    city text,
    address text,
    place_id text,
    google_id text,
    rating numeric(2,1),
    total_reviews integer,
    reviews_per_score jsonb,
    killer_insight text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: reviews; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.reviews (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    restaurant_id uuid,
    google_review_id text,
    author_name text,
    author_id text,
    review_text text,
    rating integer,
    review_date timestamp with time zone,
    owner_answer text,
    owner_answer_date timestamp with time zone,
    likes integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    scraped_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: silver_labels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.silver_labels (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    place_id text,
    label_type text NOT NULL,
    label_value text NOT NULL,
    source text
);


--
-- Name: silver_restaurants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.silver_restaurants (
    raw_ingestion_id uuid,
    slug text,
    place_id text,
    name text,
    website text,
    phone text,
    rating double precision,
    review_count integer,
    ingested_at timestamp with time zone,
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    embedding public.vector(512)
);


--
-- Name: silver_search_visibility; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.silver_search_visibility (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    place_id text NOT NULL,
    query text NOT NULL,
    query_type text,
    organic_position integer,
    in_local_pack boolean DEFAULT false,
    local_pack_position integer,
    has_knowledge_panel boolean DEFAULT false,
    knowledge_panel_rating numeric,
    knowledge_panel_reviews integer,
    total_results_estimate integer,
    fetched_at timestamp with time zone,
    fetched_date date
);


--
-- Name: silver_social_mentions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.silver_social_mentions (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    bronze_post_id uuid,
    platform text,
    restaurant_name text,
    neighbourhood text,
    cuisine text,
    social_handle text,
    is_restaurant_mention boolean DEFAULT false,
    place_id text,
    match_method text,
    match_status text,
    post_likes integer,
    post_plays integer,
    post_shares integer,
    post_comments integer,
    created_at timestamp with time zone DEFAULT now(),
    confidence numeric,
    address text,
    CONSTRAINT silver_social_mentions_confidence_check CHECK (((confidence >= (0)::numeric) AND (confidence <= (1)::numeric))),
    CONSTRAINT silver_social_mentions_match_status_check CHECK ((match_status = ANY (ARRAY['matched'::text, 'needs_review'::text, 'new_discovery'::text, 'not_a_restaurant'::text, 'unresolved'::text])))
);


--
-- Name: target_accounts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.target_accounts (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    place_id text,
    name text NOT NULL,
    added_by text NOT NULL,
    source_query text,
    source_position integer,
    offmenu_score numeric,
    added_at timestamp without time zone DEFAULT now(),
    google_place_id text
);


--
-- Name: trends_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trends_data (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    query text NOT NULL,
    geo text DEFAULT 'DE-BE'::text,
    period text,
    average_interest integer,
    trend_direction text,
    trend_pct double precision,
    peak_date text,
    peak_value integer,
    timeline_data jsonb,
    scraped_at timestamp with time zone DEFAULT now()
);


--
-- Name: tripadvisor_enrichments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tripadvisor_enrichments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    raw_ingestion_id uuid,
    slug text,
    ta_place_id text,
    ta_url text,
    ta_name text,
    ta_rating double precision,
    ta_review_count integer,
    ta_ranking_position integer,
    ta_ranking_total integer,
    ta_ranking_percentile double precision,
    ta_description text,
    ta_price_range text,
    ta_cuisine text[],
    ta_has_award boolean,
    google_review_count integer,
    ta_to_google_ratio double precision,
    tourist_review_ratio double precision,
    is_tourist_venue boolean,
    reviews_sample jsonb,
    raw_response jsonb,
    scraped_at timestamp with time zone DEFAULT now(),
    error_message text
);


--
-- Name: tripadvisor_raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tripadvisor_raw (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    raw_ingestion_id uuid,
    slug text NOT NULL,
    restaurant_name text,
    search_query text NOT NULL,
    matched_place_id text,
    matched_title text,
    match_confidence text,
    raw_search_response jsonb,
    raw_place_response jsonb,
    error_message text,
    scraped_at timestamp with time zone DEFAULT now()
);


--
-- Name: wolt_enrichments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wolt_enrichments (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    place_id text,
    wolt_id text,
    wolt_slug text,
    wolt_name text,
    wolt_score numeric,
    wolt_rating_volume integer,
    wolt_price_range integer,
    wolt_tags text[],
    wolt_estimate_range text,
    wolt_delivers boolean,
    wolt_online boolean,
    wolt_exclusive boolean,
    wolt_badge text,
    wolt_preview_items jsonb,
    raw_response jsonb,
    enriched_at timestamp without time zone DEFAULT now()
);


--
-- Name: bronze_ig_comments bronze_ig_comments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_ig_comments
    ADD CONSTRAINT bronze_ig_comments_pkey PRIMARY KEY (comment_id);


--
-- Name: bronze_ig_hashtag_posts bronze_ig_hashtag_posts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_ig_hashtag_posts
    ADD CONSTRAINT bronze_ig_hashtag_posts_pkey PRIMARY KEY (post_id, hashtag_queried);


--
-- Name: bronze_ig_posts bronze_ig_posts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_ig_posts
    ADD CONSTRAINT bronze_ig_posts_pkey PRIMARY KEY (post_id);


--
-- Name: bronze_ig_profiles bronze_ig_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_ig_profiles
    ADD CONSTRAINT bronze_ig_profiles_pkey PRIMARY KEY (restaurant_id, snapshot_date);


--
-- Name: bronze_search_rankings bronze_search_rankings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_search_rankings
    ADD CONSTRAINT bronze_search_rankings_pkey PRIMARY KEY (id);


--
-- Name: bronze_serp bronze_serp_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_serp
    ADD CONSTRAINT bronze_serp_pkey PRIMARY KEY (id);


--
-- Name: bronze_serp bronze_serp_query_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_serp
    ADD CONSTRAINT bronze_serp_query_key UNIQUE (query);


--
-- Name: bronze_social_posts bronze_social_posts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_social_posts
    ADD CONSTRAINT bronze_social_posts_pkey PRIMARY KEY (id);


--
-- Name: bronze_social_posts bronze_social_posts_platform_post_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_social_posts
    ADD CONSTRAINT bronze_social_posts_platform_post_id_key UNIQUE (platform, post_id);


--
-- Name: bronze_tiktok_comments bronze_tiktok_comments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_tiktok_comments
    ADD CONSTRAINT bronze_tiktok_comments_pkey PRIMARY KEY (comment_id);


--
-- Name: bronze_tiktok_posts bronze_tiktok_posts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_tiktok_posts
    ADD CONSTRAINT bronze_tiktok_posts_pkey PRIMARY KEY (post_id);


--
-- Name: bronze_tiktok_profiles bronze_tiktok_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_tiktok_profiles
    ADD CONSTRAINT bronze_tiktok_profiles_pkey PRIMARY KEY (restaurant_id, snapshot_date);


--
-- Name: bronze_wolt bronze_wolt_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_wolt
    ADD CONSTRAINT bronze_wolt_pkey PRIMARY KEY (wolt_id);


--
-- Name: competitors competitors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.competitors
    ADD CONSTRAINT competitors_pkey PRIMARY KEY (id);


--
-- Name: contact_enrichments contact_enrichments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contact_enrichments
    ADD CONSTRAINT contact_enrichments_pkey PRIMARY KEY (id);


--
-- Name: contact_enrichments contact_enrichments_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contact_enrichments
    ADD CONSTRAINT contact_enrichments_slug_key UNIQUE (slug);


--
-- Name: opportunities opportunities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.opportunities
    ADD CONSTRAINT opportunities_pkey PRIMARY KEY (id);


--
-- Name: raw_ingestions raw_ingestions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_ingestions
    ADD CONSTRAINT raw_ingestions_pkey PRIMARY KEY (id);


--
-- Name: raw_ingestions raw_ingestions_place_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.raw_ingestions
    ADD CONSTRAINT raw_ingestions_place_id_unique UNIQUE (place_id);


--
-- Name: report_events report_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.report_events
    ADD CONSTRAINT report_events_pkey PRIMARY KEY (id);


--
-- Name: restaurant_pipeline restaurant_pipeline_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurant_pipeline
    ADD CONSTRAINT restaurant_pipeline_pkey PRIMARY KEY (place_id);


--
-- Name: restaurant_social_handles restaurant_social_handles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurant_social_handles
    ADD CONSTRAINT restaurant_social_handles_pkey PRIMARY KEY (restaurant_id);


--
-- Name: restaurants restaurants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurants
    ADD CONSTRAINT restaurants_pkey PRIMARY KEY (id);


--
-- Name: restaurants restaurants_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurants
    ADD CONSTRAINT restaurants_slug_key UNIQUE (slug);


--
-- Name: reviews reviews_google_review_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reviews
    ADD CONSTRAINT reviews_google_review_id_unique UNIQUE (google_review_id);


--
-- Name: reviews reviews_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reviews
    ADD CONSTRAINT reviews_pkey PRIMARY KEY (id);


--
-- Name: silver_labels silver_labels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_labels
    ADD CONSTRAINT silver_labels_pkey PRIMARY KEY (id);


--
-- Name: silver_labels silver_labels_place_id_label_type_label_value_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_labels
    ADD CONSTRAINT silver_labels_place_id_label_type_label_value_key UNIQUE (place_id, label_type, label_value);


--
-- Name: silver_restaurants silver_restaurants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_restaurants
    ADD CONSTRAINT silver_restaurants_pkey PRIMARY KEY (id);


--
-- Name: silver_restaurants silver_restaurants_place_id_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_restaurants
    ADD CONSTRAINT silver_restaurants_place_id_unique UNIQUE (place_id);


--
-- Name: silver_search_visibility silver_search_visibility_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_search_visibility
    ADD CONSTRAINT silver_search_visibility_pkey PRIMARY KEY (id);


--
-- Name: silver_search_visibility silver_search_visibility_place_id_query_fetched_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_search_visibility
    ADD CONSTRAINT silver_search_visibility_place_id_query_fetched_date_key UNIQUE (place_id, query, fetched_date);


--
-- Name: silver_social_mentions silver_social_mentions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_social_mentions
    ADD CONSTRAINT silver_social_mentions_pkey PRIMARY KEY (id);


--
-- Name: target_accounts target_accounts_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_accounts
    ADD CONSTRAINT target_accounts_name_key UNIQUE (name);


--
-- Name: target_accounts target_accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_accounts
    ADD CONSTRAINT target_accounts_pkey PRIMARY KEY (id);


--
-- Name: trends_data trends_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trends_data
    ADD CONSTRAINT trends_data_pkey PRIMARY KEY (id);


--
-- Name: trends_data trends_data_query_geo_period_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trends_data
    ADD CONSTRAINT trends_data_query_geo_period_key UNIQUE (query, geo, period);


--
-- Name: tripadvisor_enrichments tripadvisor_enrichments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tripadvisor_enrichments
    ADD CONSTRAINT tripadvisor_enrichments_pkey PRIMARY KEY (id);


--
-- Name: tripadvisor_enrichments tripadvisor_enrichments_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tripadvisor_enrichments
    ADD CONSTRAINT tripadvisor_enrichments_slug_key UNIQUE (slug);


--
-- Name: tripadvisor_raw tripadvisor_raw_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tripadvisor_raw
    ADD CONSTRAINT tripadvisor_raw_pkey PRIMARY KEY (id);


--
-- Name: tripadvisor_raw tripadvisor_raw_slug_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tripadvisor_raw
    ADD CONSTRAINT tripadvisor_raw_slug_key UNIQUE (slug);


--
-- Name: wolt_enrichments wolt_enrichments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wolt_enrichments
    ADD CONSTRAINT wolt_enrichments_pkey PRIMARY KEY (id);


--
-- Name: wolt_enrichments wolt_enrichments_wolt_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wolt_enrichments
    ADD CONSTRAINT wolt_enrichments_wolt_id_key UNIQUE (wolt_id);


--
-- Name: bronze_search_rankings_place_query_date_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX bronze_search_rankings_place_query_date_idx ON public.bronze_search_rankings USING btree (place_id, query, scraped_date);


--
-- Name: bronze_serp_place_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX bronze_serp_place_id_idx ON public.bronze_serp USING btree (place_id);


--
-- Name: bronze_serp_query_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX bronze_serp_query_type_idx ON public.bronze_serp USING btree (query_type);


--
-- Name: idx_silver_labels_place_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_silver_labels_place_id ON public.silver_labels USING btree (place_id);


--
-- Name: idx_silver_labels_type_value; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_silver_labels_type_value ON public.silver_labels USING btree (label_type, label_value);


--
-- Name: restaurant_pipeline restaurant_pipeline_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER restaurant_pipeline_updated_at BEFORE UPDATE ON public.restaurant_pipeline FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


--
-- Name: bronze_ig_comments bronze_ig_comments_post_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_ig_comments
    ADD CONSTRAINT bronze_ig_comments_post_id_fkey FOREIGN KEY (post_id) REFERENCES public.bronze_ig_posts(post_id);


--
-- Name: bronze_ig_comments bronze_ig_comments_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_ig_comments
    ADD CONSTRAINT bronze_ig_comments_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: bronze_ig_posts bronze_ig_posts_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_ig_posts
    ADD CONSTRAINT bronze_ig_posts_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: bronze_ig_profiles bronze_ig_profiles_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_ig_profiles
    ADD CONSTRAINT bronze_ig_profiles_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: bronze_serp bronze_serp_place_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_serp
    ADD CONSTRAINT bronze_serp_place_id_fkey FOREIGN KEY (place_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: bronze_tiktok_comments bronze_tiktok_comments_post_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_tiktok_comments
    ADD CONSTRAINT bronze_tiktok_comments_post_id_fkey FOREIGN KEY (post_id) REFERENCES public.bronze_tiktok_posts(post_id);


--
-- Name: bronze_tiktok_comments bronze_tiktok_comments_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_tiktok_comments
    ADD CONSTRAINT bronze_tiktok_comments_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: bronze_tiktok_posts bronze_tiktok_posts_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_tiktok_posts
    ADD CONSTRAINT bronze_tiktok_posts_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: bronze_tiktok_profiles bronze_tiktok_profiles_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bronze_tiktok_profiles
    ADD CONSTRAINT bronze_tiktok_profiles_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: competitors competitors_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.competitors
    ADD CONSTRAINT competitors_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.restaurants(id) ON DELETE CASCADE;


--
-- Name: contact_enrichments contact_enrichments_raw_ingestion_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.contact_enrichments
    ADD CONSTRAINT contact_enrichments_raw_ingestion_id_fkey FOREIGN KEY (raw_ingestion_id) REFERENCES public.raw_ingestions(id);


--
-- Name: opportunities opportunities_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.opportunities
    ADD CONSTRAINT opportunities_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.restaurants(id) ON DELETE CASCADE;


--
-- Name: report_events report_events_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.report_events
    ADD CONSTRAINT report_events_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.restaurants(id) ON DELETE CASCADE;


--
-- Name: restaurant_pipeline restaurant_pipeline_place_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurant_pipeline
    ADD CONSTRAINT restaurant_pipeline_place_id_fkey FOREIGN KEY (place_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: restaurant_social_handles restaurant_social_handles_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurant_social_handles
    ADD CONSTRAINT restaurant_social_handles_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: reviews reviews_restaurant_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.reviews
    ADD CONSTRAINT reviews_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES public.silver_restaurants(id) ON DELETE CASCADE;


--
-- Name: silver_labels silver_labels_place_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_labels
    ADD CONSTRAINT silver_labels_place_id_fkey FOREIGN KEY (place_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: silver_social_mentions silver_social_mentions_bronze_post_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_social_mentions
    ADD CONSTRAINT silver_social_mentions_bronze_post_id_fkey FOREIGN KEY (bronze_post_id) REFERENCES public.bronze_social_posts(id);


--
-- Name: silver_social_mentions silver_social_mentions_place_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.silver_social_mentions
    ADD CONSTRAINT silver_social_mentions_place_id_fkey FOREIGN KEY (place_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: target_accounts target_accounts_place_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.target_accounts
    ADD CONSTRAINT target_accounts_place_id_fkey FOREIGN KEY (place_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: tripadvisor_enrichments tripadvisor_enrichments_raw_ingestion_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tripadvisor_enrichments
    ADD CONSTRAINT tripadvisor_enrichments_raw_ingestion_id_fkey FOREIGN KEY (raw_ingestion_id) REFERENCES public.raw_ingestions(id);


--
-- Name: tripadvisor_raw tripadvisor_raw_raw_ingestion_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tripadvisor_raw
    ADD CONSTRAINT tripadvisor_raw_raw_ingestion_id_fkey FOREIGN KEY (raw_ingestion_id) REFERENCES public.raw_ingestions(id);


--
-- Name: wolt_enrichments wolt_enrichments_place_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wolt_enrichments
    ADD CONSTRAINT wolt_enrichments_place_id_fkey FOREIGN KEY (place_id) REFERENCES public.silver_restaurants(place_id);


--
-- Name: bronze_ig_comments; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_ig_comments ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_ig_hashtag_posts; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_ig_hashtag_posts ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_ig_posts; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_ig_posts ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_ig_profiles; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_ig_profiles ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_search_rankings; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_search_rankings ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_serp; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_serp ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_social_posts; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_social_posts ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_tiktok_comments; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_tiktok_comments ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_tiktok_posts; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_tiktok_posts ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_tiktok_profiles; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_tiktok_profiles ENABLE ROW LEVEL SECURITY;

--
-- Name: bronze_wolt; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.bronze_wolt ENABLE ROW LEVEL SECURITY;

--
-- Name: competitors; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.competitors ENABLE ROW LEVEL SECURITY;

--
-- Name: contact_enrichments; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.contact_enrichments ENABLE ROW LEVEL SECURITY;

--
-- Name: opportunities; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.opportunities ENABLE ROW LEVEL SECURITY;

--
-- Name: raw_ingestions; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.raw_ingestions ENABLE ROW LEVEL SECURITY;

--
-- Name: report_events; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.report_events ENABLE ROW LEVEL SECURITY;

--
-- Name: restaurant_pipeline; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.restaurant_pipeline ENABLE ROW LEVEL SECURITY;

--
-- Name: restaurant_social_handles; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.restaurant_social_handles ENABLE ROW LEVEL SECURITY;

--
-- Name: restaurants; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.restaurants ENABLE ROW LEVEL SECURITY;

--
-- Name: reviews; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.reviews ENABLE ROW LEVEL SECURITY;

--
-- Name: silver_labels; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.silver_labels ENABLE ROW LEVEL SECURITY;

--
-- Name: silver_restaurants; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.silver_restaurants ENABLE ROW LEVEL SECURITY;

--
-- Name: silver_social_mentions; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.silver_social_mentions ENABLE ROW LEVEL SECURITY;

--
-- Name: target_accounts; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.target_accounts ENABLE ROW LEVEL SECURITY;

--
-- Name: trends_data; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.trends_data ENABLE ROW LEVEL SECURITY;

--
-- Name: tripadvisor_enrichments; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.tripadvisor_enrichments ENABLE ROW LEVEL SECURITY;

--
-- Name: tripadvisor_raw; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.tripadvisor_raw ENABLE ROW LEVEL SECURITY;

--
-- Name: wolt_enrichments; Type: ROW SECURITY; Schema: public; Owner: -
--

ALTER TABLE public.wolt_enrichments ENABLE ROW LEVEL SECURITY;

--
-- PostgreSQL database dump complete
--

\unrestrict WldfAKm7TQwQ6HYsfnvf2hWhXWd7NatDcWQxtA9GvcPWYhMVLSobZUfLloJqpSF

